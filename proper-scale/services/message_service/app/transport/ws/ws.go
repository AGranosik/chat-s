package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
	messagev1 "github.com/AGranosik/chat/contracts/messages"
	"github.com/gorilla/websocket"
)

const (
	writeTimeout    = 10 * time.Second
	pongWait        = 60 * time.Second
	pingPeriod      = (pongWait * 9) / 10
	maxMessageBytes = 1 << 20
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type IngestHandler interface {
	HandleIncoming(ctx context.Context, clientID string, request messagev1.SendRequest) (messagev1.Event, error)
}

type Ws struct {
	grpc        contractsv1.UserServiceClient
	hub         *Hub
	serviceDial string
	ingest      IngestHandler
}

func NewWs(grpc contractsv1.UserServiceClient, hub *Hub, serviceDial string, i IngestHandler) (*Ws, error) {
	if hub == nil {
		return nil, fmt.Errorf("hub cannot be null.")
	}

	if len(serviceDial) == 0 {
		return nil, fmt.Errorf("service dial cannot be empty")
	}
	if i == nil {
		return nil, fmt.Errorf("ingest handler cannot be nil")
	}
	return &Ws{
		grpc:        grpc,
		hub:         hub,
		serviceDial: serviceDial,
		ingest:      i,
	}, nil
}

func (h *Ws) ServeWS(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientID, roomIDs, err := getParams(r.URL.Query())

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conn, err := h.configureConnection(w, r, ctx, clientID, roomIDs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	slog.Info("client connected", "clientId", clientID)
	defer conn.Close()
	defer h.disconnect(clientID, conn)
	go h.runPing(ctx, conn, clientID)

	client := NewClientConnection(clientID, conn)
	err = h.hub.Register(roomIDs, client)

	if err != nil {
		http.Error(w, "Error on conn creation", http.StatusBadRequest)
		return
	}

	go client.WritePump(ctx)
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
				websocket.CloseNoStatusReceived,
			) {
				slog.Warn("unexpected close", "err", err)
			} else {
				slog.Info("connection closed", "err", err)
			}
			return
		}

		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue // ignore ping/pong/close control frames here, gorilla handles them
		}

		if err := h.handleMessage(data, ctx, clientID, client); err != nil {
			slog.Warn("bad message, dropping", "err", err)
			continue // don't kill the connection over one bad message
		}
	}
}

func (h *Ws) configureConnection(w http.ResponseWriter, r *http.Request, ctx context.Context, clientId string, roomIds []string) (*websocket.Conn, error) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("ws upgrade failed", "err", err)
		return nil, err
	}
	conn.SetReadLimit(maxMessageBytes)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	response, err := h.grpc.Connect(ctx, &contractsv1.ConnectUserRequest{
		InstanceName: h.serviceDial,
		RoomsId:      roomIds,
		ClientId:     clientId,
	})

	if err != nil {
		slog.Error("connect rpc failed", "err", err)
		conn.Close()
		return nil, err
	}
	if !response.Success {
		conn.Close()
		return nil, fmt.Errorf("connect rejected for client %s", clientId)
	}

	return conn, err
}

func (h *Ws) disconnect(clientId string, wsConn *websocket.Conn) {
	log.Printf("disconnected")
	h.hub.Unregister(clientId)
	wsConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := h.grpc.Disconnect(ctx, &contractsv1.DisconnectUserRequest{
		ClientId: clientId,
	})

	if err != nil {
		slog.Error("disconnection error.", "error", err.Error())
		return
	}

	if !response.Success {
		slog.Error("Grpc disconnection failure")
		return
	}
}

func (h *Ws) runPing(ctx context.Context, conn *websocket.Conn, clientId string) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				log.Printf("connection closed - ping, %v", err)
				conn.Close()
				return
			}

			pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			res, err := h.grpc.Ping(pctx, &contractsv1.PingConnectionRequest{
				ClientId:     clientId,
				InstanceName: h.serviceDial,
			})

			cancel()
			if err != nil || !res.Success {
				log.Printf("connection closed - grpc ping, err: %v", err)
				conn.Close()
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (h *Ws) handleMessage(data []byte, ctx context.Context, clientID string, client *ClientConnection) error {
	var request messagev1.SendRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return h.enqueueRejected(ctx, client, "", messagev1.InvalidRequestErrorCode)
	}
	if err := request.Validate(); err != nil {
		slog.Warn("invalid send request", "err", err)
		return h.enqueueRejected(ctx, client, request.ClientMessageID, messagev1.InvalidRequestErrorCode)
	}
	event, err := h.ingest.HandleIncoming(ctx, clientID, request)
	if err != nil {
		slog.Error("message was not accepted", "clientMessageId", request.ClientMessageID, "err", err)
		return h.enqueueRejected(ctx, client, request.ClientMessageID, messagev1.NotAcceptedErrorCode)
	}
	ack := messagev1.AcceptedAck{
		Type:            messagev1.AcceptedFrameType,
		Version:         messagev1.Version,
		MessageID:       event.MessageID,
		ClientMessageID: event.ClientMessageID,
		RoomID:          event.RoomID,
		RoomSeq:         event.RoomSeq,
		CreatedAt:       event.CreatedAt,
	}
	if err := ack.Validate(); err != nil {
		slog.Error("invalid accepted message", "clientMessageId", request.ClientMessageID, "err", err)
		return h.enqueueRejected(ctx, client, request.ClientMessageID, messagev1.NotAcceptedErrorCode)
	}
	return h.enqueue(ctx, client, ack)
}

func (h *Ws) enqueueRejected(ctx context.Context, client *ClientConnection, clientMessageID string, errorCode string) error {
	rejection := messagev1.RejectedAck{
		Type:            messagev1.RejectedFrameType,
		Version:         messagev1.Version,
		ClientMessageID: clientMessageID,
		ErrorCode:       errorCode,
	}
	if err := rejection.Validate(); err != nil {
		return fmt.Errorf("invalid rejected message: %w", err)
	}
	return h.enqueue(ctx, client, rejection)
}

func (h *Ws) enqueue(ctx context.Context, client *ClientConnection, frame any) error {
	serialized, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("marshal websocket frame: %w", err)
	}
	select {
	case client.Send <- serialized:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func getParams(url url.Values) (string, []string, error) {
	clientID := url.Get("client_id")
	if clientID == "" {

		return "", nil, fmt.Errorf("client_id is required")
	}

	roomParam := url.Get("room_ids")
	if roomParam == "" {
		return "", nil, fmt.Errorf("room_ids is required")
	}
	roomIDs := strings.Split(roomParam, ",")

	return clientID, roomIDs, nil
}
