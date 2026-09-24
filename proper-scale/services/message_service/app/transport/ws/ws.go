package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"messages/chat"
	"net/http"
	"strings"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"github.com/gorilla/websocket"
)

// TODO: dial
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

type Ws struct {
	grpc        contractsv1.UserServiceClient
	hub         *Hub
	serviceDial string
}

func NewWsHub(grpc contractsv1.UserServiceClient, hub *Hub, serviceDial string) (*Ws, error) {
	if hub == nil {
		return nil, fmt.Errorf("hub cannot be null.")
	}

	if len(serviceDial) == 0 {
		return nil, fmt.Errorf("service dial cannot be empty")
	}
	return &Ws{
		grpc:        grpc,
		hub:         hub,
		serviceDial: serviceDial,
	}, nil
}

func (h *Ws) ServeWS(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientID := r.URL.Query().Get("client_id")
	if clientID == "" {
		http.Error(w, "client_id is required", http.StatusBadRequest)
		return
	}

	roomParam := r.URL.Query().Get("room_ids")
	if roomParam == "" {
		http.Error(w, "room_ids is required", http.StatusBadRequest)
		return
	}
	roomIDs := strings.Split(roomParam, ",")
	conn, err := h.configureConnection(w, r, ctx, clientID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// slog.Info("client connected", "clientId", clientID)
	defer conn.Close()
	defer h.disconnect(ctx, clientID, conn)
	go runPing(conn, ctx)

	err = h.hub.Register(roomIDs, &ClientConnection{
		ClientID: clientID,
		Conn:     conn,
		Send:     make(chan []byte),
	})

	if err != nil {
		http.Error(w, "Error on conn creation", http.StatusBadRequest)
		return
	}

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

		if err := h.handleMessage(data, ctx); err != nil {
			slog.Warn("bad message, dropping", "err", err)
			continue // don't kill the connection over one bad message
		}
	}
}

func (h *Ws) configureConnection(w http.ResponseWriter, r *http.Request, ctx context.Context, clientId string) (*websocket.Conn, error) {
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
		Dial:     h.serviceDial,
		ClientId: clientId,
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

func (h *Ws) disconnect(ctx context.Context, clientId string, wsConn *websocket.Conn) {
	defer h.hub.Unregister(clientId)
	defer wsConn.Close()
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

func runPing(conn *websocket.Conn, ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				conn.Close()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (h *Ws) handleMessage(data []byte, ctx context.Context) error {
	var message chat.Message

	err := json.Unmarshal(data, &message)
	if err != nil {
		return err
	}

	h.hub.HandleIncoming(message, ctx)
	return nil
}
