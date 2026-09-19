package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"messages/chat"
	"net/http"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
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

type clientConnection struct {
	ClientId string
	Conn     *websocket.Conn
}

type Ws struct {
	grpc        contractsv1.UserServiceClient
	hub         *Hub
	serviceDial string
}

func NewWsHub(grpc contractsv1.UserServiceClient, hub *Hub, serviceDial string) *Ws {
	return &Ws{
		grpc:        grpc,
		hub:         hub,
		serviceDial: serviceDial,
	}
}

func (h *Ws) ServeWS(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientId := r.URL.Query().Get("clientId")
	if len(clientId) == 0 {
		slog.Error("No client id.")
		return
	}
	clientConnection := &clientConnection{
		ClientId: clientId,
	}
	conn, err := h.configureConnection(w, r, ctx, clientConnection)
	if err != nil {
		return
	}
	slog.Info("client connected", "clientId", clientId)
	defer conn.Close()
	defer h.disconnect(clientConnection, ctx)
	go runPing(conn, ctx)

	h.hub.Register(clientId)

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

func (h *Ws) configureConnection(w http.ResponseWriter, r *http.Request, ctx context.Context, client *clientConnection) (*websocket.Conn, error) {
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
		ClientId: client.ClientId,
	})

	if err != nil {
		slog.Error("connect rpc failed", "err", err)
		conn.Close()
		return nil, err
	}
	if !response.Success {
		conn.Close()
		return nil, fmt.Errorf("connect rejected for client %s", client.ClientId)
	}

	client.Conn = conn
	return conn, err
}

func (h *Ws) disconnect(client *clientConnection, ctx context.Context) {
	defer client.Conn.Close()
	defer h.hub.Unregister(client.ClientId)
	response, err := h.grpc.Disconnect(ctx, &contractsv1.DisconnectUserRequest{
		ClientId: client.ClientId,
	})

	if err != nil {
		slog.Error("disconnection error.", "error", err.Error())
		return
	}

	if !response.Success {
		slog.Error("Grpc disconnection failure", "error", err.Error())
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

	h.hub.SendMessage(message, ctx)
	return nil
}
