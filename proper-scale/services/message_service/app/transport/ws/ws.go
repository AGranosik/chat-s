package transport

import (
	"context"
	"fmt"
	"log/slog"
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

type Ws struct {
	grpc contractsv1.UserServiceClient
	hub  *Hub
}

func NewWsHub(grpc contractsv1.UserServiceClient, hub *Hub) *Ws {
	return &Ws{
		grpc: grpc,
		hub:  hub,
	}
}

//connection per client
//dial cfg

func (h *Ws) ServeWS(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//get room and clientid somehow
	clientId := r.URL.Query().Get("clientId")
	dial := r.URL.Query().Get("dial")
	conn, err := h.configureConnection(w, r, ctx, clientId, dial)
	if err != nil {
		return
	}
	defer conn.Close()
	defer h.disconnect(clientId, ctx)
	go runPing(conn, ctx)

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

		if err := handleMessage(data); err != nil {
			slog.Warn("bad message, dropping", "err", err)
			continue // don't kill the connection over one bad message
		}
	}
}

func (h *Ws) configureConnection(w http.ResponseWriter, r *http.Request, ctx context.Context, clientId string, dial string) (*websocket.Conn, error) {
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
		ClientId: clientId,
		Dial:     dial,
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

func (h *Ws) disconnect(clientId string, ctx context.Context) {
	response, err := h.grpc.Disconnect(ctx, &contractsv1.DisconnectUserRequest{
		ClientId: clientId,
	})

	if err != nil {
		slog.Error("disconnection error.", "error", err.Error())
	}

	if !response.Success {
		slog.Error("Grpc disconnection failure", "error", err.Error())
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
