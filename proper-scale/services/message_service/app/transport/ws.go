package transport

import (
	"context"
	"encoding/json"
	"log"
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

type Client struct {
	ClientId string
	Dial     string
}

type WsHub struct {
	grpc contractsv1.UserServiceClient
}

func NewWsHub(grpc contractsv1.UserServiceClient) *WsHub {
	return &WsHub{
		grpc: grpc,
	}
}

func (h *WsHub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := configureConnection(w, r)

	if err != nil {
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go runPing(conn, cancel, ctx)

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

func configureConnection(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
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

	return conn, err
}

func (h *WsHub) handleMessage(data []byte, ctx context.Context) error {
	log.Printf("msg received.")
	var in Client
	if err := json.Unmarshal(data, &in); err != nil {
		log.Printf("ws decode | err=%v", err)
		return err
	}
	log.Printf("ws decode | client=%s | dial=%s", in.ClientId, in.Dial)

	h.grpc.Connect(ctx, &contractsv1.ConnectUserRequest{
		ClientId: in.ClientId,
		Dial:     in.Dial,
	})
	return nil
}

func runPing(conn *websocket.Conn, cancel context.CancelFunc, ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				cancel()
				conn.Close()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
