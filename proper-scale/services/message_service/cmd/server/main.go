package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"time"

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

func serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("ws upgrade failed", "err", err)
		return
	}
	defer conn.Close()

	conn.SetReadLimit(maxMessageBytes)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// gorilla requires a single writer — pings must go through the same
	// goroutine as any other writes, so this ticker owns all writes here.
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					cancel() // ping failed, tear down the read loop below
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

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

func handleMessage(data []byte) error {
	log.Printf("msg received.")
	var in Client
	if err := json.Unmarshal(data, &in); err != nil {
		log.Printf("ws decode | err=%v", err)
		return nil
	}
	log.Printf("ws decode | client=%s | dial=%s", in.ClientId, in.Dial)
	return nil
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", serveWS)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	slog.Info("listening", "addr", server.Addr)
	log.Fatal(server.ListenAndServe())
}
