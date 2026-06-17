package hub

import (
	"context"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 8 * 1024
	sendBuffer     = 256
)

// Client is one websocket connection bound to a single room.
type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	roomID string
	send   chan []byte
}

func NewClient(h *Hub, conn *websocket.Conn, roomID string) *Client {
	return &Client{
		hub:    h,
		conn:   conn,
		roomID: roomID,
		send:   make(chan []byte, sendBuffer),
	}
}

// ReadPump reads frames and hands each payload to onMessage. It runs until the
// peer disconnects, then unregisters the client. Run on its own goroutine.
func (c *Client) ReadPump(onMessage func(data []byte)) {
	defer func() {
		c.hub.Unregister(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("ws read error | room=%s err=%v", c.roomID, err)
			}
			return
		}
		onMessage(data)
	}
}

// WritePump flushes queued messages and sends periodic pings to keep the
// connection alive. Run on its own goroutine.
func (c *Client) WritePump(ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub closed the channel on unregister.
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
