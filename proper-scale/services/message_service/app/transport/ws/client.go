package ws

import (
	"context"
	"log"

	"github.com/gorilla/websocket"
)

type Conn interface {
	WriteMessage(messageType int, data []byte) error
	Close() error
}
type ClientConnection struct {
	ClientID string
	conn     Conn
	Send     chan []byte
}

func NewClientConnection(clientID string, conn Conn) *ClientConnection {
	return &ClientConnection{
		ClientID: clientID,
		conn:     conn,
		Send:     make(chan []byte, 256),
	}
}

func (c *ClientConnection) WritePump(ctx context.Context) {
	defer c.conn.Close()
	for {
		select {
		case msg, ok := <-c.Send:
			if !ok {
				log.Printf("consuming channel error.")
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("errorr writng msg, err=%v", err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
