package ws

import "context"

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
	defer c.Conn.Close()
	for {
		select {
		case msg, ok := <-c.Send:
			if !ok {
				return
			}
			if err := c.Conn.WriteMessage(TextMessage, msg); err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
