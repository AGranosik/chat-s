package ws

type Conn interface {
	WriteMessage(messageType int, data []byte) error
	Close() error
}
type ClientConnection struct {
	ClientID string
	Conn     Conn
	Send     chan []byte
}

func NewClientConnection(clientID string, conn Conn) *ClientConnection {
	return &ClientConnection{
		ClientID: clientID,
		Conn:     conn,
		Send:     make(chan []byte, 256),
	}
}
