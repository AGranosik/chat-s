package ws

import (
	"context"
	"fmt"
	"messages/chat"
	"sync"
)

type MessageHandler interface {
	HandleIncoming(ctx context.Context, m chat.Message) error
}

type Hub struct {
	connections map[string]struct{}
	mu          sync.RWMutex
	s           MessageHandler
}

func NewHub(service MessageHandler) (*Hub, error) {
	if service == nil {
		return nil, fmt.Errorf("chat service cannot be null.")
	}

	return &Hub{
		connections: make(map[string]struct{}),
		s:           service,
	}, nil
}

func (h *Hub) Register(clientId string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.connections[clientId] = struct{}{}
	return nil
}

func (h *Hub) Unregister(clientId string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.connections, clientId)

	return nil
}

// it should send to connect client or just pass through to kafka
// seprate method to handle incoming?
func (h *Hub) SendMessage(message chat.Message, ctx context.Context) error {
	return h.s.HandleIncoming(ctx, message)
}
