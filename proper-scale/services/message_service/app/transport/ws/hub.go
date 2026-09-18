package ws

import (
	"context"
	"messages/chat"
	"sync"
)

type Hub struct {
	connections map[string]struct{}
	mu          sync.RWMutex
	s           *chat.ChatService
}

func NewHub(service *chat.ChatService) (*Hub, error) {
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

func (h *Hub) SendMessage(message chat.Message, ctx context.Context) error {
	return h.s.HandleIncoming(message, ctx)
}
