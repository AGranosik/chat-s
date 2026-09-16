package ws

import (
	"messages/chat"
	"sync"
)

type Hub struct {
	connections map[string][]Client
	mu          sync.RWMutex
	s           *chat.ChatService
}

func NewHub(service *chat.ChatService) (*Hub, error) {
	return &Hub{
		connections: make(map[string][]Client),
		s:           service,
	}, nil
}

func (h *Hub) Register(roomId string, client Client) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.connections[roomId] = append(h.connections[roomId], client)
	return nil
}

func (h *Hub) Unregister(roomId string, client Client) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	room, ok := h.connections[roomId]
	if !ok {
		return nil
	}

	for i, c := range room {
		if c.clientId == client.clientId {
			room[i] = room[len(room)-1]
			room = room[:len(room)-1]
			break
		}
	}

	if len(room) == 0 {
		delete(h.connections, roomId)
	} else {
		h.connections[roomId] = room
	}

	return nil
}

func (h *Hub) SendMessage(roomId string, data []byte) error {
	return h.s.HandleIncoming(roomId, data)
}
