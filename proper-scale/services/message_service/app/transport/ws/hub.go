package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"messages/chat"
	"sync"
)

type MessageHandler interface {
	HandleIncoming(ctx context.Context, m chat.Message) error
}

type DeliveryMessage struct {
	RoomID    string
	Payload   json.RawMessage `json:"payload"`
	ClientsID []string
}
type Room struct {
	ID      string
	mu      sync.RWMutex
	clients map[string]*ClientConnection
}

func newRoom(id string) *Room {
	return &Room{
		ID:      id,
		clients: make(map[string]*ClientConnection),
	}
}

func (r *Room) addClient(c *ClientConnection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[c.ClientID] = c
}

func (r *Room) removeClient(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, clientID)
}

func (r *Room) isEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients) == 0
}

func (r *Room) clientIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.clients))
	for id := range r.clients {
		ids = append(ids, id)
	}
	return ids
}

type Hub struct {
	mu    sync.RWMutex
	rooms map[string]*Room
	s     MessageHandler
}

func NewHub(service MessageHandler) (*Hub, error) {
	if service == nil {
		return nil, fmt.Errorf("chat service cannot be nil")
	}

	return &Hub{
		rooms: make(map[string]*Room),
		s:     service,
	}, nil
}

func (h *Hub) Register(roomIDs []string, conn *ClientConnection) error {
	if conn == nil {
		return fmt.Errorf("client connection cannot be nil")
	}

	h.mu.Lock()
	for _, roomId := range roomIDs {

		room, ok := h.rooms[roomId]
		if !ok {
			room = newRoom(roomId)
			h.rooms[roomId] = room
		}
		room.addClient(conn)
	}
	h.mu.Unlock()

	return nil
}

func (h *Hub) Unregister(clientID string) error {
	h.mu.Lock()
	for _, room := range h.rooms {

		room.removeClient(clientID)
		if room.isEmpty() {
			delete(h.rooms, room.ID)
		}
	}
	h.mu.Unlock()

	return nil
}

func (h *Hub) Clients(roomID string) []string {
	h.mu.RLock()
	room, ok := h.rooms[roomID]
	h.mu.RUnlock()
	if !ok {
		return nil
	}
	return room.clientIDs()
}

func (h *Hub) HandleIncoming(message chat.Message, ctx context.Context) error {
	return h.s.HandleIncoming(ctx, message)
}

func (h *Hub) SendMessage(message chat.Message, ctx context.Context) error {
	return nil
}
