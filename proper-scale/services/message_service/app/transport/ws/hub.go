package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"messages/chat"
	"slices"
	"sync"
)

type Room struct {
	ID      string
	mu      sync.RWMutex
	clients map[string]*ClientConnection
}

type Hub struct {
	mu    sync.RWMutex
	rooms map[string]*Room
}

func NewHub() (*Hub, error) {
	return &Hub{
		rooms: make(map[string]*Room),
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

func (h *Hub) Dispatch(ctx context.Context, m chat.Message) error {
	if err := m.Validate(); err != nil {
		return fmt.Errorf("invalid message event: %w", err)
	}
	serialized, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal message event: %w", err)
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	r, ok := h.rooms[m.RoomID]
	if !ok {
		return fmt.Errorf("error - dispatch to room %q", m.RoomID)
	}

	for _, c := range r.clients {
		select {
		case c.Send <- serialized:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (h *Hub) allClients() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	res := []string{}
	for _, r := range h.rooms {
		res = slices.Concat(res, r.clientIDs())
	}

	return res
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
