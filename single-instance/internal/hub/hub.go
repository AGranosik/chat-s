package hub

import (
	"context"
	"encoding/json"
	"log"

	"chat-s/internal/models"
)

type envelope struct {
	roomID  string
	payload []byte
}

type Hub struct {
	register   chan *Client
	unregister chan *Client
	broadcast  chan envelope
	rooms      map[string]map[*Client]struct{}
}

func New() *Hub {
	return &Hub{
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan envelope),
		rooms:      make(map[string]map[*Client]struct{}),
	}
}

func (h *Hub) Register(c *Client)   { h.register <- c }
func (h *Hub) Unregister(c *Client) { h.unregister <- c }

func (h *Hub) Broadcast(roomID string, msg models.Message) {
	payload, err := json.Marshal(msg)
	if err != nil {
		log.Printf("hub: marshal message | room=%s err=%v", roomID, err)
		return
	}
	h.broadcast <- envelope{roomID: roomID, payload: payload}
}

func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-h.register:
			room := h.rooms[c.roomID]
			if room == nil {
				room = make(map[*Client]struct{})
				h.rooms[c.roomID] = room
			}
			room[c] = struct{}{}
		case c := <-h.unregister:
			if room, ok := h.rooms[c.roomID]; ok {
				if _, ok := room[c]; ok {
					delete(room, c)
					close(c.send)
					if len(room) == 0 {
						delete(h.rooms, c.roomID)
					}
				}
			}
		case env := <-h.broadcast:
			for c := range h.rooms[env.roomID] {
				c.send <- env.payload
			}
		}
	}
}
