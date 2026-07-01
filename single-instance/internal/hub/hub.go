package hub

import (
	"context"
	"encoding/json"
	"log"

	"chat-s/internal/models"
)

// envelope is a room-scoped payload moving through the broadcast channel.
type envelope struct {
	roomID  string
	payload []byte
}

// Hub owns live connection state for one process. A single Run goroutine owns
// the rooms map and selects over the channels, so there is no shared-memory
// contention (the pattern from ../log-prop).
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

// Broadcast implements chat.Broadcaster. It marshals the message and queues it
// for fan-out. The hand-off blocks rather than drops — losing a message is
// worse than back-pressure (see CLAUDE.md).
func (h *Hub) Broadcast(roomID string, msg models.Message) {
	payload, err := json.Marshal(msg)
	if err != nil {
		log.Printf("hub: marshal message | room=%s err=%v", roomID, err)
		return
	}
	h.broadcast <- envelope{roomID: roomID, payload: payload}
}

// Run owns the rooms map and serialises every mutation. Cancel ctx to stop.
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
				c.send <- env.payload // blocking send: never drop a message
			}
		}
	}
}
