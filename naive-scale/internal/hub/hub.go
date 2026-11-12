package hub

import (
	"context"
	"encoding/json"
	"log"
	"naive-scale/internal/models"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type envelope struct {
	roomID  string
	payload []byte
}

const roomChannelPrefix = "room:"

func roomChannel(roomID string) string { return roomChannelPrefix + roomID }

type Hub struct {
	broker     *redis.Client
	pubSub     *redis.PubSub
	broadcast  chan envelope
	register   chan *Client
	unregister chan *Client
	rooms      map[string]map[*Client]struct{}
}

func New(redisAddr string) *Hub {
	client := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     "",
		DB:           0,
		PoolSize:     10,
		MinIdleConns: 2,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	pubSub := client.Subscribe(context.Background())
	return &Hub{
		broker:     client,
		pubSub:     pubSub,
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
	if err := h.broker.Publish(context.Background(), roomChannel(roomID), payload).Err(); err != nil {
		log.Printf("hub: publish | room=%s err=%v", roomID, err)
	}
}

func (h *Hub) Run(ctx context.Context) {
	defer h.broker.Close()
	defer h.pubSub.Close()
	go h.consume(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-h.register:
			room := h.rooms[c.roomID]
			if room == nil {
				room = make(map[*Client]struct{})
				h.rooms[c.roomID] = room
				if err := h.pubSub.Subscribe(ctx, roomChannel(c.roomID)); err != nil {
					log.Printf("hub: subscribe | room=%s err=%v", c.roomID, err)
				}
			}
			room[c] = struct{}{}
		case c := <-h.unregister:
			if room, ok := h.rooms[c.roomID]; ok {
				if _, ok := room[c]; ok {
					delete(room, c)
					close(c.send)
					if len(room) == 0 {
						delete(h.rooms, c.roomID)
						if err := h.pubSub.Unsubscribe(ctx, roomChannel(c.roomID)); err != nil {
							log.Printf("hub: unsubscribe | room=%s err=%v", c.roomID, err)
						}
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

func (h *Hub) consume(ctx context.Context) {
	ch := h.pubSub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			env := envelope{
				roomID:  strings.TrimPrefix(msg.Channel, roomChannelPrefix),
				payload: []byte(msg.Payload),
			}
			select {
			case h.broadcast <- env:
			case <-ctx.Done():
				return
			}
		}
	}
}
