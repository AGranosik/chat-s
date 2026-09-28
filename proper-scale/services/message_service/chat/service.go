package chat

import (
	"context"
	"encoding/json"
)

type Message struct {
	RoomID  string          `json:"room_id"`
	Payload json.RawMessage `json:"payload"`
}

type ChatService struct {
	publisher MessagePublisher
}

func NewChatService(p MessagePublisher) *ChatService {
	return &ChatService{
		publisher: p,
	}
}

func (c *ChatService) HandleIncoming(ctx context.Context, m Message) error {
	return c.publisher.Publish(ctx, m.RoomID, m.Payload)
}
