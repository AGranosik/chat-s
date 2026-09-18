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

func (c *ChatService) HandleIncoming(m Message, ctx context.Context) error {

	c.publisher.Publish(ctx, "messages", m.RoomID, m.Payload)
	return nil
}
