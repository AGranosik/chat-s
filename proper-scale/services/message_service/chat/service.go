package chat

import "encoding/json"

type Message struct {
	RoomID   string          `json:"room_id"`
	ClientID string          `json:"client_id"`
	Payload  json.RawMessage `json:"payload"`
}

type ChatService struct {
	publisher MessagePublisher
}

func NewChatService(p MessagePublisher) *ChatService {
	return &ChatService{
		publisher: p,
	}
}

func (c *ChatService) HandleIncoming(m Message) error {
	//kafka there no need of outbox
	return nil
}
