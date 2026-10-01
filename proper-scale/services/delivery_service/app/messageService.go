package app

import (
	"context"
	"time"
)

type Message struct {
	Key, Value []byte
	Headers    map[string]string
	Timestamp  time.Time
}

type MessageHandler interface {
	Handle(ctx context.Context, m Message) error
}

type MessageService struct {
}

func NewMessageHandler() (MessageHandler, error) {
	return &MessageService{}, nil
}
func (s *MessageService) Handle(ctx context.Context, m Message) error {
	return nil
}
