package app

import (
	"context"
	"log"
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
	log.Printf("msg received.")
	return nil
}
