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

type MessagePublisher interface {
	PublishMessage(ctx context.Context, m Message) error
}

type MessageService struct {
	publisher MessagePublisher
}

func NewMessageHandler(p MessagePublisher) (*MessageService, error) {
	return &MessageService{
		publisher: p,
	}, nil
}

func (s *MessageService) Handle(ctx context.Context, m Message) error {
	//some logic before
	return s.publisher.PublishMessage(ctx, m)
}
