package service

import (
	"context"
	"messages/chat"
)

type MessagePublisher interface {
	Publish(ctx context.Context, roomId string, value []byte) error
}

type ingestHandler struct {
	publisher MessagePublisher
}

func NewIngestHandler(p MessagePublisher) (*ingestHandler, error) {
	return &ingestHandler{
		publisher: p,
	}, nil
}

func (i *ingestHandler) HandleIncoming(ctx context.Context, m chat.Message) error {
	return i.publisher.Publish(ctx, m.RoomID, m.Payload)
}
