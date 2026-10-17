package service

import (
	"context"
	"messages/chat"
)

// // TODO: where to put it?
type ingestHandler struct {
}

func NewIngestHandler() (*ingestHandler, error) {

	return &ingestHandler{}, nil
}

func (i *ingestHandler) HandleIncoming(ctx context.Context, m chat.Message) error {
	return nil
}
