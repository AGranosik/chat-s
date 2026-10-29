package service

import (
	"context"
	"errors"
	"strings"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
)

type MessageStore interface {
	Accept(ctx context.Context, clientID string, request messagev1.SendRequest) (messagev1.Event, error)
}

type ingestHandler struct {
	store MessageStore
}

func NewIngestHandler(store MessageStore) (*ingestHandler, error) {
	if store == nil {
		return nil, errors.New("message store is required")
	}
	return &ingestHandler{store: store}, nil
}

func (i *ingestHandler) HandleIncoming(ctx context.Context, clientID string, request messagev1.SendRequest) (messagev1.Event, error) {
	if strings.TrimSpace(clientID) == "" {
		return messagev1.Event{}, errors.New("client_id is required")
	}
	if err := request.Validate(); err != nil {
		return messagev1.Event{}, err
	}
	return i.store.Accept(ctx, clientID, request)
}
