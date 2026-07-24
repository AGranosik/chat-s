package services

import (
	"context"
	"relayservice/app/storage"
)

type outboxStore interface {
	DispatchBatch(ctx context.Context) (int, error)
}

type Relay struct {
	store *storage.Store
}

func New(store *storage.Store) *Relay {
	return &Relay{
		store: store,
	}
}
