package services

import (
	"context"
	"log"
	"relayservice/app/storage"
	"time"
)

const (
	batchSize    = 100
	pollInterval = 2 * time.Second
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

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		if err := r.drain(ctx); err != nil && ctx.Err() == nil {
			log.Printf("outbox relay drain error | err=%v", err)
		}
		select {
		case <-ctx.Done():
			log.Println("worker: draining and exiting")
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) drain(ctx context.Context) error {
	for {
		n, err := r.store.DispatchBatch(ctx, batchSize, func(
			ctx context.Context,
			event []storage.OutboxEvent,
		) error {
			log.Println("drain")
			return nil
		})
		if err != nil {
			return err
		}
		if n < batchSize {
			return nil // a short batch (including 0) means the outbox is drained
		}
	}
}
