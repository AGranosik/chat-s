package outbox

import (
	"context"
	"log"
	"time"

	"naive-scale/internal/chat"
	"naive-scale/internal/storage"
)

const (
	batchSize    = 100
	pollInterval = 2 * time.Second
)

type outboxStore interface {
	DispatchBatch(ctx context.Context, limit int, dispatch func(storage.OutboxEvent) error) (int, error)
}

type Relay struct {
	store       outboxStore
	broadcaster chat.Broadcaster
}

func NewRelay(store *storage.Store, b chat.Broadcaster) *Relay {
	return &Relay{store: store, broadcaster: b}
}

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	log.Println("outbox relay started")

	for {
		if err := r.drain(ctx); err != nil && ctx.Err() == nil {
			log.Printf("outbox relay drain error | err=%v", err)
		}
		select {
		case <-ctx.Done():
			log.Println("outbox relay stopped")
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) drain(ctx context.Context) error {
	for {
		n, err := r.store.DispatchBatch(ctx, batchSize, func(e storage.OutboxEvent) error {
			r.broadcaster.Broadcast(e.RoomID, e.Message)
			return nil
		})
		if err != nil {
			return err
		}
		if n < batchSize {
			return nil
		}
	}
}
