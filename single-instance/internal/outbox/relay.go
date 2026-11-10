package outbox

import (
	"context"
	"log"
	"time"

	"chat-s/internal/chat"
	"chat-s/internal/storage"
)

const (
	batchSize    = 500
	pollInterval = time.Second
)

type outboxStore interface {
	DispatchBatch(ctx context.Context, limit int, dispatch func(storage.OutboxEvent) error) (int, error)
}

type Relay struct {
	store        outboxStore
	broadcaster  chat.Broadcaster
	pollInterval time.Duration
}

func NewRelay(store *storage.Store, b chat.Broadcaster) *Relay {
	return &Relay{store: store, broadcaster: b, pollInterval: pollInterval}
}

func (r *Relay) Run(ctx context.Context) {
	log.Println("outbox relay started")

	for {
		if err := r.drain(ctx); err != nil && ctx.Err() == nil {
			log.Printf("outbox relay drain error | err=%v", err)
		}
		timer := time.NewTimer(r.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			log.Println("outbox relay stopped")
			return
		case <-timer.C:
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
