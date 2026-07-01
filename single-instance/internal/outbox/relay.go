package outbox

import (
	"context"
	"log"
	"time"

	"chat-s/internal/chat"
	"chat-s/internal/storage"
)

const (
	batchSize    = 100
	pollInterval = 2 * time.Second
)

// outboxStore is the subset of *storage.Store the relay needs. Narrowing it to
// an interface lets drain be unit-tested with a fake, no database required.
type outboxStore interface {
	FetchUndispatched(ctx context.Context, limit int) ([]storage.OutboxEvent, error)
	MarkDispatched(ctx context.Context, ids []int64) error
}

// Relay drains the transactional outbox and hands each event to a Broadcaster.
// It polls the outbox on a fixed interval — deliberately engine-agnostic, with
// no LISTEN/NOTIFY or other Postgres-specific signalling (see
// docs/ARCHITECTURE.md "The outbox"). The store just persists messages + outbox
// rows; this relay picks up whatever is undispatched and propagates it.
type Relay struct {
	store       outboxStore
	broadcaster chat.Broadcaster
}

func NewRelay(store *storage.Store, b chat.Broadcaster) *Relay {
	return &Relay{store: store, broadcaster: b}
}

// Run drives the relay until ctx is cancelled, draining the outbox once
// immediately and then on every poll tick. A drain error is logged and retried
// on the next tick rather than killing the loop.
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

// drain dispatches undispatched events in id order until the outbox is empty.
func (r *Relay) drain(ctx context.Context) error {
	for {
		events, err := r.store.FetchUndispatched(ctx, batchSize)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		ids := make([]int64, 0, len(events))
		for _, e := range events {
			// Blocking hand-off to the hub — never drop (matches the project's
			// "blocking consuming, not dropping messages" rule).
			r.broadcaster.Broadcast(e.RoomID, e.Message)
			ids = append(ids, e.ID)
		}
		// Mark dispatched only after the broadcast hand-off. A crash before this
		// re-dispatches on restart (at-least-once; clients de-dupe on message id).
		if err := r.store.MarkDispatched(ctx, ids); err != nil {
			return err
		}
		if len(events) < batchSize {
			return nil
		}
	}
}
