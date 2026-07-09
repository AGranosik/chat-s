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

// outboxStore is the subset of *storage.Store the relay needs. Narrowing it to
// an interface lets drain be unit-tested with a fake, no database required.
type outboxStore interface {
	DispatchBatch(ctx context.Context, limit int, dispatch func(storage.OutboxEvent) error) (int, error)
}

// Relay drains the transactional outbox and hands each event to a Broadcaster.
// It polls the outbox on a fixed interval — deliberately engine-agnostic, with
// no LISTEN/NOTIFY or other Postgres-specific signalling. The store just
// persists messages + outbox rows; this relay picks up whatever is undispatched
// and propagates it.
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
// Each batch is claimed with FOR UPDATE SKIP LOCKED and marked dispatched in one
// transaction (see storage.DispatchBatch), so the relays running on the other
// instances never re-broadcast a row this one already handled — the fix for the
// duplicate-delivery the load test surfaced. The broadcast hand-off happens
// inside that transaction, before the commit, keeping delivery at-least-once.
func (r *Relay) drain(ctx context.Context) error {
	for {
		n, err := r.store.DispatchBatch(ctx, batchSize, func(e storage.OutboxEvent) error {
			// Blocking hand-off to the hub — never drop (matches the project's
			// "blocking consuming, not dropping messages" rule).
			r.broadcaster.Broadcast(e.RoomID, e.Message)
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
