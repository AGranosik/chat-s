package outbox

import (
	"context"
	"errors"
	"log"
	"time"

	"chat-s/internal/chat"
	"chat-s/internal/storage"
)

const (
	notifyChannel = "outbox_events"
	batchSize     = 100
	pollInterval  = 2 * time.Second
)

// Relay drains the transactional outbox and hands each event to a Broadcaster.
// It is woken by Postgres LISTEN/NOTIFY for low latency, with a periodic poll as
// a crash-recovery safety net (see docs/ARCHITECTURE.md "The outbox").
type Relay struct {
	store       *storage.Store
	broadcaster chat.Broadcaster
}

func NewRelay(store *storage.Store, b chat.Broadcaster) *Relay {
	return &Relay{store: store, broadcaster: b}
}

// Run drives the relay until ctx is cancelled. On a connection-level error it
// reacquires a listen connection after a short backoff.
func (r *Relay) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := r.listenAndDrain(ctx); err != nil && ctx.Err() == nil {
			log.Printf("outbox relay error, reconnecting | err=%v", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	log.Println("outbox relay stopped")
}

func (r *Relay) listenAndDrain(ctx context.Context) error {
	conn, err := r.store.Pool().Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "listen "+notifyChannel); err != nil {
		return err
	}
	log.Println("outbox relay listening")

	// Drain anything already pending (e.g. rows left undispatched by a crash)
	// before we start waiting for new notifications.
	if err := r.drain(ctx); err != nil {
		return err
	}

	for {
		waitCtx, cancel := context.WithTimeout(ctx, pollInterval)
		_, err := conn.Conn().WaitForNotification(waitCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				return err // connection-level problem; reacquire in Run
			}
			// Deadline hit: this is the poll fallback — fall through and drain.
		}
		if err := r.drain(ctx); err != nil {
			return err
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
