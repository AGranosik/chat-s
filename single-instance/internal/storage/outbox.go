package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"chat-s/internal/models"
)

// OutboxEvent is one undispatched row decoded for the relay.
type OutboxEvent struct {
	ID      int64
	RoomID  string
	Message models.Message
}

// EnqueueOutbox writes an outbox event within tx. payload is the JSON-encoded
// message to broadcast. Called in the same transaction as InsertMessage so the
// two commit atomically.
func EnqueueOutbox(ctx context.Context, tx pgx.Tx, roomID string, payload []byte) error {
	if _, err := tx.Exec(ctx,
		`insert into outbox (room_id, payload) values ($1, $2)`,
		roomID, payload,
	); err != nil {
		return fmt.Errorf("enqueue outbox: %w", err)
	}
	return nil
}

// FetchUndispatched returns up to limit undispatched events, oldest first, so
// the relay preserves per-room message order.
func (s *Store) FetchUndispatched(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := s.pool.Query(ctx,
		`select id, room_id, payload
		 from outbox
		 where dispatched_at is null
		 order by id
		 limit $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch outbox: %w", err)
	}
	defer rows.Close()

	var out []OutboxEvent
	for rows.Next() {
		var (
			e       OutboxEvent
			payload []byte
		)
		if err := rows.Scan(&e.ID, &e.RoomID, &payload); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		if err := json.Unmarshal(payload, &e.Message); err != nil {
			return nil, fmt.Errorf("decode outbox payload (id=%d): %w", e.ID, err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MarkDispatched stamps the given outbox rows as dispatched.
func (s *Store) MarkDispatched(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.pool.Exec(ctx,
		`update outbox set dispatched_at = now() where id = any($1)`,
		ids,
	); err != nil {
		return fmt.Errorf("mark dispatched: %w", err)
	}
	return nil
}

// DispatchBatch claims up to limit undispatched events, hands each to dispatch,
// stamps them dispatched, and commits — all in ONE transaction. It returns the
// number of events processed (0 when the outbox is empty).
//
// The claiming SELECT takes FOR UPDATE row locks and holds them across the
// dispatch hand-off until the commit, so persist → broadcast → mark is one atomic
// unit. Delivery stays at-least-once: if the process crashes (or dispatch/mark
// errors) before commit, the tx rolls back and the rows are picked up again on a
// later poll (clients de-dupe on message id).
func (s *Store) DispatchBatch(ctx context.Context, limit int, dispatch func(OutboxEvent) error) (int, error) {
	var processed int
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`select id, room_id, payload
			 from outbox
			 where dispatched_at is null
			 order by id
			 limit $1
			 for update`,
			limit,
		)
		if err != nil {
			return fmt.Errorf("claim outbox: %w", err)
		}

		// Materialise the batch, then CLOSE rows before running any further query
		// on this tx — pgx allows only one in-flight result set per connection, so
		// the UPDATE below would fail if rows were still open.
		var events []OutboxEvent
		for rows.Next() {
			var (
				e       OutboxEvent
				payload []byte
			)
			if err := rows.Scan(&e.ID, &e.RoomID, &payload); err != nil {
				rows.Close()
				return fmt.Errorf("scan outbox: %w", err)
			}
			if err := json.Unmarshal(payload, &e.Message); err != nil {
				rows.Close()
				return fmt.Errorf("decode outbox payload (id=%d): %w", e.ID, err)
			}
			events = append(events, e)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("claim outbox: %w", err)
		}
		if len(events) == 0 {
			return nil
		}

		ids := make([]int64, 0, len(events))
		for _, e := range events {
			// Blocking hand-off inside the tx: the claim (row lock) is held until
			// the rows are stamped and committed — never drop, never double-send.
			if err := dispatch(e); err != nil {
				return err
			}
			ids = append(ids, e.ID)
		}

		if _, err := tx.Exec(ctx,
			`update outbox set dispatched_at = now() where id = any($1)`, ids,
		); err != nil {
			return fmt.Errorf("mark dispatched: %w", err)
		}
		processed = len(events)
		return nil
	})
	return processed, err
}
