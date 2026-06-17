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
