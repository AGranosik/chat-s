package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"naive-scale/internal/models"
)

type OutboxEvent struct {
	ID      int64
	RoomID  string
	Message models.Message
}

func EnqueueOutbox(ctx context.Context, tx pgx.Tx, roomID string, payload []byte) error {
	if _, err := tx.Exec(ctx,
		`insert into outbox (room_id, payload) values ($1, $2)`,
		roomID, payload,
	); err != nil {
		return fmt.Errorf("enqueue outbox: %w", err)
	}
	return nil
}

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

func (s *Store) DispatchBatch(ctx context.Context, limit int, dispatch func(OutboxEvent) error) (int, error) {
	var processed int
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`select id, room_id, payload
			 from outbox
			 where dispatched_at is null
			 order by id
			 limit $1
			 for update skip locked`,
			limit,
		)
		if err != nil {
			return fmt.Errorf("claim outbox: %w", err)
		}

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
