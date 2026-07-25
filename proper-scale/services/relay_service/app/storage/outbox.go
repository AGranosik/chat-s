package storage

import (
	"context"
	"fmt"
)

type OutboxEvent struct {
	ID      int64
	RoomID  string
	Payload []byte
}

func (s *Store) DispatchBatch(ctx context.Context, batchSize int, dispatch func([]OutboxEvent) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx,
		`select id, room_id, payload
			 from outbox
			 where dispatched_at is null
			 order by id
			 limit $1`,
		batchSize)

	if err != nil {
		return fmt.Errorf("claim outbox: %w", err)
	}

	events := make([]OutboxEvent, 0, batchSize)

	for rows.Next() {
		var event OutboxEvent

		if err := rows.Scan(&event.ID, &event.RoomID, &event.Payload); err != nil {
			rows.Close()
			return fmt.Errorf("scan outbox: %w", err)
		}

		events = append(events, event)
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
		ids = append(ids, e.ID)
	}

	if err := dispatch(events); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx,
		`update outbox set dispatched_at = now() where id = any($1)`, ids,
	); err != nil {
		return fmt.Errorf("mark dispatched: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}
