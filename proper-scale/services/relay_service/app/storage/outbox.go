package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type OutboxEvent struct {
	ID      int64
	RoomID  string
	Payload []byte
}

const rollbackTimeout = 5 * time.Second

func (s *Store) DispatchBatch(ctx context.Context, batchSize int, dispatch func(context.Context, []OutboxEvent) error) (int, error) {

	if batchSize <= 0 {
		return 0, fmt.Errorf("batch size must be positive, got %d", batchSize)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	rows, err := tx.Query(ctx,
		`select id, room_id, payload
			 from outbox
			 where dispatched_at is null
			 order by id
			 limit $1
			 for update skip locked`,
		batchSize)

	if err != nil {
		return 0, fmt.Errorf("claim outbox: %w", err)
	}

	events, err := pgx.AppendRows(make([]OutboxEvent, 0, batchSize), rows, pgx.RowToStructByName[OutboxEvent])
	if err != nil {
		return 0, fmt.Errorf("scan outbox: %w", err)
	}

	if len(events) == 0 {
		return 0, nil
	}

	if err := dispatch(ctx, events); err != nil {
		return 0, fmt.Errorf("dispatch outbox batch of %d: %w", len(events), err)
	}

	ids := make([]int64, 0, len(events))

	for _, e := range events {
		ids = append(ids, e.ID)
	}

	if _, err := tx.Exec(ctx,
		`update outbox
			 set dispatched_at = now()
			 where id = any($1) and dispatched_at is null`,
		ids,
	); err != nil {
		return 0, fmt.Errorf("mark dispatched: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox batch: %w", err)
	}

	return len(events), nil
}
