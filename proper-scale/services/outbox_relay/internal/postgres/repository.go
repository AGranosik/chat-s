package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Entry struct {
	MessageID string
	RoomID    string
	RoomSeq   int64
	Event     []byte
}

type Repository struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Repository, error) {
	if databaseURL == "" {
		return nil, errors.New("database URL is required")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() {
	r.pool.Close()
}

func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *Repository) NextPending(ctx context.Context, limit int) ([]Entry, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT pending.message_id::text, pending.room_id, pending.room_seq, pending.event
		FROM message_outbox AS pending
		WHERE pending.published_at IS NULL
		  AND NOT EXISTS (
			SELECT 1
			FROM message_outbox AS previous
			WHERE previous.room_id = pending.room_id
			  AND previous.published_at IS NULL
			  AND previous.room_seq < pending.room_seq
		  )
		ORDER BY pending.created_at ASC, pending.room_id ASC, pending.room_seq ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending outbox rows: %w", err)
	}
	defer rows.Close()

	entries := make([]Entry, 0, limit)
	for rows.Next() {
		var entry Entry
		if err := rows.Scan(&entry.MessageID, &entry.RoomID, &entry.RoomSeq, &entry.Event); err != nil {
			return nil, fmt.Errorf("scan pending outbox row: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending outbox rows: %w", err)
	}
	return entries, nil
}

func (r *Repository) MarkPublished(ctx context.Context, messageID string) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE message_outbox
		SET published_at = NOW()
		WHERE message_id = $1::uuid AND published_at IS NULL`, messageID)
	if err != nil {
		return fmt.Errorf("mark outbox row published: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("mark outbox row %s published: row was no longer pending", messageID)
	}
	return nil
}
