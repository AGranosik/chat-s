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
		SELECT message_id::text, room_id, room_seq, event
		FROM message_outbox
		WHERE published_at IS NULL
		ORDER BY outbox_id
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

func (r *Repository) MarkPublished(ctx context.Context, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	result, err := r.pool.Exec(ctx, `
		UPDATE message_outbox
		SET published_at = NOW()
		WHERE message_id = ANY($1::text[]::uuid[]) AND published_at IS NULL`, messageIDs)
	if err != nil {
		return fmt.Errorf("mark outbox rows published: %w", err)
	}
	if result.RowsAffected() != int64(len(messageIDs)) {
		return fmt.Errorf("mark outbox rows published: %d of %d rows were still pending", result.RowsAffected(), len(messageIDs))
	}
	return nil
}
