package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"naive-scale/internal/models"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func (s *Store) WithTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func InsertMessage(ctx context.Context, tx pgx.Tx, m *models.Message) error {
	err := tx.QueryRow(ctx,
		`insert into messages (room_id, user_id, body)
		 values ($1, $2, $3)
		 returning id, created_at`,
		m.RoomID, m.UserID, m.Body,
	).Scan(&m.ID, &m.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert message: %w", err)
	}
	return nil
}

func (s *Store) History(ctx context.Context, roomID string, before int64, limit int) ([]models.Message, error) {
	rows, err := s.pool.Query(ctx,
		`select id, room_id, user_id, body, created_at
		 from messages
		 where room_id = $1 and ($2 = 0 or id < $2)
		 order by id desc
		 limit $3`,
		roomID, before, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	defer rows.Close()

	var out []models.Message
	for rows.Next() {
		var m models.Message
		if err := rows.Scan(&m.ID, &m.RoomID, &m.UserID, &m.Body, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
