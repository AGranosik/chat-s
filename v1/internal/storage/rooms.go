package storage

import (
	"context"
	"fmt"

	"chat-s/internal/models"
)

// CreateRoom inserts a room and returns it with its generated id.
func (s *Store) CreateRoom(ctx context.Context, name string) (models.Room, error) {
	var r models.Room
	err := s.pool.QueryRow(ctx,
		`insert into rooms (name) values ($1)
		 returning id, name, created_at`,
		name,
	).Scan(&r.ID, &r.Name, &r.CreatedAt)
	if err != nil {
		return models.Room{}, fmt.Errorf("create room: %w", err)
	}
	return r, nil
}

// ListRooms returns all rooms, newest first.
func (s *Store) ListRooms(ctx context.Context) ([]models.Room, error) {
	rows, err := s.pool.Query(ctx,
		`select id, name, created_at from rooms order by created_at desc`)
	if err != nil {
		return nil, fmt.Errorf("list rooms: %w", err)
	}
	defer rows.Close()

	var out []models.Room
	for rows.Next() {
		var r models.Room
		if err := rows.Scan(&r.ID, &r.Name, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan room: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
