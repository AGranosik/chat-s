package storage

import (
	"context"
	"fmt"

	"chat-s/internal/models"
)

// CreateUser inserts a user and returns it with its generated id.
func (s *Store) CreateUser(ctx context.Context, username string) (models.User, error) {
	var u models.User
	err := s.pool.QueryRow(ctx,
		`insert into users (username) values ($1)
		 returning id, username, created_at`,
		username,
	).Scan(&u.ID, &u.Username, &u.CreatedAt)
	if err != nil {
		return models.User{}, fmt.Errorf("create user: %w", err)
	}
	return u, nil
}

// ListUsers returns all users, newest first.
func (s *Store) ListUsers(ctx context.Context) ([]models.User, error) {
	rows, err := s.pool.Query(ctx,
		`select id, username, created_at from users order by created_at desc`)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	var out []models.User
	for rows.Next() {
		var u models.User
		if err := rows.Scan(&u.ID, &u.Username, &u.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
