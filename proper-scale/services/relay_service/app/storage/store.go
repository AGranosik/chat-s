package storage

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// txBeginner is the only part of *pgxpool.Pool the store actually uses. Keeping
// the field an interface lets the outbox logic be driven by a stub in tests
// without a live database; production still builds a Store from a real pool.
type txBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Store struct {
	pool txBeginner
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}
