//go:build integration

// Package integration holds end-to-end tests that run against a real Postgres,
// spun up once per package via testcontainers. They exercise the seams the
// unit tests fake out: real SQL, the transactional outbox, the LISTEN/NOTIFY
// relay, and a full websocket round-trip. Run with:
//
//	go test -tags=integration ./internal/integration/...
//
// Requires a running Docker daemon (Docker Desktop on Windows).
package integration

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"chat-s/internal/storage"
)

// Shared across the package: one container, one pool, migrations applied once.
// Per-test isolation comes from freshDB (truncate + reseed), not new containers.
var (
	testDSN   string
	testStore *storage.Store
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run owns the container lifecycle so deferred cleanup runs before os.Exit.
func run(m *testing.M) int {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("chat"),
		tcpostgres.WithUsername("chat"),
		tcpostgres.WithPassword("chat"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		// No Docker / can't pull image: skip the whole suite rather than fail,
		// so `go test -tags=integration ./...` is green on machines without it.
		log.Printf("integration: cannot start postgres container, skipping | err=%v", err)
		return 0
	}
	defer func() { _ = container.Terminate(ctx) }()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("integration: connection string: %v", err)
		return 1
	}
	testDSN = dsn

	if err := storage.Migrate(ctx, dsn); err != nil {
		log.Printf("integration: migrate: %v", err)
		return 1
	}
	pool, err := storage.Connect(ctx, dsn)
	if err != nil {
		log.Printf("integration: connect: %v", err)
		return 1
	}
	defer pool.Close()
	testStore = storage.New(pool)

	return m.Run()
}

// Fixed seed UUIDs, matching migrations/0001 so FK inserts always have a parent.
const (
	seedRoomID = "00000000-0000-0000-0000-000000000001"
	seedUserID = "00000000-0000-0000-0000-000000000001"
)

// freshDB resets message/outbox state between tests and guarantees the seed
// room+user exist (the migration inserts them; truncate leaves them intact, but
// reseeding makes each test self-contained even if a prior test removed them).
func freshDB(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := testStore.Pool().Exec(ctx,
		`truncate messages, outbox restart identity`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := testStore.Pool().Exec(ctx,
		`insert into rooms (id, name) values ($1, 'general')
		 on conflict (id) do nothing`, seedRoomID); err != nil {
		t.Fatalf("seed room: %v", err)
	}
	if _, err := testStore.Pool().Exec(ctx,
		`insert into users (id, username) values ($1, 'demo')
		 on conflict (id) do nothing`, seedUserID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
}

// countRows is a tiny query helper used across tests.
func countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := testStore.Pool().QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", query, err)
	}
	return n
}

// eventually polls fn until it returns nil or the timeout elapses. Used instead
// of fixed sleeps so assertions tolerate both the LISTEN/NOTIFY fast path and
// the relay's ~2s poll fallback without being flaky.
func eventually(t *testing.T, timeout time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if last = fn(); last == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %v", timeout, last)
}
