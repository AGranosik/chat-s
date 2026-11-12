//go:build integration

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

	"naive-scale/internal/storage"
)

var (
	testDSN       string
	testStore     *storage.Store
	testRedisAddr string
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

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
		log.Printf("integration: cannot start postgres container | err=%v", err)
		return 1
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

	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		log.Printf("integration: cannot start redis container | err=%v", err)
		return 1
	}
	defer func() { _ = redisC.Terminate(ctx) }()
	redisAddr, err := redisC.Endpoint(ctx, "")
	if err != nil {
		log.Printf("integration: redis endpoint: %v", err)
		return 1
	}
	testRedisAddr = redisAddr

	return m.Run()
}

const (
	seedRoomID = "00000000-0000-0000-0000-000000000001"
	seedUserID = "00000000-0000-0000-0000-000000000001"
)

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

func countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := testStore.Pool().QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", query, err)
	}
	return n
}

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
