//go:build integration

// Integration tests for the outbox against a real Postgres, started as a
// throwaway container. The unit tests in outbox_test.go cover the control flow
// with a stubbed pgx; these cover what only the server can answer - `for update
// skip locked`, the undispatched filter, id ordering, and that a rollback
// really does leave rows claimable.
//
// Run with (Docker must be running):
//
//	go test -tags integration ./app/storage/...
//
// One container is shared by the whole package; each test gets its own schema
// so they stay isolated and can run in parallel.
package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"relayservice/app/storage"
	"relayservice/migrations"
)

const (
	// Matches the image compose runs, so the tests exercise the same server
	// version the relay is deployed against.
	postgresImage = "postgres:16"

	containerTimeout = 3 * time.Minute // generous: the first run may pull the image
	setupTimeout     = 15 * time.Second
	testTimeout      = 30 * time.Second
)

const roomA = "11111111-1111-1111-1111-111111111111"

// containerDSN points at the package-wide Postgres container started by TestMain.
var containerDSN string

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()

	container, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase("chat"),
		postgres.WithUsername("chat"),
		postgres.WithPassword("chat"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		// Nothing can run without it, and skipping would let a broken suite
		// look green.
		log.Printf("start %s container (is Docker running?): %v", postgresImage, err)
		os.Exit(1)
	}

	containerDSN, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("container connection string: %v", err)
		terminate(container)
		os.Exit(1)
	}

	code := m.Run()
	terminate(container)
	os.Exit(code)
}

func terminate(container testcontainers.Container) {
	if err := testcontainers.TerminateContainer(container); err != nil {
		log.Printf("terminate postgres container: %v", err)
	}
}

// newTestPool returns a pool pinned to a throwaway schema holding a fresh copy
// of the outbox table, dropped when the test ends. Tests get an empty table
// each and can run in parallel without seeing each other's rows.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()

	admin, err := pgxpool.New(ctx, containerDSN)
	if err != nil {
		t.Fatalf("connect to the postgres container: %v", err)
	}
	t.Cleanup(admin.Close)

	schema := testSchemaName(t)
	if _, err := admin.Exec(ctx, "create schema "+schema); err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
		defer cancel()
		if _, err := admin.Exec(ctx, "drop schema "+schema+" cascade"); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
	})

	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatalf("parse container DSN: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to schema %s: %v", schema, err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, outboxDDL(t)); err != nil {
		t.Fatalf("apply migration to %s: %v", schema, err)
	}

	return pool
}

// outboxDDL pulls the Up half out of the real migration, so these tests break
// if the schema drifts from what the relay is deployed against.
func outboxDDL(t *testing.T) string {
	t.Helper()

	raw, err := migrations.FS.ReadFile("0001_init.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}

	_, afterUp, ok := strings.Cut(string(raw), "-- +goose Up")
	if !ok {
		t.Fatal("0001_init.sql has no `-- +goose Up` marker")
	}
	up, _, ok := strings.Cut(afterUp, "-- +goose Down")
	if !ok {
		t.Fatal("0001_init.sql has no `-- +goose Down` marker")
	}
	return up
}

// testSchemaName builds a unique, quote-free identifier that stays inside
// Postgres' 63-byte limit.
func testSchemaName(t *testing.T) string {
	var sanitized strings.Builder
	for _, r := range strings.ToLower(t.Name()) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sanitized.WriteRune(r)
		} else {
			sanitized.WriteByte('_')
		}
	}

	name := sanitized.String()
	if len(name) > 24 {
		name = name[:24]
	}
	return fmt.Sprintf("outbox_test_%s_%d", name, time.Now().UnixNano())
}

func seedEvent(t *testing.T, pool *pgxpool.Pool, roomID, payload string) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()

	var id int64
	err := pool.QueryRow(ctx,
		`insert into outbox (room_id, payload) values ($1, $2) returning id`,
		roomID, []byte(payload),
	).Scan(&id)
	if err != nil {
		t.Fatalf("seed outbox row: %v", err)
	}
	return id
}

func markDispatched(t *testing.T, pool *pgxpool.Pool, id int64) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()

	if _, err := pool.Exec(ctx, `update outbox set dispatched_at = now() where id = $1`, id); err != nil {
		t.Fatalf("mark row %d dispatched: %v", id, err)
	}
}

// undispatchedIDs is the state DispatchBatch is really manipulating: which rows
// a later run would still pick up.
func undispatchedIDs(t *testing.T, pool *pgxpool.Pool) []int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()

	rows, err := pool.Query(ctx, `select id from outbox where dispatched_at is null order by id`)
	if err != nil {
		t.Fatalf("query undispatched: %v", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan undispatched: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read undispatched: %v", err)
	}
	return ids
}

func idsOf(events []storage.OutboxEvent) []int64 {
	ids := make([]int64, 0, len(events))
	for _, e := range events {
		ids = append(ids, e.ID)
	}
	return ids
}

func collect(into *[]storage.OutboxEvent) func(context.Context, []storage.OutboxEvent) error {
	return func(_ context.Context, events []storage.OutboxEvent) error {
		*into = append(*into, events...)
		return nil
	}
}

func TestDispatchBatchClaimsOldestUndispatchedRows(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t)
	store := storage.New(pool)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	alreadySent := seedEvent(t, pool, roomA, `{"seq":0}`)
	markDispatched(t, pool, alreadySent)

	first := seedEvent(t, pool, roomA, `{"seq":1}`)
	second := seedEvent(t, pool, roomA, `{"seq":2}`)
	third := seedEvent(t, pool, roomA, `{"seq":3}`)

	var got []storage.OutboxEvent
	n, err := store.DispatchBatch(ctx, 2, collect(&got))
	if err != nil {
		t.Fatalf("DispatchBatch error = %v, want nil", err)
	}
	if n != 2 {
		t.Errorf("DispatchBatch n = %d, want 2 (the batch size caps the claim)", n)
	}
	if want := []int64{first, second}; !slices.Equal(idsOf(got), want) {
		t.Errorf("dispatched ids %v, want %v - oldest undispatched first, already-sent rows skipped", idsOf(got), want)
	}
	if want := []int64{third}; !slices.Equal(undispatchedIDs(t, pool), want) {
		t.Errorf("undispatched ids %v, want %v", undispatchedIDs(t, pool), want)
	}

	// The next run picks up where this one stopped, and then drains.
	got = nil
	n, err = store.DispatchBatch(ctx, 2, collect(&got))
	if err != nil {
		t.Fatalf("second DispatchBatch error = %v, want nil", err)
	}
	if n != 1 {
		t.Errorf("second DispatchBatch n = %d, want 1", n)
	}
	if want := []int64{third}; !slices.Equal(idsOf(got), want) {
		t.Errorf("second run dispatched ids %v, want %v", idsOf(got), want)
	}
	if remaining := undispatchedIDs(t, pool); len(remaining) != 0 {
		t.Errorf("undispatched ids %v, want none", remaining)
	}
}

func TestDispatchBatchRoundTripsRoomIDAndPayload(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t)
	store := storage.New(pool)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Key order and spacing are deliberate: jsonb would reorder these keys and
	// strip the spaces, so this literal fails if the column ever goes back.
	const payload = `{"from": "adrian", "body": "hello"}`
	id := seedEvent(t, pool, roomA, payload)

	var got []storage.OutboxEvent
	if _, err := store.DispatchBatch(ctx, 10, collect(&got)); err != nil {
		t.Fatalf("DispatchBatch error = %v, want nil", err)
	}
	if len(got) != 1 {
		t.Fatalf("dispatched %d events, want 1", len(got))
	}

	if got[0].ID != id {
		t.Errorf("event id = %d, want %d", got[0].ID, id)
	}
	if got[0].RoomID != roomA {
		t.Errorf("event room id = %q, want %q", got[0].RoomID, roomA)
	}
	// bytea round-trips verbatim, which is what lets the relay promise
	// subscribers the exact bytes the producer wrote.
	if !bytes.Equal(got[0].Payload, []byte(payload)) {
		t.Errorf("event payload = %q, want %q", got[0].Payload, payload)
	}
}

func TestDispatchBatchWithEmptyOutbox(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t)
	store := storage.New(pool)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	dispatched := false
	n, err := store.DispatchBatch(ctx, 10, func(context.Context, []storage.OutboxEvent) error {
		dispatched = true
		return nil
	})
	if err != nil {
		t.Fatalf("DispatchBatch error = %v, want nil", err)
	}
	if n != 0 {
		t.Errorf("DispatchBatch n = %d, want 0", n)
	}
	if dispatched {
		t.Error("dispatch was called with an empty outbox")
	}
}

// A failed fanout must leave the rows exactly as it found them, otherwise the
// outbox loses messages instead of retrying them.
func TestDispatchBatchLeavesRowsClaimableWhenDispatchFails(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t)
	store := storage.New(pool)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	first := seedEvent(t, pool, roomA, `{"seq":1}`)
	second := seedEvent(t, pool, roomA, `{"seq":2}`)

	wantErr := errors.New("fanout unavailable")
	if _, err := store.DispatchBatch(ctx, 10, func(context.Context, []storage.OutboxEvent) error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("DispatchBatch error = %v, want it to wrap %v", err, wantErr)
	}

	if want := []int64{first, second}; !slices.Equal(undispatchedIDs(t, pool), want) {
		t.Fatalf("undispatched ids %v, want %v - the failed batch must roll back", undispatchedIDs(t, pool), want)
	}

	var got []storage.OutboxEvent
	n, err := store.DispatchBatch(ctx, 10, collect(&got))
	if err != nil {
		t.Fatalf("retry DispatchBatch error = %v, want nil", err)
	}
	if n != 2 {
		t.Errorf("retry DispatchBatch n = %d, want 2", n)
	}
	if want := []int64{first, second}; !slices.Equal(idsOf(got), want) {
		t.Errorf("retry dispatched ids %v, want %v", idsOf(got), want)
	}
}

// The whole point of `for update skip locked`: a second relay instance running
// at the same time claims different rows instead of blocking on, or duplicating,
// the first one's batch.
func TestDispatchBatchSkipsRowsHeldByAConcurrentWorker(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t)
	store := storage.New(pool)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	var seeded []int64
	for i := range 4 {
		seeded = append(seeded, seedEvent(t, pool, roomA, fmt.Sprintf(`{"seq":%d}`, i)))
	}

	claimed := make(chan struct{}) // first worker is inside its transaction
	release := make(chan struct{}) // let the first worker commit
	firstDone := make(chan error, 1)

	var firstIDs []int64
	go func() {
		_, err := store.DispatchBatch(ctx, 2, func(_ context.Context, events []storage.OutboxEvent) error {
			firstIDs = idsOf(events)
			close(claimed)
			<-release
			return nil
		})
		firstDone <- err
	}()

	select {
	case <-claimed:
	case err := <-firstDone:
		t.Fatalf("first DispatchBatch returned before claiming rows: %v", err)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the first worker to claim its batch")
	}

	// Runs while the first worker still holds its row locks.
	var secondEvents []storage.OutboxEvent
	n, err := store.DispatchBatch(ctx, 2, collect(&secondEvents))
	if err != nil {
		t.Fatalf("concurrent DispatchBatch error = %v, want nil", err)
	}
	if n != 2 {
		t.Fatalf("concurrent DispatchBatch n = %d, want 2 - it should skip the locked rows, not block on them", n)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first DispatchBatch error = %v, want nil", err)
	}

	second := idsOf(secondEvents)
	for _, id := range second {
		if slices.Contains(firstIDs, id) {
			t.Errorf("id %d was claimed by both workers (first=%v second=%v) - it would be fanned out twice", id, firstIDs, second)
		}
	}

	all := slices.Concat(firstIDs, second)
	slices.Sort(all)
	if !slices.Equal(all, seeded) {
		t.Errorf("the two workers claimed %v between them, want %v", all, seeded)
	}
	if remaining := undispatchedIDs(t, pool); len(remaining) != 0 {
		t.Errorf("undispatched ids %v, want none once both workers commit", remaining)
	}
}

func TestDispatchBatchLeavesOutboxUntouchedForInvalidBatchSize(t *testing.T) {
	t.Parallel()

	pool := newTestPool(t)
	store := storage.New(pool)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	id := seedEvent(t, pool, roomA, `{"seq":1}`)

	if _, err := store.DispatchBatch(ctx, 0, collect(new([]storage.OutboxEvent))); err == nil {
		t.Fatal("DispatchBatch error = nil, want an error")
	}
	if want := []int64{id}; !slices.Equal(undispatchedIDs(t, pool), want) {
		t.Errorf("undispatched ids %v, want %v", undispatchedIDs(t, pool), want)
	}
}
