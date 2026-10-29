package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStoreAcceptIdempotencyPersistsOutbox(t *testing.T) {
	pool := newIsolatedTestPool(t)
	store, err := NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	firstRequest := sendRequest("room-1", "client-key-1", `{"text":"first"}`)
	first, err := store.Accept(ctx, "client-1", firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	if first.RoomSeq != 1 {
		t.Fatalf("first accept sequence = %d, want 1", first.RoomSeq)
	}

	retry, err := store.Accept(ctx, "client-1", sendRequest("room-1", "client-key-1", `{"text":"changed"}`))
	if err != nil {
		t.Fatal(err)
	}
	if retry.MessageID != first.MessageID || retry.RoomSeq != first.RoomSeq || string(retry.Payload) != string(first.Payload) {
		t.Fatalf("retry = %+v; want original event %+v", retry, first)
	}

	second, err := store.Accept(ctx, "client-1", sendRequest("room-1", "client-key-2", `{"text":"second"}`))
	if err != nil {
		t.Fatal(err)
	}
	if second.RoomSeq != 2 {
		t.Fatalf("second accept sequence = %d, want 2", second.RoomSeq)
	}

	var messages, outbox int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM messages`).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM message_outbox`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if messages != 2 || outbox != 2 {
		t.Fatalf("messages/outbox rows = %d/%d, want 2/2", messages, outbox)
	}
	var outboxSeq int64
	var eventBytes []byte
	if err := pool.QueryRow(ctx, `SELECT room_seq, event FROM message_outbox WHERE message_id::text = $1`, first.MessageID).Scan(&outboxSeq, &eventBytes); err != nil {
		t.Fatal(err)
	}
	var outboxEvent messagev1.Event
	if err := json.Unmarshal(eventBytes, &outboxEvent); err != nil {
		t.Fatal(err)
	}
	if outboxSeq != first.RoomSeq || outboxEvent.RoomSeq != first.RoomSeq || outboxEvent.MessageID != first.MessageID {
		t.Fatalf("outbox event = %+v with sequence %d, want first event %+v", outboxEvent, outboxSeq, first)
	}
}

func TestStoreAcceptRollbackLeavesNoRows(t *testing.T) {
	pool := newIsolatedTestPool(t)
	store, err := NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = pool.Exec(ctx, `
		CREATE FUNCTION fail_outbox_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.room_id = 'rollback-room' THEN
				RAISE EXCEPTION 'intentional test failure';
			END IF;
			RETURN NEW;
		END
		$$`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER fail_outbox_insert BEFORE INSERT ON message_outbox FOR EACH ROW EXECUTE FUNCTION fail_outbox_insert()`); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Accept(ctx, "client-1", sendRequest("rollback-room", "client-key-1", `{"text":"fails"}`)); err == nil {
		t.Fatal("Accept() error = nil, want outbox failure")
	}
	for _, table := range []string{"messages", "message_outbox", "room_counters"} {
		var count int
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		if err := pool.QueryRow(ctx, query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s rows after rollback = %d, want 0", table, count)
		}
	}
}

func newIsolatedTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("MESSAGE_TEST_DATABASE_URL")
	if strings.TrimSpace(dsn) == "" {
		t.Skip("MESSAGE_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	adminPool, err := NewPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schemaID, err := newMessageID()
	if err != nil {
		adminPool.Close()
		t.Fatal(err)
	}
	schema := "message_test_" + strings.ReplaceAll(schemaID, "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		adminPool.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
		t.Fatal(err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
		t.Fatal(err)
	}
	migrationDir := filepath.Join("..", "..", "migrations")
	if err := RunMigrations(ctx, pool, migrationDir); err != nil {
		pool.Close()
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool, migrationDir); err != nil {
		pool.Close()
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		adminPool.Close()
	})
	return pool
}

func sendRequest(roomID, clientMessageID, payload string) messagev1.SendRequest {
	return messagev1.SendRequest{
		Type:            messagev1.SendFrameType,
		Version:         messagev1.Version,
		ClientMessageID: clientMessageID,
		RoomID:          roomID,
		Payload:         json.RawMessage(payload),
	}
}
