package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
	"outbox_relay/internal/postgres"
)

func TestProcessBatchPublishesBeforeMarking(t *testing.T) {
	repository, entry := newTestRepository(t)
	publisher := &testPublisher{
		publish: func(_ context.Context, topic, roomID string, event []byte) (int32, int64, error) {
			if len(repository.marked) != 0 {
				t.Fatal("outbox row was marked before Kafka acknowledged it")
			}
			if topic != "messages" || roomID != entry.RoomID {
				t.Fatalf("publish destination = %q/%q", topic, roomID)
			}
			if string(event) != string(entry.Event) {
				t.Fatal("published event differs from the outbox event")
			}
			return 4, 9, nil
		},
	}
	worker := newTestRelay(t, repository, publisher, 3)

	if err := worker.processBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 1 {
		t.Fatalf("publish calls = %d, want 1", publisher.calls)
	}
	if len(repository.marked) != 1 || repository.marked[0] != entry.MessageID {
		t.Fatalf("marked rows = %v, want [%s]", repository.marked, entry.MessageID)
	}
}

func TestProcessBatchRetriesPublishFailure(t *testing.T) {
	repository, entry := newTestRepository(t)
	attempt := 0
	publisher := &testPublisher{
		publish: func(_ context.Context, _, _ string, _ []byte) (int32, int64, error) {
			attempt++
			if attempt < 3 {
				return 0, 0, errors.New("temporary broker failure")
			}
			return 2, 5, nil
		},
	}
	worker := newTestRelay(t, repository, publisher, 3)
	worker.config.RetryBackoff = time.Nanosecond

	if err := worker.processBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 3 {
		t.Fatalf("publish calls = %d, want 3", publisher.calls)
	}
	if len(repository.marked) != 1 || repository.marked[0] != entry.MessageID {
		t.Fatalf("marked rows = %v, want [%s]", repository.marked, entry.MessageID)
	}
}

func TestProcessBatchLeavesRowPendingAfterRetriesExhausted(t *testing.T) {
	repository, _ := newTestRepository(t)
	publisher := &testPublisher{
		publish: func(_ context.Context, _, _ string, _ []byte) (int32, int64, error) {
			return 0, 0, errors.New("broker unavailable")
		},
	}
	worker := newTestRelay(t, repository, publisher, 2)
	worker.config.RetryBackoff = time.Nanosecond

	if err := worker.processBatch(context.Background()); err == nil {
		t.Fatal("processBatch() error = nil, want publish failure")
	}
	if publisher.calls != 2 {
		t.Fatalf("publish calls = %d, want 2", publisher.calls)
	}
	if len(repository.marked) != 0 {
		t.Fatalf("marked rows = %v, want none", repository.marked)
	}
}

type testRepository struct {
	entries []postgres.Entry
	marked  []string
}

func (r *testRepository) NextPending(_ context.Context, limit int) ([]postgres.Entry, error) {
	if len(r.entries) > limit {
		return append([]postgres.Entry(nil), r.entries[:limit]...), nil
	}
	return append([]postgres.Entry(nil), r.entries...), nil
}

func (r *testRepository) MarkPublished(_ context.Context, messageID string) error {
	r.marked = append(r.marked, messageID)
	return nil
}

type testPublisher struct {
	calls   int
	publish func(context.Context, string, string, []byte) (int32, int64, error)
}

func (p *testPublisher) Publish(ctx context.Context, topic, roomID string, event []byte) (int32, int64, error) {
	p.calls++
	return p.publish(ctx, topic, roomID, event)
}

func newTestRelay(t *testing.T, repository Repository, publisher *testPublisher, maxRetries int) *Relay {
	t.Helper()
	worker, err := New(repository, publisher, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Topic:        "messages",
		BatchSize:    10,
		MaxRetries:   maxRetries,
		RetryBackoff: time.Nanosecond,
		PollInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func newTestRepository(t *testing.T) (*testRepository, postgres.Entry) {
	t.Helper()
	event := messagev1.Event{
		Version:         messagev1.Version,
		MessageID:       "09c2df7e-c465-45e5-8b7f-dae2f7eb64ce",
		ClientMessageID: "client-message-1",
		RoomID:          "room-1",
		RoomSeq:         1,
		CreatedAt:       time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC),
		Payload:         json.RawMessage(`{"text":"hello"}`),
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	entry := postgres.Entry{
		MessageID: event.MessageID,
		RoomID:    event.RoomID,
		RoomSeq:   event.RoomSeq,
		Event:     eventJSON,
	}
	return &testRepository{entries: []postgres.Entry{entry}}, entry
}
