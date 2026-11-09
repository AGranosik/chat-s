package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
	"outbox_relay/internal/kafka"
	"outbox_relay/internal/postgres"
)

func TestProcessBatchPublishesWholeBatchBeforeMarking(t *testing.T) {
	entries := []postgres.Entry{
		newEntry(t, "room-1", 1),
		newEntry(t, "room-2", 1),
		newEntry(t, "room-1", 2),
	}
	repository := &testRepository{entries: entries}
	publisher := &testPublisher{
		publish: func(topic string, messages []kafka.Message) []error {
			if len(repository.marked) != 0 {
				t.Fatal("outbox rows were marked before Kafka acknowledged them")
			}
			if topic != "messages" {
				t.Fatalf("topic = %q, want messages", topic)
			}
			return make([]error, len(messages))
		},
	}
	worker := newTestRelay(t, repository, publisher, 3)

	processed, err := worker.processBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if processed != 3 {
		t.Fatalf("processed = %d, want 3", processed)
	}
	if len(publisher.batches) != 1 {
		t.Fatalf("publish calls = %d, want 1", len(publisher.batches))
	}
	got := publisher.batches[0]
	for i, entry := range entries {
		if got[i].Key != entry.RoomID || string(got[i].Value) != string(entry.Event) {
			t.Fatalf("message %d = %s/%s, want %s/%s", i, got[i].Key, got[i].Value, entry.RoomID, entry.Event)
		}
	}
	if len(repository.marked) != 1 {
		t.Fatalf("mark calls = %d, want 1", len(repository.marked))
	}
	if want := ids(entries...); !slices.Equal(repository.marked[0], want) {
		t.Fatalf("marked rows = %v, want %v", repository.marked[0], want)
	}
}

func TestProcessBatchEmptyOutboxDoesNothing(t *testing.T) {
	repository := &testRepository{}
	publisher := &testPublisher{publish: func(string, []kafka.Message) []error {
		t.Fatal("publish must not be called for an empty outbox")
		return nil
	}}
	worker := newTestRelay(t, repository, publisher, 3)

	processed, err := worker.processBatch(context.Background())
	if err != nil || processed != 0 {
		t.Fatalf("processBatch() = %d, %v; want 0, nil", processed, err)
	}
	if len(repository.marked) != 0 {
		t.Fatalf("mark calls = %d, want 0", len(repository.marked))
	}
}

func TestProcessBatchRetriesFailedRowAndLaterRowsOfItsRoom(t *testing.T) {
	room1First := newEntry(t, "room-1", 1)
	room1Second := newEntry(t, "room-1", 2)
	room2 := newEntry(t, "room-2", 1)
	repository := &testRepository{entries: []postgres.Entry{room1First, room2, room1Second}}
	publisher := &testPublisher{
		publish: func(_ string, messages []kafka.Message) []error {
			results := make([]error, len(messages))
			if len(messages) == 3 {
				results[0] = errors.New("temporary broker failure")
			}
			return results
		},
	}
	worker := newTestRelay(t, repository, publisher, 3)

	if _, err := worker.processBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(publisher.batches) != 2 {
		t.Fatalf("publish calls = %d, want 2", len(publisher.batches))
	}
	retried := publisher.batches[1]
	if len(retried) != 2 || string(retried[0].Value) != string(room1First.Event) || string(retried[1].Value) != string(room1Second.Event) {
		t.Fatalf("retried messages = %v, want room-1 seq 1 then seq 2", retried)
	}
	if want := ids(room2, room1First, room1Second); !slices.Equal(repository.marked[0], want) {
		t.Fatalf("marked rows = %v, want %v", repository.marked[0], want)
	}
}

func TestProcessBatchLeavesFailedRoomPendingAfterRetriesExhausted(t *testing.T) {
	room1 := newEntry(t, "room-1", 1)
	room1Later := newEntry(t, "room-1", 2)
	room2 := newEntry(t, "room-2", 1)
	repository := &testRepository{entries: []postgres.Entry{room1, room2, room1Later}}
	publisher := &testPublisher{
		publish: func(_ string, messages []kafka.Message) []error {
			results := make([]error, len(messages))
			for i, message := range messages {
				if message.Key == "room-1" && string(message.Value) == string(room1.Event) {
					results[i] = errors.New("broker unavailable")
				}
			}
			return results
		},
	}
	worker := newTestRelay(t, repository, publisher, 2)

	if _, err := worker.processBatch(context.Background()); err == nil {
		t.Fatal("processBatch() error = nil, want publish failure")
	}
	if len(publisher.batches) != 2 {
		t.Fatalf("publish calls = %d, want 2", len(publisher.batches))
	}
	if len(repository.marked) != 1 {
		t.Fatalf("mark calls = %d, want 1", len(repository.marked))
	}
	if want := ids(room2); !slices.Equal(repository.marked[0], want) {
		t.Fatalf("marked rows = %v, want %v", repository.marked[0], want)
	}
}

func TestProcessBatchMarksNothingWhenWholeBatchFails(t *testing.T) {
	repository := &testRepository{entries: []postgres.Entry{newEntry(t, "room-1", 1), newEntry(t, "room-2", 1)}}
	publisher := &testPublisher{
		publish: func(_ string, messages []kafka.Message) []error {
			results := make([]error, len(messages))
			for i := range results {
				results[i] = errors.New("broker unavailable")
			}
			return results
		},
	}
	worker := newTestRelay(t, repository, publisher, 3)

	if _, err := worker.processBatch(context.Background()); err == nil {
		t.Fatal("processBatch() error = nil, want publish failure")
	}
	if len(publisher.batches) != 3 {
		t.Fatalf("publish calls = %d, want 3", len(publisher.batches))
	}
	if len(repository.marked) != 0 {
		t.Fatalf("mark calls = %d, want 0", len(repository.marked))
	}
}

func TestProcessBatchReturnsMarkFailure(t *testing.T) {
	repository := &testRepository{
		entries: []postgres.Entry{newEntry(t, "room-1", 1)},
		markErr: errors.New("database unavailable"),
	}
	publisher := &testPublisher{
		publish: func(_ string, messages []kafka.Message) []error {
			return make([]error, len(messages))
		},
	}
	worker := newTestRelay(t, repository, publisher, 3)

	if _, err := worker.processBatch(context.Background()); err == nil {
		t.Fatal("processBatch() error = nil, want mark failure")
	}
}

func TestSplitResultsTreatsMissingResultsAsFailures(t *testing.T) {
	entries := []postgres.Entry{newEntry(t, "room-1", 1), newEntry(t, "room-2", 1)}

	succeeded, retry, err := splitResults(entries, []error{nil})

	if len(succeeded) != 1 || succeeded[0].RoomID != "room-1" {
		t.Fatalf("succeeded = %v, want room-1", succeeded)
	}
	if len(retry) != 1 || retry[0].RoomID != "room-2" || err == nil {
		t.Fatalf("retry = %v, err = %v; want room-2 and an error", retry, err)
	}
}

type testRepository struct {
	entries []postgres.Entry
	marked  [][]string
	markErr error
}

func (r *testRepository) NextPending(_ context.Context, _ []int, limit int) ([]postgres.Entry, error) {
	if len(r.entries) > limit {
		return append([]postgres.Entry(nil), r.entries[:limit]...), nil
	}
	return append([]postgres.Entry(nil), r.entries...), nil
}

func (r *testRepository) MarkPublished(_ context.Context, messageIDs []string) error {
	if r.markErr != nil {
		return r.markErr
	}
	r.marked = append(r.marked, append([]string(nil), messageIDs...))
	return nil
}

type testPublisher struct {
	batches [][]kafka.Message
	publish func(string, []kafka.Message) []error
}

func (p *testPublisher) PublishBatch(_ context.Context, topic string, messages []kafka.Message) []error {
	p.batches = append(p.batches, append([]kafka.Message(nil), messages...))
	return p.publish(topic, messages)
}

func newTestRelay(t *testing.T, repository Repository, publisher *testPublisher, maxRetries int) *Relay {
	t.Helper()
	worker, err := New(repository, publisher, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Topic:        "messages",
		Buckets:      []int{0},
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

func newEntry(t *testing.T, roomID string, roomSeq int64) postgres.Entry {
	t.Helper()
	event := messagev1.Event{
		Version:         messagev1.Version,
		MessageID:       fmt.Sprintf("%s-%d", roomID, roomSeq),
		ClientMessageID: fmt.Sprintf("client-%s-%d", roomID, roomSeq),
		RoomID:          roomID,
		RoomSeq:         roomSeq,
		CreatedAt:       time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC),
		Payload:         json.RawMessage(`{"text":"hello"}`),
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return postgres.Entry{
		MessageID: event.MessageID,
		RoomID:    event.RoomID,
		RoomSeq:   event.RoomSeq,
		Event:     eventJSON,
	}
}

func ids(entries ...postgres.Entry) []string {
	result := make([]string, len(entries))
	for i, entry := range entries {
		result[i] = entry.MessageID
	}
	return result
}
