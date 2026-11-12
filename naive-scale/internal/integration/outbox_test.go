//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"naive-scale/internal/chat"
	"naive-scale/internal/models"
	"naive-scale/internal/storage"
)

func TestOutbox_HandleIncomingPersistsBothRows(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	svc := chat.NewService(testStore)

	if err := svc.HandleIncoming(ctx, seedRoomID, chat.Incoming{UserID: seedUserID, Body: "hello"}); err != nil {
		t.Fatalf("handle incoming: %v", err)
	}

	if got := countRows(t, `select count(*) from messages where body = 'hello'`); got != 1 {
		t.Errorf("messages = %d, want 1", got)
	}
	if got := countRows(t, `select count(*) from outbox where dispatched_at is null`); got != 1 {
		t.Errorf("undispatched outbox = %d, want 1", got)
	}

	events, err := testStore.FetchUndispatched(ctx, 10)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(events) != 1 || events[0].Message.Body != "hello" || events[0].Message.UserID != seedUserID {
		t.Errorf("outbox payload = %+v, want body=hello user=seed", events)
	}
}

func TestOutbox_ValidationWritesNothing(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	svc := chat.NewService(testStore)

	cases := []chat.Incoming{
		{UserID: seedUserID, Body: "   "},
		{UserID: "", Body: "hi"},
	}
	for _, in := range cases {
		if err := svc.HandleIncoming(ctx, seedRoomID, in); !errors.Is(err, chat.ErrInvalid) {
			t.Errorf("HandleIncoming(%+v) err = %v, want ErrInvalid", in, err)
		}
	}

	if got := countRows(t, `select count(*) from messages`); got != 0 {
		t.Errorf("messages = %d, want 0", got)
	}
	if got := countRows(t, `select count(*) from outbox`); got != 0 {
		t.Errorf("outbox = %d, want 0", got)
	}
}

func TestOutbox_RelayDispatchesEnqueuedEvent(t *testing.T) {
	rec := &recordingBroadcaster{}
	st := newStack(t, rec)
	ctx := context.Background()

	if err := st.svc.HandleIncoming(ctx, seedRoomID, chat.Incoming{UserID: seedUserID, Body: "pick-me-up"}); err != nil {
		t.Fatalf("handle incoming: %v", err)
	}

	eventually(t, 8*time.Second, func() error {
		for _, m := range rec.snapshot() {
			if m.Body == "pick-me-up" {
				return nil
			}
		}
		return errors.New("broadcast not seen yet")
	})

	eventually(t, 5*time.Second, func() error {
		if countRows(t, `select count(*) from outbox where dispatched_at is null`) == 0 {
			return nil
		}
		return errors.New("row still undispatched")
	})
}

func TestOutbox_RelayPollDrainsDirectlyEnqueuedRow(t *testing.T) {
	rec := &recordingBroadcaster{}
	_ = newStack(t, rec)
	ctx := context.Background()

	msg := models.Message{RoomID: seedRoomID, Body: "poll-only"}
	payload, _ := json.Marshal(msg)
	err := testStore.WithTx(ctx, func(tx pgx.Tx) error {
		return storage.EnqueueOutbox(ctx, tx, seedRoomID, payload)
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	eventually(t, 8*time.Second, func() error {
		for _, m := range rec.snapshot() {
			if m.Body == "poll-only" {
				return nil
			}
		}
		return errors.New("poll fallback has not drained the row yet")
	})
}

func TestOutbox_ConcurrentRelaysDispatchEachRowOnce(t *testing.T) {
	freshDB(t)
	ctx := context.Background()

	const total = 300
	for i := 0; i < total; i++ {
		msg := models.Message{RoomID: seedRoomID, Body: fmt.Sprintf("m-%d", i)}
		payload, _ := json.Marshal(msg)
		if err := testStore.WithTx(ctx, func(tx pgx.Tx) error {
			return storage.EnqueueOutbox(ctx, tx, seedRoomID, payload)
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	seen := make(map[int64]int)

	const relays = 3
	var wg sync.WaitGroup
	for r := 0; r < relays; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := testStore.DispatchBatch(ctx, 16, func(e storage.OutboxEvent) error {
					mu.Lock()
					seen[e.ID]++
					mu.Unlock()
					return nil
				})
				if err != nil {
					t.Errorf("dispatch batch: %v", err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(seen) != total {
		t.Errorf("distinct rows dispatched = %d, want %d", len(seen), total)
	}
	for id, c := range seen {
		if c != 1 {
			t.Errorf("outbox id %d dispatched %d times, want exactly 1", id, c)
		}
	}
	if got := countRows(t, `select count(*) from outbox where dispatched_at is null`); got != 0 {
		t.Errorf("undispatched rows remain = %d, want 0", got)
	}
}
