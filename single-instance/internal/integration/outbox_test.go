//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"chat-s/internal/chat"
	"chat-s/internal/models"
	"chat-s/internal/storage"
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
