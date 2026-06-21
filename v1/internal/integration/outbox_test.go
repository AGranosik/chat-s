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

// HandleIncoming must commit the message row and the outbox row atomically.
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

	// The outbox payload round-trips to the persisted message.
	events, err := testStore.FetchUndispatched(ctx, 10)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(events) != 1 || events[0].Message.Body != "hello" || events[0].Message.UserID != seedUserID {
		t.Errorf("outbox payload = %+v, want body=hello user=seed", events)
	}
}

// Invalid frames must write neither a message nor an outbox row.
func TestOutbox_ValidationWritesNothing(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	svc := chat.NewService(testStore)

	cases := []chat.Incoming{
		{UserID: seedUserID, Body: "   "}, // empty after trim
		{UserID: "", Body: "hi"},          // missing user
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

// The relay, woken by pg_notify on commit, drains the new event to the
// broadcaster and stamps it dispatched — the live fast path.
func TestOutbox_RelayDispatchesViaNotify(t *testing.T) {
	rec := &recordingBroadcaster{}
	st := newStack(t, rec) // relay feeds the recorder; ctx cleaned up by t.Cleanup
	ctx := context.Background()

	if err := st.svc.HandleIncoming(ctx, seedRoomID, chat.Incoming{UserID: seedUserID, Body: "notify-me"}); err != nil {
		t.Fatalf("handle incoming: %v", err)
	}

	eventually(t, 5*time.Second, func() error {
		for _, m := range rec.snapshot() {
			if m.Body == "notify-me" {
				return nil
			}
		}
		return errors.New("broadcast not seen yet")
	})

	// And the row is marked dispatched after hand-off.
	eventually(t, 5*time.Second, func() error {
		if countRows(t, `select count(*) from outbox where dispatched_at is null`) == 0 {
			return nil
		}
		return errors.New("row still undispatched")
	})
}

// A row inserted directly (no pg_notify) must still be drained by the relay's
// periodic poll fallback.
func TestOutbox_RelayPollFallbackDrainsUnnotifiedRow(t *testing.T) {
	rec := &recordingBroadcaster{}
	_ = newStack(t, rec)
	ctx := context.Background()

	// Enqueue without firing pg_notify — only the poll can find this.
	msg := models.Message{RoomID: seedRoomID, Body: "poll-only"}
	payload, _ := json.Marshal(msg)
	err := testStore.WithTx(ctx, func(tx pgx.Tx) error {
		return storage.EnqueueOutbox(ctx, tx, seedRoomID, payload)
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Poll interval is ~2s; allow generous slack.
	eventually(t, 8*time.Second, func() error {
		for _, m := range rec.snapshot() {
			if m.Body == "poll-only" {
				return nil
			}
		}
		return errors.New("poll fallback has not drained the row yet")
	})
}
