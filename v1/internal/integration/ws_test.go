//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"

	"chat-s/internal/models"
	"chat-s/internal/storage"
)

// The full vertical slice: client A's frame travels send → service (tx) →
// relay drain → hub → client B's socket.
func TestWS_RoundTripBetweenTwoClients(t *testing.T) {
	st := newStack(t, nil) // nil → relay feeds the real hub (production path)

	a := st.dialWS(t, seedRoomID)
	b := st.dialWS(t, seedRoomID)

	send(t, a, seedUserID, "hi from A")

	// Both clients are in the room, so both receive the broadcast.
	clients := map[string]*websocket.Conn{"A": a, "B": b}
	for name, conn := range clients {
		msg := readMessage(t, conn, 5*time.Second)
		if msg.Body != "hi from A" {
			t.Errorf("client %s got %q, want 'hi from A'", name, msg.Body)
		}
		if msg.ID == 0 || msg.RoomID != seedRoomID || msg.UserID != seedUserID {
			t.Errorf("client %s message = %+v, want id/room/user populated", name, msg)
		}
	}
}

// After a round-trip the message is durable and served by the history endpoint.
func TestWS_MessageAppearsInHistory(t *testing.T) {
	st := newStack(t, nil)

	a := st.dialWS(t, seedRoomID)
	send(t, a, seedUserID, "persist me")
	_ = readMessage(t, a, 5*time.Second) // wait until fan-out (so it's committed)

	var msgs []models.Message
	st.getJSON(t, "/api/rooms/"+seedRoomID+"/messages", &msgs)
	if len(msgs) != 1 || msgs[0].Body != "persist me" {
		t.Fatalf("history = %+v, want one 'persist me' message", msgs)
	}
}

// At-least-once recovery: an undispatched outbox row — e.g. one left behind by a
// crash after commit but before fan-out — is rediscovered by the relay's scan
// and delivered to connected clients (the Phase 5 guarantee). We connect and
// confirm the client first (a warmup round-trip), so the assertion is about the
// relay finding an unnotified row, not about connect/register timing.
func TestWS_UndispatchedRowIsRediscoveredAndDelivered(t *testing.T) {
	st := newStack(t, nil) // clean DB, relay + real hub running
	ctx := context.Background()

	conn := st.dialWS(t, seedRoomID)

	// Warmup: prove the client is registered and the full pipeline works.
	send(t, conn, seedUserID, "warmup")
	if got := readMessage(t, conn, 5*time.Second); got.Body != "warmup" {
		t.Fatalf("warmup = %q, want 'warmup'", got.Body)
	}

	// Now strand a message + outbox row WITHOUT firing pg_notify — the relay can
	// only find it via its periodic scan, exactly as it would on crash recovery.
	msg := models.Message{RoomID: seedRoomID, UserID: seedUserID, Body: "stranded"}
	err := testStore.WithTx(ctx, func(tx pgx.Tx) error {
		if err := storage.InsertMessage(ctx, tx, &msg); err != nil {
			return err
		}
		payload, _ := json.Marshal(msg)
		return storage.EnqueueOutbox(ctx, tx, seedRoomID, payload)
	})
	if err != nil {
		t.Fatalf("seed undispatched: %v", err)
	}

	got := readMessage(t, conn, 8*time.Second) // poll interval ~2s + slack
	if got.Body != "stranded" {
		t.Fatalf("rediscovered message = %q, want 'stranded'", got.Body)
	}

	eventually(t, 5*time.Second, func() error {
		if countRows(t, `select count(*) from outbox where dispatched_at is null`) == 0 {
			return nil
		}
		return errors.New("stranded row still undispatched")
	})
}
