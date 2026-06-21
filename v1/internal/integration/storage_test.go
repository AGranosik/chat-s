//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"

	"chat-s/internal/models"
	"chat-s/internal/storage"
)

// insertMessage is a small helper that runs InsertMessage in its own tx.
func insertMessage(t *testing.T, body string) models.Message {
	t.Helper()
	m := models.Message{RoomID: seedRoomID, UserID: seedUserID, Body: body}
	err := testStore.WithTx(context.Background(), func(tx pgx.Tx) error {
		return storage.InsertMessage(context.Background(), tx, &m)
	})
	if err != nil {
		t.Fatalf("insert message: %v", err)
	}
	return m
}

func TestStorage_InsertAndHistoryPagination(t *testing.T) {
	freshDB(t)
	ctx := context.Background()

	// Insert 5 messages; ids are monotonic (bigserial).
	var ids []int64
	for _, b := range []string{"m1", "m2", "m3", "m4", "m5"} {
		ids = append(ids, insertMessage(t, b).ID)
	}

	// Newest-first, limit honored.
	page1, err := testStore.History(ctx, seedRoomID, 0, 2)
	if err != nil {
		t.Fatalf("history page1: %v", err)
	}
	if len(page1) != 2 || page1[0].Body != "m5" || page1[1].Body != "m4" {
		t.Fatalf("page1 = %+v, want m5,m4 newest-first", page1)
	}

	// Keyset: next page older than the last id seen.
	page2, err := testStore.History(ctx, seedRoomID, page1[1].ID, 2)
	if err != nil {
		t.Fatalf("history page2: %v", err)
	}
	if len(page2) != 2 || page2[0].Body != "m3" || page2[1].Body != "m2" {
		t.Fatalf("page2 = %+v, want m3,m2", page2)
	}

	// Scanned fields are populated.
	if page1[0].ID != ids[4] || page1[0].RoomID != seedRoomID || page1[0].UserID != seedUserID {
		t.Errorf("scanned message = %+v, want id=%d room/user seeded", page1[0], ids[4])
	}
	if page1[0].CreatedAt.IsZero() {
		t.Errorf("created_at not populated")
	}
}

func TestStorage_HistoryEmptyRoom(t *testing.T) {
	freshDB(t)
	msgs, err := testStore.History(context.Background(), seedRoomID, 0, 50)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("history = %+v, want empty", msgs)
	}
}

func TestStorage_InsertRejectsUnknownRoomFK(t *testing.T) {
	freshDB(t)
	missingRoom := "00000000-0000-0000-0000-0000000000ff"
	m := models.Message{RoomID: missingRoom, UserID: seedUserID, Body: "x"}
	err := testStore.WithTx(context.Background(), func(tx pgx.Tx) error {
		return storage.InsertMessage(context.Background(), tx, &m)
	})
	if err == nil {
		t.Fatal("insert with unknown room_id succeeded, want FK violation")
	}
}

func TestStorage_RoomsAndUsersCRUD(t *testing.T) {
	freshDB(t)
	ctx := context.Background()

	room, err := testStore.CreateRoom(ctx, "random")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	if room.ID == "" || room.Name != "random" {
		t.Errorf("created room = %+v", room)
	}

	user, err := testStore.CreateUser(ctx, "alice")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if user.ID == "" || user.Username != "alice" {
		t.Errorf("created user = %+v", user)
	}

	rooms, err := testStore.ListRooms(ctx)
	if err != nil {
		t.Fatalf("list rooms: %v", err)
	}
	if !containsRoom(rooms, "random") || !containsRoom(rooms, "general") {
		t.Errorf("rooms = %+v, want general + random", rooms)
	}

	users, err := testStore.ListUsers(ctx)
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	if !containsUser(users, "alice") || !containsUser(users, "demo") {
		t.Errorf("users = %+v, want demo + alice", users)
	}

	// Unique constraints surface as errors.
	if _, err := testStore.CreateRoom(ctx, "random"); err == nil {
		t.Error("duplicate room name succeeded, want unique violation")
	}
	if _, err := testStore.CreateUser(ctx, "alice"); err == nil {
		t.Error("duplicate username succeeded, want unique violation")
	}
}

func TestStorage_OutboxEnqueueFetchMark(t *testing.T) {
	freshDB(t)
	ctx := context.Background()

	// Enqueue three events directly, each carrying a decodable message payload.
	for i := 1; i <= 3; i++ {
		msg := models.Message{ID: int64(i), RoomID: seedRoomID, Body: "e" + string(rune('0'+i))}
		payload, _ := json.Marshal(msg)
		err := testStore.WithTx(ctx, func(tx pgx.Tx) error {
			return storage.EnqueueOutbox(ctx, tx, seedRoomID, payload)
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	events, err := testStore.FetchUndispatched(ctx, 10)
	if err != nil {
		t.Fatalf("fetch undispatched: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("undispatched = %d, want 3", len(events))
	}
	// Ordered by id (outbox row id), payload decoded.
	if events[0].ID >= events[1].ID || events[1].ID >= events[2].ID {
		t.Errorf("events not id-ordered: %+v", events)
	}
	if events[0].RoomID != seedRoomID || events[0].Message.Body != "e1" {
		t.Errorf("event[0] = %+v, want room seeded, body e1", events[0])
	}

	// Mark the first two dispatched; only the third remains.
	if err := testStore.MarkDispatched(ctx, []int64{events[0].ID, events[1].ID}); err != nil {
		t.Fatalf("mark dispatched: %v", err)
	}
	remaining, err := testStore.FetchUndispatched(ctx, 10)
	if err != nil {
		t.Fatalf("fetch after mark: %v", err)
	}
	if len(remaining) != 1 || remaining[0].ID != events[2].ID {
		t.Fatalf("remaining = %+v, want only event[2]", remaining)
	}

	if got := countRows(t, `select count(*) from outbox where dispatched_at is not null`); got != 2 {
		t.Errorf("dispatched rows = %d, want 2", got)
	}
}

func containsRoom(rooms []models.Room, name string) bool {
	for _, r := range rooms {
		if r.Name == name {
			return true
		}
	}
	return false
}

func containsUser(users []models.User, name string) bool {
	for _, u := range users {
		if u.Username == name {
			return true
		}
	}
	return false
}
