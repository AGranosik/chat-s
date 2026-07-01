package hub

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"chat-s/internal/models"
)

// newTestClient builds a client without a websocket connection. The hub's Run
// loop never touches conn — only the read/write pumps do — so a nil conn is
// safe for fan-out tests.
func newTestClient(roomID string) *Client {
	return &Client{roomID: roomID, send: make(chan []byte, sendBuffer)}
}

// startHub runs a hub and returns it with a cleanup that stops the Run goroutine.
func startHub(t *testing.T) *Hub {
	t.Helper()
	h := New()
	ctx, cancel := context.WithCancel(context.Background())
	go h.Run(ctx)
	t.Cleanup(cancel)
	return h
}

// recvWithin reads one payload from the channel or fails after timeout.
func recvWithin(t *testing.T, ch <-chan []byte, d time.Duration) []byte {
	t.Helper()
	select {
	case b := <-ch:
		return b
	case <-time.After(d):
		t.Fatal("timed out waiting for broadcast")
		return nil
	}
}

func TestBroadcast_DeliversToRoomMember(t *testing.T) {
	h := startHub(t)
	c := newTestClient("room1")
	h.Register(c)

	msg := models.Message{ID: 1, RoomID: "room1", UserID: "u1", Body: "hello"}
	h.Broadcast("room1", msg)

	var got models.Message
	if err := json.Unmarshal(recvWithin(t, c.send, time.Second), &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got != msg {
		t.Errorf("delivered %+v, want %+v", got, msg)
	}
}

func TestBroadcast_IsolatedByRoom(t *testing.T) {
	h := startHub(t)
	in := newTestClient("room1")
	out := newTestClient("room2")
	h.Register(in)
	h.Register(out)

	h.Broadcast("room1", models.Message{ID: 1, RoomID: "room1", Body: "hi"})

	// room1 member gets it.
	recvWithin(t, in.send, time.Second)

	// room2 member must not.
	select {
	case b := <-out.send:
		t.Fatalf("room2 client received cross-room message: %s", b)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestBroadcast_FanOutToMultipleMembers(t *testing.T) {
	h := startHub(t)
	a := newTestClient("room1")
	b := newTestClient("room1")
	h.Register(a)
	h.Register(b)

	h.Broadcast("room1", models.Message{ID: 7, RoomID: "room1", Body: "yo"})

	recvWithin(t, a.send, time.Second)
	recvWithin(t, b.send, time.Second)
}

func TestUnregister_ClosesSendAndStopsDelivery(t *testing.T) {
	h := startHub(t)
	c := newTestClient("room1")
	h.Register(c)
	h.Unregister(c)

	// Unregister closes the send channel; a receive returns the zero value with
	// ok=false once the close is observed.
	if _, ok := <-c.send; ok {
		t.Error("expected send channel to be closed after unregister")
	}
}

func TestBroadcast_UnknownRoomIsNoOp(t *testing.T) {
	h := startHub(t)
	// Must not panic or block when no clients exist for the room.
	h.Broadcast("ghost-room", models.Message{ID: 1, RoomID: "ghost-room", Body: "x"})
}
