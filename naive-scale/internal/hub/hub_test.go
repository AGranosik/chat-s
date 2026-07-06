package hub

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"naive-scale/internal/models"
)

// testRedisAddr is the host:port of a Redis container shared by the whole
// package, started once in TestMain. These tests require Docker: if the
// container can't start, the suite fails rather than skips.
var testRedisAddr string

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run owns the Redis container lifecycle so deferred cleanup runs before
// os.Exit. These tests require Docker: a container that won't start (no daemon,
// image pull fails) fails the suite rather than skipping it.
func run(m *testing.M) int {
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		log.Printf("hub: cannot start redis container | err=%v", err)
		return 1
	}
	defer func() { _ = container.Terminate(ctx) }()

	addr, err := container.Endpoint(ctx, "")
	if err != nil {
		log.Printf("hub: redis endpoint: %v", err)
		return 1
	}
	testRedisAddr = addr

	return m.Run()
}

// newTestClient builds a client without a websocket connection. The hub's Run
// loop never touches conn — only the read/write pumps do — so a nil conn is
// safe for fan-out tests.
func newTestClient(roomID string) *Client {
	return &Client{roomID: roomID, send: make(chan []byte, sendBuffer)}
}

// startHub runs a hub wired to the package Redis container and returns it with a
// cleanup that stops the Run goroutine.
func startHub(t *testing.T) *Hub {
	t.Helper()
	h := New(testRedisAddr)
	ctx, cancel := context.WithCancel(context.Background())
	go h.Run(ctx)
	t.Cleanup(cancel)
	return h
}

// roomFor returns a room id unique to this test. Tests share one Redis, and a
// hub's subscriptions only drop once its Run goroutine finishes tearing down
// (after t.Cleanup cancels it), so a fixed room name could let one test observe
// another's lingering subscription. Unique names remove the cross-test coupling.
func roomFor(t *testing.T, suffix string) string {
	return t.Name() + "/" + suffix
}

// waitSubscribed blocks until Redis reports an active subscriber for the room's
// channel. Register only queues the work; Run performs the SUBSCRIBE
// asynchronously on the pub/sub connection, so without this a Broadcast can
// publish before the subscription lands and the message is silently lost.
func waitSubscribed(t *testing.T, h *Hub, roomID string) {
	t.Helper()
	ch := roomChannel(roomID)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		res, err := h.broker.PubSubNumSub(context.Background(), ch).Result()
		if err != nil {
			t.Fatalf("pubsub numsub: %v", err)
		}
		if res[ch] > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("subscription for %s not active within timeout", ch)
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
	room := roomFor(t, "1")
	c := newTestClient(room)
	h.Register(c)
	waitSubscribed(t, h, room)

	msg := models.Message{ID: 1, RoomID: room, UserID: "u1", Body: "hello"}
	h.Broadcast(room, msg)

	var got models.Message
	if err := json.Unmarshal(recvWithin(t, c.send, 2*time.Second), &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got != msg {
		t.Errorf("delivered %+v, want %+v", got, msg)
	}
}

func TestBroadcast_IsolatedByRoom(t *testing.T) {
	h := startHub(t)
	roomA := roomFor(t, "1")
	roomB := roomFor(t, "2")
	in := newTestClient(roomA)
	out := newTestClient(roomB)
	h.Register(in)
	h.Register(out)
	waitSubscribed(t, h, roomA)
	waitSubscribed(t, h, roomB)

	h.Broadcast(roomA, models.Message{ID: 1, RoomID: roomA, Body: "hi"})

	// roomA member gets it.
	recvWithin(t, in.send, 2*time.Second)

	// roomB member must not.
	select {
	case b := <-out.send:
		t.Fatalf("cross-room delivery: roomB client received %s", b)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestBroadcast_FanOutToMultipleMembers(t *testing.T) {
	h := startHub(t)
	room := roomFor(t, "1")
	a := newTestClient(room)
	b := newTestClient(room)
	h.Register(a)
	h.Register(b)
	waitSubscribed(t, h, room)

	h.Broadcast(room, models.Message{ID: 7, RoomID: room, Body: "yo"})

	recvWithin(t, a.send, 2*time.Second)
	recvWithin(t, b.send, 2*time.Second)
}

func TestUnregister_ClosesSendAndStopsDelivery(t *testing.T) {
	h := startHub(t)
	c := newTestClient(roomFor(t, "1"))
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
	// Publishing to a room with no subscribers must not panic or block.
	room := roomFor(t, "ghost")
	h.Broadcast(room, models.Message{ID: 1, RoomID: room, Body: "x"})
}
