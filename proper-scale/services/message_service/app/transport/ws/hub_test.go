package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"messages/chat"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func newConnWithSend(id string, buffer int) *ClientConnection {
	return &ClientConnection{
		ClientID: id,
		Send:     make(chan []byte, buffer),
	}
}

func roomClients(h *Hub, roomID string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	r, ok := h.rooms[roomID]
	if !ok {
		return nil
	}
	ids := r.clientIDs()
	sort.Strings(ids)
	return ids
}

func hasRoom(h *Hub, roomID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	_, ok := h.rooms[roomID]
	return ok
}

func receive(t *testing.T, c *ClientConnection) json.RawMessage {
	t.Helper()

	select {
	case p := <-c.Send:
		return p
	default:
		t.Fatalf("client %s: expected a payload, channel was empty", c.ClientID)
		return nil
	}
}

func TestNewHub(t *testing.T) {
	hub, err := NewHub()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hub == nil {
		t.Fatal("expected non-nil hub")
	}
	if hub.rooms == nil {
		t.Fatal("expected rooms map to be initialized")
	}
	if len(hub.rooms) != 0 {
		t.Fatalf("expected empty rooms map, got %d entries", len(hub.rooms))
	}
}

func TestHubRegister(t *testing.T) {
	t.Run("nil connection returns error", func(t *testing.T) {
		hub, _ := NewHub()

		if err := hub.Register([]string{"room1"}, nil); err == nil {
			t.Fatal("expected error, got nil")
		}
		if hasRoom(hub, "room1") {
			t.Fatal("expected no room to be created for a nil connection")
		}
	})

	t.Run("registers client into a single room", func(t *testing.T) {
		hub, _ := NewHub()
		conn := &ClientConnection{ClientID: "client1"}

		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := roomClients(hub, "room1")
		want := []string{"client1"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("registers client into multiple rooms", func(t *testing.T) {
		hub, _ := NewHub()
		conn := &ClientConnection{ClientID: "client1"}

		if err := hub.Register([]string{"room1", "room2"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		for _, roomID := range []string{"room1", "room2"} {
			got := roomClients(hub, roomID)
			want := []string{"client1"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("room %s: got %v, want %v", roomID, got, want)
			}
		}
	})

	t.Run("registers multiple clients into the same room", func(t *testing.T) {
		hub, _ := NewHub()
		conn1 := &ClientConnection{ClientID: "client1"}
		conn2 := &ClientConnection{ClientID: "client2"}

		if err := hub.Register([]string{"room1"}, conn1); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room1"}, conn2); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := roomClients(hub, "room1")
		want := []string{"client1", "client2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("re-registering the same client does not duplicate it", func(t *testing.T) {
		hub, _ := NewHub()
		conn := &ClientConnection{ClientID: "client1"}

		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := roomClients(hub, "room1")
		want := []string{"client1"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("empty room list registers no rooms", func(t *testing.T) {
		hub, _ := NewHub()
		conn := &ClientConnection{ClientID: "client1"}

		if err := hub.Register([]string{}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(hub.rooms) != 0 {
			t.Fatalf("expected no rooms, got %d", len(hub.rooms))
		}
	})
}

func TestHubUnregister(t *testing.T) {
	t.Run("removes client from all rooms", func(t *testing.T) {
		hub, _ := NewHub()
		conn := &ClientConnection{ClientID: "client1"}
		if err := hub.Register([]string{"room1", "room2"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := hub.Unregister("client1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := roomClients(hub, "room1"); got != nil {
			t.Fatalf("room1: got %v, want nil", got)
		}
		if got := roomClients(hub, "room2"); got != nil {
			t.Fatalf("room2: got %v, want nil", got)
		}
	})

	t.Run("deletes a room once it becomes empty", func(t *testing.T) {
		hub, _ := NewHub()
		conn := &ClientConnection{ClientID: "client1"}
		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := hub.Unregister("client1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if hasRoom(hub, "room1") {
			t.Fatal("expected room1 to be removed from the hub")
		}
	})

	t.Run("keeps a room that still has other clients", func(t *testing.T) {
		hub, _ := NewHub()
		conn1 := &ClientConnection{ClientID: "client1"}
		conn2 := &ClientConnection{ClientID: "client2"}
		if err := hub.Register([]string{"room1"}, conn1); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room1"}, conn2); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := hub.Unregister("client1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := roomClients(hub, "room1")
		want := []string{"client2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("unregistering an unknown client is a no-op", func(t *testing.T) {
		hub, _ := NewHub()
		conn := &ClientConnection{ClientID: "client1"}
		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := hub.Unregister("unknown"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := roomClients(hub, "room1")
		want := []string{"client1"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("unregistering from an empty hub is a no-op", func(t *testing.T) {
		hub, _ := NewHub()

		if err := hub.Unregister("client1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(hub.rooms) != 0 {
			t.Fatalf("expected no rooms, got %d", len(hub.rooms))
		}
	})
}

func TestHubAllClients(t *testing.T) {
	t.Run("empty hub returns no clients", func(t *testing.T) {
		hub, _ := NewHub()

		if got := hub.allClients(); len(got) != 0 {
			t.Fatalf("got %v, want empty", got)
		}
	})

	t.Run("returns clients from every room", func(t *testing.T) {
		hub, _ := NewHub()
		conn1 := &ClientConnection{ClientID: "client1"}
		conn2 := &ClientConnection{ClientID: "client2"}
		if err := hub.Register([]string{"room1"}, conn1); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room2"}, conn2); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := hub.allClients()
		sort.Strings(got)
		want := []string{"client1", "client2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func TestHubDispatch(t *testing.T) {
	ctx := context.Background()

	t.Run("unknown room returns error", func(t *testing.T) {
		hub, _ := NewHub()

		err := hub.Dispatch(ctx, chat.Message{RoomID: "nonexistent"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("delivers payload to every client in the room", func(t *testing.T) {
		hub, _ := NewHub()
		conn1 := newConnWithSend("client1", 1)
		conn2 := newConnWithSend("client2", 1)
		if err := hub.Register([]string{"room1"}, conn1); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room1"}, conn2); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		payload := json.RawMessage(`{"text":"hi"}`)

		if err := hub.Dispatch(ctx, chat.Message{RoomID: "room1", Payload: payload}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		for _, conn := range []*ClientConnection{conn1, conn2} {
			if got := receive(t, conn); string(got) != string(payload) {
				t.Fatalf("client %s: got payload %s, want %s", conn.ClientID, got, payload)
			}
		}
	})

	t.Run("delivers only to clients of the target room", func(t *testing.T) {
		hub, _ := NewHub()
		inRoom1 := newConnWithSend("client1", 1)
		inRoom2 := newConnWithSend("client2", 1)
		inBoth := newConnWithSend("client3", 2)
		if err := hub.Register([]string{"room1"}, inRoom1); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room2"}, inRoom2); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room1", "room2"}, inBoth); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		msg := chat.Message{RoomID: "room1", Payload: json.RawMessage(`{"text":"hi"}`)}
		if err := hub.Dispatch(ctx, msg); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := len(inRoom1.Send); got != 1 {
			t.Fatalf("client1: got %d payloads, want 1", got)
		}
		if got := len(inRoom2.Send); got != 0 {
			t.Fatalf("client2: got %d payloads, want 0", got)
		}
		if got := len(inBoth.Send); got != 1 {
			t.Fatalf("client3: got %d payloads, want 1", got)
		}
	})

	t.Run("room with no remaining clients returns error", func(t *testing.T) {
		hub, _ := NewHub()
		conn := newConnWithSend("client1", 1)
		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Unregister("client1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := hub.Dispatch(ctx, chat.Message{RoomID: "room1"}); err == nil {
			t.Fatal("expected error, got nil")
		}
		if got := len(conn.Send); got != 0 {
			t.Fatalf("got %d payloads, want 0", got)
		}
	})
}

func TestRoomAddClient(t *testing.T) {
	room := newRoom("room1")
	conn := &ClientConnection{ClientID: "client1"}

	room.addClient(conn)

	got := room.clientIDs()
	want := []string{"client1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestRoomAddClientOverwritesExisting(t *testing.T) {
	room := newRoom("room1")
	first := &ClientConnection{ClientID: "client1"}
	second := &ClientConnection{ClientID: "client1"}

	room.addClient(first)
	room.addClient(second)

	got := room.clientIDs()
	want := []string{"client1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if room.clients["client1"] != second {
		t.Fatal("expected the second connection to replace the first")
	}
}

func TestRoomRemoveClient(t *testing.T) {
	room := newRoom("room1")
	conn := &ClientConnection{ClientID: "client1"}
	room.addClient(conn)

	room.removeClient("client1")

	if !room.isEmpty() {
		t.Fatal("expected room to be empty")
	}
}

func TestRoomRemoveUnknownClient(t *testing.T) {
	room := newRoom("room1")
	conn := &ClientConnection{ClientID: "client1"}
	room.addClient(conn)

	room.removeClient("unknown")

	got := room.clientIDs()
	want := []string{"client1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestRoomIsEmpty(t *testing.T) {
	room := newRoom("room1")
	if !room.isEmpty() {
		t.Fatal("expected a new room to be empty")
	}

	conn := &ClientConnection{ClientID: "client1"}
	room.addClient(conn)
	if room.isEmpty() {
		t.Fatal("expected room to be non-empty after adding a client")
	}

	room.removeClient("client1")
	if !room.isEmpty() {
		t.Fatal("expected room to be empty after removing its only client")
	}
}

func TestRoomClientIDs(t *testing.T) {
	room := newRoom("room1")

	if got := room.clientIDs(); len(got) != 0 {
		t.Fatalf("got %v, want empty", got)
	}

	conn1 := &ClientConnection{ClientID: "client1"}
	conn2 := &ClientConnection{ClientID: "client2"}
	room.addClient(conn1)
	room.addClient(conn2)

	got := room.clientIDs()
	sort.Strings(got)
	want := []string{"client1", "client2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestHubConcurrentRegisterAndUnregister(t *testing.T) {
	hub, _ := NewHub()
	const clientCount = 50

	var wg sync.WaitGroup
	for i := range clientCount {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn := &ClientConnection{ClientID: fmt.Sprintf("client%d", i)}
			if err := hub.Register([]string{"room1"}, conn); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if got := len(roomClients(hub, "room1")); got != clientCount {
		t.Fatalf("got %d clients, want %d", got, clientCount)
	}

	for i := range clientCount {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := hub.Unregister(fmt.Sprintf("client%d", i)); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if hasRoom(hub, "room1") {
		t.Fatal("expected room1 to be removed once all clients unregistered")
	}
}
