package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"messages/chat"
	"reflect"
	"sort"
	"sync"
	"testing"
)

type mockMessageHandler struct {
	mu       sync.Mutex
	messages []chat.Message
	err      error
}

func (m *mockMessageHandler) HandleIncoming(ctx context.Context, msg chat.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
	return m.err
}

func (m *mockMessageHandler) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}

func TestNewHub(t *testing.T) {
	t.Run("nil service returns error", func(t *testing.T) {
		hub, err := NewHub(nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if hub != nil {
			t.Fatalf("expected nil hub, got %#v", hub)
		}
	})

	t.Run("valid service returns initialized hub", func(t *testing.T) {
		hub, err := NewHub(&mockMessageHandler{})
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
	})
}

func TestHubRegister(t *testing.T) {
	t.Run("nil connection returns error", func(t *testing.T) {
		hub, err := NewHub(&mockMessageHandler{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room1"}, nil); err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("registers client into a single room", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
		conn := &ClientConnection{ClientID: "client1"}

		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := hub.Clients("room1")
		want := []string{"client1"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("registers client into multiple rooms", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
		conn := &ClientConnection{ClientID: "client1"}

		if err := hub.Register([]string{"room1", "room2"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		for _, roomID := range []string{"room1", "room2"} {
			got := hub.Clients(roomID)
			want := []string{"client1"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("room %s: got %v, want %v", roomID, got, want)
			}
		}
	})

	t.Run("registers multiple clients into the same room", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
		conn1 := &ClientConnection{ClientID: "client1"}
		conn2 := &ClientConnection{ClientID: "client2"}

		if err := hub.Register([]string{"room1"}, conn1); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room1"}, conn2); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := hub.Clients("room1")
		sort.Strings(got)
		want := []string{"client1", "client2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("re-registering the same client does not duplicate it", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
		conn := &ClientConnection{ClientID: "client1"}

		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := hub.Clients("room1")
		want := []string{"client1"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("empty room list registers no rooms", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
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
		hub, _ := NewHub(&mockMessageHandler{})
		conn := &ClientConnection{ClientID: "client1"}
		if err := hub.Register([]string{"room1", "room2"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := hub.Unregister("client1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := hub.Clients("room1"); got != nil {
			t.Fatalf("room1: got %v, want nil", got)
		}
		if got := hub.Clients("room2"); got != nil {
			t.Fatalf("room2: got %v, want nil", got)
		}
	})

	t.Run("deletes a room once it becomes empty", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
		conn := &ClientConnection{ClientID: "client1"}
		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := hub.Unregister("client1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		hub.mu.RLock()
		_, ok := hub.rooms["room1"]
		hub.mu.RUnlock()
		if ok {
			t.Fatal("expected room1 to be removed from the hub")
		}
	})

	t.Run("keeps a room that still has other clients", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
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

		got := hub.Clients("room1")
		want := []string{"client2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("unregistering an unknown client is a no-op", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
		conn := &ClientConnection{ClientID: "client1"}
		if err := hub.Register([]string{"room1"}, conn); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := hub.Unregister("unknown"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := hub.Clients("room1")
		want := []string{"client1"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("unregistering from an empty hub is a no-op", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})

		if err := hub.Unregister("client1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(hub.rooms) != 0 {
			t.Fatalf("expected no rooms, got %d", len(hub.rooms))
		}
	})
}

func TestHubClients(t *testing.T) {
	t.Run("unknown room returns nil", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
		if got := hub.Clients("nonexistent"); got != nil {
			t.Fatalf("got %v, want nil", got)
		}
	})

	t.Run("known room with no clients returns empty slice", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
		hub.mu.Lock()
		hub.rooms["room1"] = newRoom("room1")
		hub.mu.Unlock()

		got := hub.Clients("room1")
		if len(got) != 0 {
			t.Fatalf("got %v, want empty", got)
		}
	})

	t.Run("returns every client id in the room", func(t *testing.T) {
		hub, _ := NewHub(&mockMessageHandler{})
		conn1 := &ClientConnection{ClientID: "client1"}
		conn2 := &ClientConnection{ClientID: "client2"}
		if err := hub.Register([]string{"room1"}, conn1); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := hub.Register([]string{"room1"}, conn2); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := hub.Clients("room1")
		sort.Strings(got)
		want := []string{"client1", "client2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func TestHubHandleIncoming(t *testing.T) {
	t.Run("delegates to the underlying handler", func(t *testing.T) {
		handler := &mockMessageHandler{}
		hub, _ := NewHub(handler)
		msg := chat.Message{RoomID: "room1", Payload: json.RawMessage(`{"text":"hi"}`)}

		if err := hub.HandleIncoming(msg, context.Background()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got := handler.callCount(); got != 1 {
			t.Fatalf("got %d calls, want 1", got)
		}
		if handler.messages[0].RoomID != "room1" {
			t.Fatalf("got room id %q, want %q", handler.messages[0].RoomID, "room1")
		}
		if !reflect.DeepEqual(handler.messages[0].Payload, msg.Payload) {
			t.Fatalf("got payload %s, want %s", handler.messages[0].Payload, msg.Payload)
		}
	})

	t.Run("propagates the handler error", func(t *testing.T) {
		wantErr := errors.New("handler failure")
		handler := &mockMessageHandler{err: wantErr}
		hub, _ := NewHub(handler)

		err := hub.HandleIncoming(chat.Message{}, context.Background())
		if !errors.Is(err, wantErr) {
			t.Fatalf("got %v, want %v", err, wantErr)
		}
	})
}

func TestHubSendMessage(t *testing.T) {
	hub, _ := NewHub(&mockMessageHandler{})
	if err := hub.SendMessage(chat.Message{}, context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
	hub, _ := NewHub(&mockMessageHandler{})
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

	if got := len(hub.Clients("room1")); got != clientCount {
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

	if got := hub.Clients("room1"); got != nil {
		t.Fatalf("got %v, want nil", got)
	}
}
