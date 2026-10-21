package ws

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type writtenMessage struct {
	messageType int
	data        []byte
}

type mockConn struct {
	mu       sync.Mutex
	written  []writtenMessage
	calls    int
	closed   int
	writeErr error
}

func (m *mockConn) WriteMessage(messageType int, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls++
	if m.writeErr != nil {
		return m.writeErr
	}
	m.written = append(m.written, writtenMessage{
		messageType: messageType,
		data:        append([]byte(nil), data...),
	})
	return nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed++
	return nil
}

func (m *mockConn) writes() []writtenMessage {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]writtenMessage(nil), m.written...)
}

func (m *mockConn) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.calls
}

func (m *mockConn) closeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.closed
}

func startPump(t *testing.T, ctx context.Context, c *ClientConnection) <-chan struct{} {
	t.Helper()

	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.WritePump(ctx)
	}()
	return done
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WritePump did not return in time")
	}
}

func TestNewClientConnection(t *testing.T) {
	conn := &mockConn{}

	c := NewClientConnection("client1", conn)

	if c.ClientID != "client1" {
		t.Fatalf("got client id %q, want %q", c.ClientID, "client1")
	}
	if c.conn != conn {
		t.Fatal("expected the provided connection to be stored")
	}
	if c.Send == nil {
		t.Fatal("expected Send channel to be initialized")
	}
	if got := cap(c.Send); got != 256 {
		t.Fatalf("got Send capacity %d, want 256", got)
	}
	if got := len(c.Send); got != 0 {
		t.Fatalf("got %d queued messages, want 0", got)
	}
}

func TestClientConnectionWritePump(t *testing.T) {
	t.Run("writes queued messages in order as text messages", func(t *testing.T) {
		conn := &mockConn{}
		c := NewClientConnection("client1", conn)
		want := []string{"first", "second", "third"}

		done := startPump(t, context.Background(), c)
		for _, msg := range want {
			c.Send <- []byte(msg)
		}
		close(c.Send)
		waitDone(t, done)

		got := conn.writes()
		if len(got) != len(want) {
			t.Fatalf("got %d writes, want %d", len(got), len(want))
		}
		for i, w := range want {
			if got[i].messageType != websocket.TextMessage {
				t.Fatalf("write %d: got message type %d, want %d", i, got[i].messageType, websocket.TextMessage)
			}
			if string(got[i].data) != w {
				t.Fatalf("write %d: got %q, want %q", i, got[i].data, w)
			}
		}
	})

	t.Run("closing the send channel stops the pump and closes the connection", func(t *testing.T) {
		conn := &mockConn{}
		c := NewClientConnection("client1", conn)

		done := startPump(t, context.Background(), c)
		close(c.Send)
		waitDone(t, done)

		if got := conn.closeCount(); got != 1 {
			t.Fatalf("got %d close calls, want 1", got)
		}
		if got := conn.callCount(); got != 0 {
			t.Fatalf("got %d write calls, want 0", got)
		}
	})

	t.Run("context cancellation stops the pump and closes the connection", func(t *testing.T) {
		conn := &mockConn{}
		c := NewClientConnection("client1", conn)
		ctx, cancel := context.WithCancel(context.Background())

		done := startPump(t, ctx, c)
		cancel()
		waitDone(t, done)

		if got := conn.closeCount(); got != 1 {
			t.Fatalf("got %d close calls, want 1", got)
		}
		if got := conn.callCount(); got != 0 {
			t.Fatalf("got %d write calls, want 0", got)
		}
	})

	t.Run("write error stops the pump and closes the connection", func(t *testing.T) {
		conn := &mockConn{writeErr: errors.New("write failure")}
		c := NewClientConnection("client1", conn)
		c.Send <- []byte("first")
		c.Send <- []byte("second")

		done := startPump(t, context.Background(), c)
		waitDone(t, done)

		if got := conn.callCount(); got != 1 {
			t.Fatalf("got %d write calls, want 1", got)
		}
		if got := conn.closeCount(); got != 1 {
			t.Fatalf("got %d close calls, want 1", got)
		}
		if got := len(conn.writes()); got != 0 {
			t.Fatalf("got %d successful writes, want 0", got)
		}
	})
}
