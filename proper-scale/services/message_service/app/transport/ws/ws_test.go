package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"messages/chat"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
)

const (
	testDial    = "some-dial"
	waitTimeout = 2 * time.Second
)

type fakeMessageHandler struct {
	mu   sync.Mutex
	msgs []chat.Message
	err  error
}

func (h *fakeMessageHandler) HandleIncoming(_ context.Context, m chat.Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, m)
	return h.err
}

func (h *fakeMessageHandler) received() []chat.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]chat.Message(nil), h.msgs...)
}

type fakeUserServiceClient struct {
	connectFn     func(context.Context, *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error)
	disconnectFn  func(context.Context, *contractsv1.DisconnectUserRequest) (*contractsv1.ConnectionResponse, error)
	isConnectedFn func(context.Context, *contractsv1.ClientsConnectionDestRequest) (*contractsv1.UserConnectionResponse, error)
	pingFn        func(context.Context, *contractsv1.PingConnectionRequest) (*contractsv1.ConnectionResponse, error)
}

func (f *fakeUserServiceClient) Connect(ctx context.Context, in *contractsv1.ConnectUserRequest, _ ...grpc.CallOption) (*contractsv1.ConnectionResponse, error) {
	if f.connectFn != nil {
		return f.connectFn(ctx, in)
	}
	return &contractsv1.ConnectionResponse{Success: true}, nil
}

func (f *fakeUserServiceClient) Disconnect(ctx context.Context, in *contractsv1.DisconnectUserRequest, _ ...grpc.CallOption) (*contractsv1.ConnectionResponse, error) {
	if f.disconnectFn != nil {
		return f.disconnectFn(ctx, in)
	}
	return &contractsv1.ConnectionResponse{Success: true}, nil
}

func (f *fakeUserServiceClient) IsConnected(ctx context.Context, in *contractsv1.ClientsConnectionDestRequest, _ ...grpc.CallOption) (*contractsv1.UserConnectionResponse, error) {
	if f.isConnectedFn != nil {
		return f.isConnectedFn(ctx, in)
	}
	return &contractsv1.UserConnectionResponse{}, nil
}

func (f *fakeUserServiceClient) Ping(ctx context.Context, in *contractsv1.PingConnectionRequest, _ ...grpc.CallOption) (*contractsv1.ConnectionResponse, error) {
	if f.pingFn != nil {
		return f.pingFn(ctx, in)
	}
	return &contractsv1.ConnectionResponse{Success: true}, nil
}

func TestNewWs(t *testing.T) {
	handler := &fakeMessageHandler{}

	t.Run("nil hub returns error", func(t *testing.T) {
		if _, err := NewWs(&fakeUserServiceClient{}, nil, testDial, handler); err == nil {
			t.Error("should return error on creation when hub is nil")
		}
	})

	t.Run("dial cannot be empty", func(t *testing.T) {
		if _, err := NewWs(&fakeUserServiceClient{}, &Hub{}, "", handler); err == nil {
			t.Error("should return error on creation when dial is empty")
		}
	})

	t.Run("creation success", func(t *testing.T) {
		if _, err := NewWs(&fakeUserServiceClient{}, &Hub{}, testDial, handler); err != nil {
			t.Errorf("should create successfully, got: %v", err)
		}
	})
}

func TestWsQuery(t *testing.T) {
	w := newTestWs(t)

	t.Run("no client id - bad request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ws", nil)
		rec := httptest.NewRecorder()

		w.ServeWS(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("got status %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("no rooms - bad request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ws?client_id=1", nil)
		rec := httptest.NewRecorder()

		w.ServeWS(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("got status %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("single room - success", func(t *testing.T) {
		hub := newHub(t)
		url := startServer(t, hub, &fakeUserServiceClient{}, &fakeMessageHandler{})

		dial(t, url+"?client_id=42&room_ids=a")

		eventually(t, func() bool { return roomCount(hub) == 1 }, "room should be created")
	})
}

func TestHub(t *testing.T) {
	t.Run("rooms created", func(t *testing.T) {
		hub := newHub(t)
		url := startServer(t, hub, &fakeUserServiceClient{}, &fakeMessageHandler{})

		dial(t, url+"?client_id=42&room_ids=a,b")

		eventually(t, func() bool { return roomCount(hub) == 2 }, "both rooms should be created")
	})

	t.Run("connection still open on message failure", func(t *testing.T) {
		handler := &fakeMessageHandler{err: fmt.Errorf("some error")}
		url := startServer(t, newHub(t), &fakeUserServiceClient{}, handler)
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		raw := mustMarshal(t, msg("a", "hehehe"))
		for i := 0; i < 2; i++ {
			if err := c.WriteMessage(websocket.TextMessage, raw); err != nil {
				t.Fatal(err)
			}
		}

		eventually(t, func() bool { return len(handler.received()) == 2 },
			"handler should receive both messages even though it returns an error")
	})

	t.Run("unregister called on connection closure", func(t *testing.T) {
		hub := newHub(t)
		url := startServer(t, hub, &fakeUserServiceClient{}, &fakeMessageHandler{})
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		eventually(t, func() bool { return roomCount(hub) == 2 }, "rooms should be created")

		c.Close()

		eventually(t, func() bool { return roomCount(hub) == 0 }, "rooms should be deleted after close")
	})
}

func TestGrpc(t *testing.T) {
	t.Run("connect called on connection upgrade", func(t *testing.T) {
		connected := make(chan *contractsv1.ConnectUserRequest, 1)
		fake := &fakeUserServiceClient{
			connectFn: func(_ context.Context, req *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
				connected <- req
				return &contractsv1.ConnectionResponse{Success: true}, nil
			},
		}
		url := startServer(t, newHub(t), fake, &fakeMessageHandler{})
		dial(t, url+"?client_id=42&room_ids=a,b")

		select {
		case req := <-connected:
			if req.ClientId != "42" {
				t.Errorf("client id = %q, want %q", req.ClientId, "42")
			}
			if req.InstanceName != testDial {
				t.Errorf("instance name = %q, want %q", req.InstanceName, testDial)
			}
			if got := strings.Join(req.RoomsId, ","); got != "a,b" {
				t.Errorf("rooms = %q, want %q", got, "a,b")
			}
		case <-time.After(waitTimeout):
			t.Fatal("grpc connect was not called")
		}
	})

	t.Run("connection closed on connect error", func(t *testing.T) {
		fake := &fakeUserServiceClient{
			connectFn: func(context.Context, *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
				return nil, fmt.Errorf("mock error")
			},
		}
		url := startServer(t, newHub(t), fake, &fakeMessageHandler{})
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		expectServerClose(t, c)
	})

	t.Run("connection closed on unsuccessful connect response", func(t *testing.T) {
		fake := &fakeUserServiceClient{
			connectFn: func(context.Context, *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
				return &contractsv1.ConnectionResponse{Success: false}, nil
			},
		}
		url := startServer(t, newHub(t), fake, &fakeMessageHandler{})
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		expectServerClose(t, c)
	})

	t.Run("disconnect called on connection closure", func(t *testing.T) {
		disconnected := make(chan *contractsv1.DisconnectUserRequest, 1)
		fake := &fakeUserServiceClient{
			disconnectFn: func(_ context.Context, req *contractsv1.DisconnectUserRequest) (*contractsv1.ConnectionResponse, error) {
				disconnected <- req
				return &contractsv1.ConnectionResponse{Success: true}, nil
			},
		}
		hub := newHub(t)
		url := startServer(t, hub, fake, &fakeMessageHandler{})
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		eventually(t, func() bool { return roomCount(hub) == 2 }, "rooms should be created")

		c.Close()

		select {
		case req := <-disconnected:
			if req.ClientId != "42" {
				t.Errorf("client id = %q, want %q", req.ClientId, "42")
			}
		case <-time.After(waitTimeout):
			t.Fatal("grpc disconnect was not called")
		}
	})
}

func TestMessage(t *testing.T) {
	t.Run("message deserialized and passed to ingest handler", func(t *testing.T) {
		handler := &fakeMessageHandler{}
		url := startServer(t, newHub(t), &fakeUserServiceClient{}, handler)
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		want := msg("a", "hehehe")
		if err := c.WriteMessage(websocket.TextMessage, mustMarshal(t, want)); err != nil {
			t.Fatal(err)
		}

		eventually(t, func() bool { return len(handler.received()) == 1 }, "message not received by handler")
		assertSameMessage(t, handler.received()[0], want)
	})

	t.Run("malformed json is dropped and connection stays open", func(t *testing.T) {
		handler := &fakeMessageHandler{}
		url := startServer(t, newHub(t), &fakeUserServiceClient{}, handler)
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		if err := c.WriteMessage(websocket.TextMessage, []byte("{not json")); err != nil {
			t.Fatal(err)
		}
		want := msg("a", "hehehe")
		if err := c.WriteMessage(websocket.TextMessage, mustMarshal(t, want)); err != nil {
			t.Fatal(err)
		}

		eventually(t, func() bool { return len(handler.received()) == 1 }, "valid message after a bad one should still be handled")
		assertSameMessage(t, handler.received()[0], want)
	})
}

func newTestWs(t *testing.T) *Ws {
	t.Helper()
	w, err := NewWs(&fakeUserServiceClient{}, &Hub{}, testDial, &fakeMessageHandler{})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func msg(roomID, payload string) chat.Message {
	serializedPayload, _ := json.Marshal(payload)
	return chat.Message{
		RoomID:  roomID,
		Payload: serializedPayload,
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertSameMessage(t *testing.T, got, want chat.Message) {
	t.Helper()
	if got.RoomID != want.RoomID || !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("wrong message passed: got %+v, want %+v", got, want)
	}
}

func newHub(t *testing.T) *Hub {
	t.Helper()
	hub, err := NewHub()
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func roomCount(h *Hub) int {
	return len(h.rooms)
}

func eventually(t *testing.T, cond func() bool, failMsg string) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(failMsg)
}

func expectServerClose(t *testing.T, c *websocket.Conn) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(waitTimeout))
	_, _, err := c.ReadMessage()
	if err == nil {
		t.Fatal("expected connection to be closed by the server")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("server did not close the connection in time")
	}
	t.Logf("got expected close error: %v", err)
}

func startServer(t *testing.T, hub *Hub, client contractsv1.UserServiceClient, ingest IngestHandler) string {
	t.Helper()
	w, err := NewWs(client, hub, testDial, ingest)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(w.ServeWS))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
