package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"messages/chat"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
)

type fakeMessageHandler struct {
	m      chat.Message
	called bool
	err    error
}

func (h *fakeMessageHandler) HandleIncoming(ctx context.Context, m chat.Message) error {
	h.called = true
	h.m = m
	return h.err
}

type fakeUserServiceClient struct {
	connectFn     func(context.Context, *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error)
	disconnectFn  func(context.Context, *contractsv1.DisconnectUserRequest) (*contractsv1.ConnectionResponse, error)
	isConnectedFn func(context.Context, *contractsv1.IsUserConnectedRequest) (*contractsv1.UserConnectionResponse, error)
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

func (f *fakeUserServiceClient) IsConnected(ctx context.Context, in *contractsv1.IsUserConnectedRequest, _ ...grpc.CallOption) (*contractsv1.UserConnectionResponse, error) {
	if f.isConnectedFn != nil {
		return f.isConnectedFn(ctx, in)
	}
	return &contractsv1.UserConnectionResponse{}, nil
}

func TestNewWsHub(t *testing.T) {
	t.Run("nil hub returns error", func(t *testing.T) {

		_, err := NewWs(&fakeUserServiceClient{}, nil, "somme fake")

		if err == nil {
			t.Errorf("Should return error on creation when hub is null.")
		}
	})

	t.Run("dial cannot be empty", func(t *testing.T) {
		_, err := NewWs(&fakeUserServiceClient{}, &Hub{}, "")

		if err == nil {
			t.Errorf("Should return error on creation.")
		}
	})

	t.Run("creation sucess", func(t *testing.T) {
		_, err := NewWs(&fakeUserServiceClient{}, &Hub{}, "some-dial")

		if err != nil {
			t.Errorf("Should create successfully")
		}
	})
}

func TestWsQuery(t *testing.T) {
	hub := newWsHub()

	t.Run("no client id - bad request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ws", nil)
		rec := httptest.NewRecorder()

		hub.ServeWS(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("got status %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("no rooms - bad request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ws?client_id=1", nil)
		rec := httptest.NewRecorder()

		hub.ServeWS(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("got status %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("single rooom - success", func(t *testing.T) {
		handler := &fakeMessageHandler{}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")
		time.Sleep(1 * time.Second)
		if len(hub.rooms) == 0 {
			t.Errorf("Room should be created.")
		}
		c.Close()
	})
}

func TestHub(t *testing.T) {
	// ws := newWsHub()

	t.Run("room created", func(t *testing.T) {
		handler := &fakeMessageHandler{}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		time.Sleep(1 * time.Second)
		if len(hub.rooms) == 0 {
			t.Errorf("Room should be created.")
		}
		c.Close()
	})

	t.Run("connection still open on message failure", func(t *testing.T) {
		handler := &fakeMessageHandler{
			err: fmt.Errorf("some error"),
		}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		payload := "hehehe"
		msg := msg("a", payload)
		serializedMsg, _ := json.Marshal(msg)
		if err := c.WriteMessage(websocket.TextMessage, serializedMsg); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)

		//still can write message
		if err := c.WriteMessage(websocket.TextMessage, serializedMsg); err != nil {
			t.Fatal(err)
		}

		c.Close()
	})
	t.Run("unregister called on connection closure", func(t *testing.T) {
		handler := &fakeMessageHandler{
			err: fmt.Errorf("some error"),
		}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		time.Sleep(1 * time.Second)
		c.Close()
		time.Sleep(1 * time.Second)

		if len(hub.rooms) != 0 {
			t.Errorf("Room should be deleted")
		}
	})
}

func TestGrpc(t *testing.T) {
	t.Run("connect called on connection upgrade", func(t *testing.T) {
		called := false

		handler := &fakeMessageHandler{
			err: fmt.Errorf("some error"),
		}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{
			connectFn: func(ctx context.Context, cur *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
				called = true
				return &contractsv1.ConnectionResponse{
					Success: true,
				}, nil
			},
		}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")
		time.Sleep(1 * time.Second)

		c.Close()

		if !called {
			t.Errorf("grpc connect don't called")
		}

	})

	t.Run("connect closed on connect error", func(t *testing.T) {
		handler := &fakeMessageHandler{
			err: fmt.Errorf("some error"),
		}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{
			connectFn: func(ctx context.Context, cur *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
				return nil, fmt.Errorf("mock error")
			},
		}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")
		_, _, err := c.ReadMessage()
		if err == nil {
			t.Fatal("expected connection to be closed by the server")
		}
		t.Logf("got expected close error: %v", err)
	})

	t.Run("connect closed on not success connection", func(t *testing.T) {
		handler := &fakeMessageHandler{
			err: fmt.Errorf("some error"),
		}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{
			connectFn: func(ctx context.Context, cur *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
				return &contractsv1.ConnectionResponse{
					Success: false,
				}, nil
			},
		}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")
		_, _, err := c.ReadMessage()
		if err == nil {
			t.Fatal("expected connection to be closed by the server")
		}
		t.Logf("got expected close error: %v", err)
	})
	t.Run("disconnect called on connection closure", func(t *testing.T) {
		called := false
		handler := &fakeMessageHandler{
			err: fmt.Errorf("some error"),
		}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{
			connectFn: func(ctx context.Context, cur *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
				return &contractsv1.ConnectionResponse{
					Success: true,
				}, nil
			},
			disconnectFn: func(ctx context.Context, dur *contractsv1.DisconnectUserRequest) (*contractsv1.ConnectionResponse, error) {
				called = true
				return &contractsv1.ConnectionResponse{
					Success: true,
				}, nil
			},
		}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")
		time.Sleep(1 * time.Second)

		c.Close()
		time.Sleep(1 * time.Second)
		_, _, err := c.ReadMessage()
		if err == nil {
			t.Fatal("expected connection to be closed by the server")
		}

		if !called {
			t.Errorf("grpc disconnect not called")
		}
	})
}

func TestMessage(t *testing.T) {
	t.Run("message passed further", func(t *testing.T) {
		handler := &fakeMessageHandler{}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		payload := "hehehe"
		serializedPayload, _ := json.Marshal(payload)
		msg := chat.Message{
			RoomID:  "a",
			Payload: serializedPayload,
		}

		serializedMsg, _ := json.Marshal(msg)
		if err := c.WriteMessage(websocket.TextMessage, serializedMsg); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		if !handler.called {
			t.Errorf("Message not received")
		}

		c.Close()
	})

	t.Run("message deserialized", func(t *testing.T) {
		handler := &fakeMessageHandler{}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		payload := "hehehe"
		msg := msg("a", payload)

		serializedMsg, _ := json.Marshal(msg)
		if err := c.WriteMessage(websocket.TextMessage, serializedMsg); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		if handler.m.RoomID != msg.RoomID || !bytes.Equal(msg.Payload, handler.m.Payload) {
			t.Errorf("Wrong message passed")
		}

		c.Close()
	})
}

func newWsHub() *Ws {
	hub, _ := NewWs(&fakeUserServiceClient{}, &Hub{}, "some-dial")
	return hub
}

func msg(m string, r string) chat.Message {
	serializedPayload, _ := json.Marshal(m)
	return chat.Message{
		RoomID:  r,
		Payload: serializedPayload,
	}
}

func newHub(handler *fakeMessageHandler) *Hub {
	if handler == nil {
		handler = &fakeMessageHandler{}
	}
	hub, _ := NewHub(handler)
	return hub
}

func startServer(t *testing.T, hub *Hub, client contractsv1.UserServiceClient) string {
	t.Helper()
	if hub == nil {
		hub = &Hub{}
	}
	w, err := NewWs(client, hub, "some-dial") // use your real Hub constructor
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(w.ServeWS))
	// t.Cleanup(srv.Close)
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
