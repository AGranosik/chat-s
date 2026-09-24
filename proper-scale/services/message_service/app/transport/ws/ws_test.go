package ws

import (
	"context"
	"messages/chat"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
)

type fakeMessageHandler struct {
	m      chat.Message
	called bool
}

func (h *fakeMessageHandler) HandleIncoming(ctx context.Context, m chat.Message) error {
	h.called = true

	h.m = m

	return nil
}

type fakeUserServiceClient struct {
	connectFn    func(context.Context, *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error)
	disconnectFn func(context.Context, *contractsv1.DisconnectUserRequest) (*contractsv1.ConnectionResponse, error)
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

func TestNewWsHub(t *testing.T) {
	t.Run("nil hub returns error", func(t *testing.T) {

		_, err := NewWsHub(&fakeUserServiceClient{}, nil, "somme fake")

		if err == nil {
			t.Errorf("Should return error on creation when hub is null.")
		}
	})

	t.Run("dial cannot be empty", func(t *testing.T) {
		_, err := NewWsHub(&fakeUserServiceClient{}, &Hub{}, "")

		if err == nil {
			t.Errorf("Should return error on creation.")
		}
	})

	t.Run("creation sucess", func(t *testing.T) {
		_, err := NewWsHub(&fakeUserServiceClient{}, &Hub{}, "some-dial")

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

	//TODO: make it pass
	// t.Run("single rooom - success", func(t *testing.T) {
	// 	req := httptest.NewRequest(http.MethodGet, "/ws?client_id=1&room_ids=123", nil)
	// 	rec := httptest.NewRecorder()

	// 	hub.ServeWS(rec, req)
	// 	if rec.Code != http.StatusOK {
	// 		t.Fatalf("got status %d, want %d", rec.Code, http.StatusOK)
	// 	}
	// })
}

func TestHub(t *testing.T) {
	// ws := newWsHub()

	t.Run("room created", func(t *testing.T) {
		handler := &fakeMessageHandler{}
		hub := newHub(handler)

		// connected := make(chan *contractsv1.ConnectUserRequest, 1)
		// disconnected := make(chan *contractsv1.DisconnectUserRequest, 1)
		fake := &fakeUserServiceClient{}
		// fake := &fakeUserServiceClient{
		// 	connectFn: func(_ context.Context, in *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
		// 		connected <- in
		// 		return &contractsv1.ConnectionResponse{Success: true}, nil
		// 	},
		// 	disconnectFn: func(_ context.Context, in *contractsv1.DisconnectUserRequest) (*contractsv1.ConnectionResponse, error) {
		// 		disconnected <- in
		// 		return &contractsv1.ConnectionResponse{Success: true}, nil
		// 	},
		// }
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		if len(hub.rooms) == 0 {
			t.Errorf("Room should be created.")
		}
		c.Close()
	})

	t.Run("message passed further", func(t *testing.T) {
		handler := &fakeMessageHandler{}
		hub := newHub(handler)
		fake := &fakeUserServiceClient{}
		url := startServer(t, hub, fake)
		c := dial(t, url+"?client_id=42&room_ids=a,b")

		if !handler.called {
			t.Errorf("no message")
		}

		c.Close()
	})
	t.Run("connection open on message failure", func(t *testing.T) {})
	t.Run("unregister called on connection closure", func(t *testing.T) {})
}

func TestGrpc(t *testing.T) {

}

func TestMessage(t *testing.T) {

}

func TestFallback(t *testing.T) {

}

func newWsHub() *Ws {
	hub, _ := NewWsHub(&fakeUserServiceClient{}, &Hub{}, "some-dial")
	return hub
}

// TODO: make naming better
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
	w, err := NewWsHub(client, hub, "some-dial") // use your real Hub constructor
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
