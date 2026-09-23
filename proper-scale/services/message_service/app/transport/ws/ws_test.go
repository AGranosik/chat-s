package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"google.golang.org/grpc"
)

type fakeUserServiceClient struct {
}

func (s *fakeUserServiceClient) Connect(ctx context.Context, in *contractsv1.ConnectUserRequest, opts ...grpc.CallOption) (*contractsv1.ConnectionResponse, error) {
	return nil, nil
}
func (s *fakeUserServiceClient) Disconnect(ctx context.Context, in *contractsv1.DisconnectUserRequest, opts ...grpc.CallOption) (*contractsv1.ConnectionResponse, error) {
	return nil, nil
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
	hub := newHub()

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
		req := httptest.NewRequest(http.MethodGet, "/ws?client_id=1&room_ids=123", nil)
		rec := httptest.NewRecorder()

		hub.ServeWS(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("got status %d, want %d", rec.Code, http.StatusOK)
		}
	})
}

func newHub() *Ws {
	hub, _ := NewWsHub(&fakeUserServiceClient{}, &Hub{}, "some-dial")
	return hub
}
