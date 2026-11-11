//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"chat-s/internal/chat"
	"chat-s/internal/hub"
	"chat-s/internal/models"
	"chat-s/internal/outbox"
	"chat-s/internal/storage"
	"chat-s/internal/transport"
)

type stack struct {
	srv   *httptest.Server
	hub   *hub.Hub
	store *storage.Store
	svc   *chat.Service
}

func newStack(t *testing.T, broadcaster chat.Broadcaster) *stack {
	t.Helper()
	freshDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	h := hub.New()
	go h.Run(ctx)

	svc := chat.NewService(testStore)

	bc := broadcaster
	if bc == nil {
		bc = h
	}
	relay := outbox.NewRelay(testStore, bc)
	go relay.Run(ctx)

	handler := transport.NewRouter(ctx, testStore, svc, h)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return &stack{srv: srv, hub: h, store: testStore, svc: svc}
}

func (s *stack) wsURL(room string) string {
	u := strings.Replace(s.srv.URL, "http://", "ws://", 1)
	return u + "/ws?room=" + url.QueryEscape(room)
}

func (s *stack) dialWS(t *testing.T, room string) *websocket.Conn {
	t.Helper()
	conn, resp, err := websocket.DefaultDialer.Dial(s.wsURL(room), nil)
	if err != nil {
		status := ""
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("dial ws (%s): %v", status, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func send(t *testing.T, conn *websocket.Conn, userID, body string) {
	t.Helper()
	frame, _ := json.Marshal(chat.Incoming{UserID: userID, Body: body})
	if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
		t.Fatalf("ws write: %v", err)
	}
}

func readMessage(t *testing.T, conn *websocket.Conn, timeout time.Duration) models.Message {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	var m models.Message
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("decode broadcast %q: %v", data, err)
	}
	return m
}

func (s *stack) getJSON(t *testing.T, path string, out any) {
	t.Helper()
	resp, err := http.Get(s.srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: status %d: %s", path, resp.StatusCode, b)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode GET %s: %v", path, err)
	}
}

func (s *stack) postJSON(t *testing.T, path string, in, out any) int {
	t.Helper()
	body, _ := json.Marshal(in)
	resp, err := http.Post(s.srv.URL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode POST %s: %v", path, err)
		}
	}
	return resp.StatusCode
}

type recordingBroadcaster struct {
	mu  sync.Mutex
	got []models.Message
}

func (r *recordingBroadcaster) Broadcast(_ string, msg models.Message) {
	r.mu.Lock()
	r.got = append(r.got, msg)
	r.mu.Unlock()
}

func (r *recordingBroadcaster) snapshot() []models.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]models.Message, len(r.got))
	copy(out, r.got)
	return out
}
