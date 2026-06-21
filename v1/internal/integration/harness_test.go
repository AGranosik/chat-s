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

// stack is the fully wired server (hub + service + relay + router) on an
// httptest.Server, mirroring cmd/server/main.go. recBC, when non-nil, taps the
// broadcaster so a test can observe relay hand-offs without a live ws client.
type stack struct {
	srv   *httptest.Server
	hub   *hub.Hub
	store *storage.Store
	svc   *chat.Service
}

// newStack resets the DB and wires the real components against the shared store,
// starting the hub + relay on a context cancelled in t.Cleanup. broadcaster
// picks which Broadcaster the relay feeds: pass nil to use the real hub (the
// production path); pass a recorder to observe events directly.
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

// wsURL converts the httptest base URL to a ws:// dial URL for a room.
func (s *stack) wsURL(room string) string {
	u := strings.Replace(s.srv.URL, "http://", "ws://", 1)
	return u + "/ws?room=" + url.QueryEscape(room)
}

// dialWS opens a websocket client to the room and closes it in t.Cleanup.
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

// send writes a chat frame over the socket.
func send(t *testing.T, conn *websocket.Conn, userID, body string) {
	t.Helper()
	frame, _ := json.Marshal(chat.Incoming{UserID: userID, Body: body})
	if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
		t.Fatalf("ws write: %v", err)
	}
}

// readMessage reads one broadcast message frame, failing on timeout.
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

// getJSON does a GET and decodes the JSON body into out.
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

// postJSON does a POST with a JSON body and returns the status + decoded body.
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

// recordingBroadcaster captures relay hand-offs for tests that don't use a ws
// client. Guarded by a mutex because the relay runs on its own goroutine.
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
