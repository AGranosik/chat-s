package transport

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"chat-s/internal/chat"
	"chat-s/internal/hub"
	"chat-s/internal/models"
	"chat-s/internal/storage"
)

const (
	defaultHistoryLimit = 50
	maxHistoryLimit     = 200
)

// Router wires REST + websocket handlers over the store, service, and hub.
type Router struct {
	store *storage.Store
	svc   *chat.Service
	hub   *hub.Hub
	ctx   context.Context // long-lived server context for connection pumps
}

// NewRouter returns the configured http.Handler. ctx is the server's lifetime
// context; connection pumps stop when it is cancelled.
func NewRouter(ctx context.Context, store *storage.Store, svc *chat.Service, h *hub.Hub) http.Handler {
	rt := &Router{store: store, svc: svc, hub: h, ctx: ctx}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", rt.handleHealth)
	mux.HandleFunc("GET /api/rooms", rt.handleListRooms)
	mux.HandleFunc("POST /api/rooms", rt.handleCreateRoom)
	mux.HandleFunc("GET /api/users", rt.handleListUsers)
	mux.HandleFunc("POST /api/users", rt.handleCreateUser)
	mux.HandleFunc("GET /api/rooms/{id}/messages", rt.handleHistory)
	mux.HandleFunc("GET /ws", rt.handleWS)
	return mux
}

func (rt *Router) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (rt *Router) handleListRooms(w http.ResponseWriter, r *http.Request) {
	rooms, err := rt.store.ListRooms(r.Context())
	if err != nil {
		log.Printf("list rooms | err=%v", err)
		http.Error(w, "list rooms failed", http.StatusInternalServerError)
		return
	}
	if rooms == nil {
		rooms = []models.Room{}
	}
	writeJSON(w, http.StatusOK, rooms)
}

func (rt *Router) handleCreateRoom(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
		http.Error(w, "invalid body: expected {\"name\": ...}", http.StatusBadRequest)
		return
	}
	room, err := rt.store.CreateRoom(r.Context(), body.Name)
	if err != nil {
		log.Printf("create room | err=%v", err)
		http.Error(w, "create room failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, room)
}

func (rt *Router) handleListUsers(w http.ResponseWriter, r *http.Request) {
	users, err := rt.store.ListUsers(r.Context())
	if err != nil {
		log.Printf("list users | err=%v", err)
		http.Error(w, "list users failed", http.StatusInternalServerError)
		return
	}
	if users == nil {
		users = []models.User{}
	}
	writeJSON(w, http.StatusOK, users)
}

func (rt *Router) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Username == "" {
		http.Error(w, "invalid body: expected {\"username\": ...}", http.StatusBadRequest)
		return
	}
	user, err := rt.store.CreateUser(r.Context(), body.Username)
	if err != nil {
		log.Printf("create user | err=%v", err)
		http.Error(w, "create user failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, user)
}

func (rt *Router) handleHistory(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("id")

	before := int64(0)
	if v := r.URL.Query().Get("before"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			http.Error(w, "invalid before cursor", http.StatusBadRequest)
			return
		}
		before = n
	}

	limit := defaultHistoryLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > maxHistoryLimit {
			http.Error(w, "invalid limit (1..200)", http.StatusBadRequest)
			return
		}
		limit = n
	}

	msgs, err := rt.store.History(r.Context(), roomID, before, limit)
	if err != nil {
		log.Printf("history | room=%s err=%v", roomID, err)
		http.Error(w, "history failed", http.StatusInternalServerError)
		return
	}
	if msgs == nil {
		msgs = []models.Message{}
	}
	writeJSON(w, http.StatusOK, msgs)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json | err=%v", err)
	}
}
