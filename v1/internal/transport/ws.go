package transport

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/websocket"

	"chat-s/internal/chat"
	"chat-s/internal/hub"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Origin check is open for now — tightened in PLAN Phase 7 (hardening).
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// handleWS upgrades GET /ws?room=<id> and registers the client with the hub.
func (rt *Router) handleWS(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		http.Error(w, "missing room", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed | room=%s err=%v", roomID, err)
		return
	}

	client := hub.NewClient(rt.hub, conn, roomID)
	rt.hub.Register(client)

	go client.WritePump(rt.ctx)
	go client.ReadPump(func(data []byte) {
		var in chat.Incoming
		if err := json.Unmarshal(data, &in); err != nil {
			log.Printf("ws decode | room=%s err=%v", roomID, err)
			return
		}
		if err := rt.svc.HandleIncoming(rt.ctx, roomID, in); err != nil {
			log.Printf("ws handle | room=%s err=%v", roomID, err)
		}
	})
}
