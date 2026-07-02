package models

import "time"

// Message is a single chat message. IDs are bigserial (monotonic), so the id
// doubles as the keyset cursor for history pagination.
type Message struct {
	ID        int64     `json:"id"`
	RoomID    string    `json:"room_id"`
	UserID    string    `json:"user_id"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}
