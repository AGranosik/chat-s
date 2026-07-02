package models

import "time"

// Room is a chat room. Clients bind to one room id when they open a socket.
type Room struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}
