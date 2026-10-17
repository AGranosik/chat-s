package chat

import "encoding/json"

type Message struct {
	RoomID  string          `json:"room_id"`
	Payload json.RawMessage `json:"payload"`
}
