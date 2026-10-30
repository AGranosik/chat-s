package main

import (
	"encoding/json"
	"testing"
	"time"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
)

func TestDecodeServerFrame(t *testing.T) {
	t.Run("accepted acknowledgement", func(t *testing.T) {
		data := []byte(`{"type":"accepted","version":1,"message_id":"message-1","client_message_id":"client-key-1","room_id":"room-1","room_seq":7,"created_at":"2026-10-28T12:00:00Z"}`)
		frame, err := decodeServerFrame(data)
		if err != nil {
			t.Fatal(err)
		}
		if frame.kind != messagev1.AcceptedFrameType || frame.accepted.RoomSeq != 7 {
			t.Fatalf("frame = %+v", frame)
		}
	})

	t.Run("live event envelope", func(t *testing.T) {
		event := messagev1.Event{
			Version:         messagev1.Version,
			MessageID:       "message-1",
			ClientMessageID: "client-key-1",
			RoomID:          "room-1",
			RoomSeq:         7,
			CreatedAt:       time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
			Payload:         json.RawMessage(`{"text":"hello"}`),
		}
		data, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := decodeServerFrame(data)
		if err != nil {
			t.Fatal(err)
		}
		if frame.kind != "event" || frame.event.RoomSeq != 7 {
			t.Fatalf("frame = %+v", frame)
		}
	})
}

func TestWebsocketURLIncludesClientAndRoomSubscriptions(t *testing.T) {
	got, err := websocketURL("wss://chat.example/ws?token=abc", "client 1", []string{"room-1", "room-2"})
	if err != nil {
		t.Fatal(err)
	}
	want := "wss://chat.example/ws?client_id=client+1&room_ids=room-1%2Croom-2&token=abc"
	if got != want {
		t.Fatalf("WebSocket URL = %q, want %q", got, want)
	}
}
