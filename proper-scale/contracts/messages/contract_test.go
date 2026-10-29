package messages

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestV1EventShape(t *testing.T) {
	data := []byte(`{"version":1,"message_id":"0192f4d8-2c38-7a31-b5e4-40e8ef1d3ea6","client_message_id":"0192f4d8-2c38-7a31-b5e4-40e8ef1d3ea7","room_id":"room-123","room_seq":42,"created_at":"2026-10-28T12:00:00Z","payload":{"text":"hello"}}`)

	var event Event
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		t.Fatalf("decode event fixture: %v", err)
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("validate event fixture: %v", err)
	}
	var payload map[string]string
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("decode raw payload: %v", err)
	}
	if payload["text"] != "hello" {
		t.Fatalf("payload text = %q, want hello", payload["text"])
	}
}

func TestAcceptedAckValidate(t *testing.T) {
	ack := AcceptedAck{
		Type:            AcceptedFrameType,
		Version:         Version,
		MessageID:       "message-1",
		ClientMessageID: "client-key-1",
		RoomID:          "room-123",
		RoomSeq:         42,
		CreatedAt:       time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
	}
	if err := ack.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	ack.RoomSeq = 0
	if err := ack.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want non-positive room_seq error")
	}
}

func TestSendRequestValidate(t *testing.T) {
	tests := []struct {
		name    string
		request SendRequest
		wantErr bool
	}{
		{
			name: "valid request",
			request: SendRequest{
				Type:            SendFrameType,
				Version:         Version,
				ClientMessageID: "client-key-1",
				RoomID:          "room-123",
				Payload:         json.RawMessage(`{"text":"hello"}`),
			},
		},
		{
			name: "missing idempotency key",
			request: SendRequest{
				Type:    SendFrameType,
				Version: Version,
				RoomID:  "room-123",
				Payload: json.RawMessage(`{"text":"hello"}`),
			},
			wantErr: true,
		},
		{
			name: "unsupported version",
			request: SendRequest{
				Type:            SendFrameType,
				Version:         2,
				ClientMessageID: "client-key-1",
				RoomID:          "room-123",
				Payload:         json.RawMessage(`{"text":"hello"}`),
			},
			wantErr: true,
		},
		{
			name: "invalid payload",
			request: SendRequest{
				Type:            SendFrameType,
				Version:         Version,
				ClientMessageID: "client-key-1",
				RoomID:          "room-123",
				Payload:         json.RawMessage(`{`),
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.request.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}
