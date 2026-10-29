package messages

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const Version = 1

const (
	SendFrameType     = "send"
	AcceptedFrameType = "accepted"
	RejectedFrameType = "rejected"
)

const (
	InvalidRequestErrorCode = "invalid_request"
	NotAcceptedErrorCode    = "not_accepted"
)

type SendRequest struct {
	Type            string          `json:"type"`
	Version         int             `json:"version"`
	ClientMessageID string          `json:"client_message_id"`
	RoomID          string          `json:"room_id"`
	Payload         json.RawMessage `json:"payload"`
}

func (r SendRequest) Validate() error {
	if r.Type != SendFrameType {
		return errors.New("type must be send")
	}
	if r.Version != Version {
		return errors.New("version must be 1")
	}
	if strings.TrimSpace(r.ClientMessageID) == "" {
		return errors.New("client_message_id is required")
	}
	if strings.TrimSpace(r.RoomID) == "" {
		return errors.New("room_id is required")
	}
	if !json.Valid(r.Payload) {
		return errors.New("payload must be valid JSON")
	}
	return nil
}

type Event struct {
	Version         int             `json:"version"`
	MessageID       string          `json:"message_id"`
	ClientMessageID string          `json:"client_message_id"`
	RoomID          string          `json:"room_id"`
	RoomSeq         int64           `json:"room_seq"`
	CreatedAt       time.Time       `json:"created_at"`
	Payload         json.RawMessage `json:"payload"`
}

func (e Event) Validate() error {
	if e.Version != Version {
		return errors.New("version must be 1")
	}
	if strings.TrimSpace(e.MessageID) == "" {
		return errors.New("message_id is required")
	}
	if strings.TrimSpace(e.ClientMessageID) == "" {
		return errors.New("client_message_id is required")
	}
	if strings.TrimSpace(e.RoomID) == "" {
		return errors.New("room_id is required")
	}
	if e.RoomSeq < 1 {
		return errors.New("room_seq must be positive")
	}
	if e.CreatedAt.IsZero() {
		return errors.New("created_at is required")
	}
	if !json.Valid(e.Payload) {
		return errors.New("payload must be valid JSON")
	}
	return nil
}

type AcceptedAck struct {
	Type            string    `json:"type"`
	Version         int       `json:"version"`
	MessageID       string    `json:"message_id"`
	ClientMessageID string    `json:"client_message_id"`
	RoomID          string    `json:"room_id"`
	RoomSeq         int64     `json:"room_seq"`
	CreatedAt       time.Time `json:"created_at"`
}

type RejectedAck struct {
	Type            string `json:"type"`
	Version         int    `json:"version"`
	ClientMessageID string `json:"client_message_id,omitempty"`
	ErrorCode       string `json:"error_code"`
}

func (a RejectedAck) Validate() error {
	if a.Type != RejectedFrameType {
		return errors.New("type must be rejected")
	}
	if a.Version != Version {
		return errors.New("version must be 1")
	}
	if strings.TrimSpace(a.ErrorCode) == "" {
		return errors.New("error_code is required")
	}
	return nil
}

func (a AcceptedAck) Validate() error {
	if a.Type != AcceptedFrameType {
		return errors.New("type must be accepted")
	}
	if a.Version != Version {
		return errors.New("version must be 1")
	}
	if strings.TrimSpace(a.MessageID) == "" {
		return errors.New("message_id is required")
	}
	if strings.TrimSpace(a.ClientMessageID) == "" {
		return errors.New("client_message_id is required")
	}
	if strings.TrimSpace(a.RoomID) == "" {
		return errors.New("room_id is required")
	}
	if a.RoomSeq < 1 {
		return errors.New("room_seq must be positive")
	}
	if a.CreatedAt.IsZero() {
		return errors.New("created_at is required")
	}
	return nil
}
