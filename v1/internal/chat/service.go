package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"chat-s/internal/models"
	"chat-s/internal/storage"
)

const maxBodyLen = 4000

// ErrInvalid is returned when an incoming frame fails validation.
var ErrInvalid = errors.New("invalid message")

// Broadcaster fans a message out to everyone in a room. The in-memory hub
// implements it today; a Redis/Kafka impl implements the same interface when we
// scale out (see docs/ARCHITECTURE.md "scaling seam"). The outbox relay is the
// only caller.
type Broadcaster interface {
	Broadcast(roomID string, msg models.Message)
}

// Incoming is a message frame received over the websocket.
type Incoming struct {
	UserID string `json:"user_id"`
	Body   string `json:"body"`
}

// Service runs validate → (persist + enqueue) for inbound messages. It does not
// broadcast — the outbox relay does, after the transaction commits.
type Service struct {
	store *storage.Store
}

func NewService(store *storage.Store) *Service {
	return &Service{store: store}
}

// HandleIncoming validates the frame, then inserts the message and its outbox
// event in one transaction. A pg_notify fired on commit wakes the relay. See
// docs/ARCHITECTURE.md "The outbox".
func (s *Service) HandleIncoming(ctx context.Context, roomID string, in Incoming) error {
	body := strings.TrimSpace(in.Body)
	switch {
	case body == "":
		return fmt.Errorf("%w: empty body", ErrInvalid)
	case len(body) > maxBodyLen:
		return fmt.Errorf("%w: body exceeds %d bytes", ErrInvalid, maxBodyLen)
	case in.UserID == "":
		return fmt.Errorf("%w: missing user_id", ErrInvalid)
	}

	msg := models.Message{RoomID: roomID, UserID: in.UserID, Body: body}

	return s.store.WithTx(ctx, func(tx pgx.Tx) error {
		if err := storage.InsertMessage(ctx, tx, &msg); err != nil {
			return err
		}
		payload, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal outbox payload: %w", err)
		}
		if err := storage.EnqueueOutbox(ctx, tx, roomID, payload); err != nil {
			return err
		}
		// NOTIFY is delivered to listeners on commit, so the relay dispatches
		// within milliseconds. Its periodic poll covers any missed signal.
		if _, err := tx.Exec(ctx, "select pg_notify('outbox_events', '')"); err != nil {
			return fmt.Errorf("notify: %w", err)
		}
		return nil
	})
}
