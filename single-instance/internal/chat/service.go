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

var ErrInvalid = errors.New("invalid message")

type Broadcaster interface {
	Broadcast(roomID string, msg models.Message)
}

type Incoming struct {
	UserID string `json:"user_id"`
	Body   string `json:"body"`
}

type Service struct {
	store *storage.Store
}

func NewService(store *storage.Store) *Service {
	return &Service{store: store}
}

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
		return storage.EnqueueOutbox(ctx, tx, roomID, payload)
	})
}
