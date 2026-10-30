package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
	contractsv1 "github.com/AGranosik/chat/contracts/protos"
)

type Message struct {
	Key, Value []byte
	Headers    map[string]string
	Timestamp  time.Time
}

type MessagePublisher interface {
	PublishMessage(ctx context.Context, m Message, instancesNames []string) error
}

type MessageService struct {
	publisher MessagePublisher
	grpc      contractsv1.UserServiceClient
}

func NewMessageHandler(p MessagePublisher, c contractsv1.UserServiceClient) (*MessageService, error) {
	if p == nil {
		return nil, errors.New("message publisher is required")
	}
	if c == nil {
		return nil, errors.New("presence client is required")
	}
	return &MessageService{
		publisher: p,
		grpc:      c,
	}, nil
}

func (s *MessageService) Handle(ctx context.Context, m Message) error {
	var event messagev1.Event
	if err := json.Unmarshal(m.Value, &event); err != nil {
		return fmt.Errorf("decode v1 message event: %w", err)
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate v1 message event: %w", err)
	}
	if string(m.Key) != event.RoomID {
		return fmt.Errorf("Kafka room key %q does not match event room %q", string(m.Key), event.RoomID)
	}

	res, err := s.grpc.IsConnected(ctx, &contractsv1.ClientsConnectionDestRequest{
		RoomId: event.RoomID,
	})

	if err != nil {
		return fmt.Errorf("presence lookup for room %q: %w", event.RoomID, err)
	}
	if res == nil {
		return errors.New("presence lookup returned an empty response")
	}
	instanceNames := res.GetInstanceNames()
	if len(instanceNames) == 0 {
		slog.Warn("live delivery miss: no connected instances",
			"room_id", event.RoomID,
			"message_id", event.MessageID,
			"room_seq", event.RoomSeq,
		)
		return nil
	}

	return s.publisher.PublishMessage(ctx, m, instanceNames)
}
