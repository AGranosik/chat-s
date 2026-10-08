package app

import (
	"context"
	"fmt"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
)

type Message struct {
	Key, Value []byte
	Headers    map[string]string
	Timestamp  time.Time
	//todo: missing msg service
}

type MessagePublisher interface {
	PublishMessage(ctx context.Context, m Message) error
}

type MessageService struct {
	publisher MessagePublisher
	grpc      contractsv1.UserServiceClient
}

func NewMessageHandler(p MessagePublisher, c contractsv1.UserServiceClient) (*MessageService, error) {
	return &MessageService{
		publisher: p,
		grpc:      c,
	}, nil
}

func (s *MessageService) Handle(ctx context.Context, m Message) error {
	res, err := s.grpc.IsConnected(ctx, &contractsv1.ClientsConnectionDestRequest{
		RoomId: string(m.Key),
	})

	if err != nil {
		return fmt.Errorf("grpc connection request error: %v", err)
	}

	if len(res.GetInstanceNames()) == 0 {
		return nil
	}

	return s.publisher.PublishMessage(ctx, m)
}
