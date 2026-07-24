package storage

import "context"

type OutboxEvent struct {
	ID      int64
	RoomID  string
	Message models.Message
	// concern separation
	// how to keep code clean
}

func DispatchBatch(ctx context.Context) (int, error)
