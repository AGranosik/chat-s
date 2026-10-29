package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type retryingRedisConsumer struct {
	calls atomic.Int32
}

func (c *retryingRedisConsumer) Run(ctx context.Context) error {
	if c.calls.Add(1) == 1 {
		return errors.New("redis unavailable")
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestRunRedisConsumerRetriesAndStops(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	consumer := &retryingRedisConsumer{}

	if err := runRedisConsumer(ctx, consumer); err != nil {
		t.Fatalf("runRedisConsumer() error = %v", err)
	}
	if calls := consumer.calls.Load(); calls != 2 {
		t.Fatalf("consumer run calls = %d, want 2", calls)
	}
}
