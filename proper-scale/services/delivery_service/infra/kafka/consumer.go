package kafka

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func Run(ctx context.Context, cfg Config) error {
	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, newSaramaConfig())
	if err != nil {
		return err
	}
	defer group.Close()

	// Ends when the group is closed.
	go func() {
		for err := range group.Errors() {
			log.Printf("consumer group error: %v", err)
		}
	}()

	h := &handler{process: process}
	for {
		if err := group.Consume(ctx, cfg.Topics, h); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) || ctx.Err() != nil {
				return nil
			}
			log.Printf("consume error: %v", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return nil
			}
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}
