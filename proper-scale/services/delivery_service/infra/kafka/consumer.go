package kafka

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type Consumer struct {
	cfg     Config
	handler *Handler
}

func NewConsumer(h *Handler) (*Consumer, error) {
	config := loadConfig()
	return &Consumer{
		cfg:     config,
		handler: h,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	group, err := sarama.NewConsumerGroup(c.cfg.brokers, c.cfg.groupID, c.cfg.newSaramaConfig())
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
	for {
		if err := group.Consume(ctx, c.cfg.topics, c.handler); err != nil {
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
