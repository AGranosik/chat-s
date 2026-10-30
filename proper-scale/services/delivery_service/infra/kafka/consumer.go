package kafka

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/IBM/sarama"
)

const consumerRetryBackoff = time.Second

type Consumer struct {
	cfg     Config
	handler *Handler
}

func NewConsumer(h *Handler, c Config) (*Consumer, error) {
	return &Consumer{
		cfg:     c,
		handler: h,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	group, err := sarama.NewConsumerGroup(c.cfg.Brokers, c.cfg.GroupID, c.cfg.newSaramaConfig())
	if err != nil {
		return err
	}
	defer group.Close()

	go func() {
		for err := range group.Errors() {
			log.Printf("consumer group error: %v", err)
		}
	}()
	for {
		err := group.Consume(ctx, c.cfg.Topics, c.handler)
		if errors.Is(err, sarama.ErrClosedConsumerGroup) || ctx.Err() != nil {
			return nil
		}
		if err != nil {
			log.Printf("consume error: %v", err)
		}
		timer := time.NewTimer(consumerRetryBackoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil
		}
	}
}
