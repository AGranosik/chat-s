package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type Publisher interface {
	Publish(context.Context, string, string, []byte) (int32, int64, error)
}

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Net.DialTimeout = 5 * time.Second
	cfg.Net.ReadTimeout = 10 * time.Second
	cfg.Net.WriteTimeout = 10 * time.Second
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond
	cfg.Producer.Idempotent = true
	cfg.Producer.Partitioner = sarama.NewHashPartitioner

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &Producer{producer: producer}, nil
}

func (p *Producer) Publish(ctx context.Context, topic, roomID string, event []byte) (int32, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(roomID),
		Value: sarama.ByteEncoder(event),
	}
	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		return 0, 0, fmt.Errorf("send Kafka message: %w", err)
	}
	return partition, offset, nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
