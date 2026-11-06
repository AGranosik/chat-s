package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type Message struct {
	Key   string
	Value []byte
}

type Publisher interface {
	PublishBatch(ctx context.Context, topic string, messages []Message) []error
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

func (p *Producer) PublishBatch(ctx context.Context, topic string, messages []Message) []error {
	results := make([]error, len(messages))
	if len(messages) == 0 {
		return results
	}
	if err := ctx.Err(); err != nil {
		return fillErrors(results, err)
	}

	batch := make([]*sarama.ProducerMessage, len(messages))
	indexes := make(map[*sarama.ProducerMessage]int, len(messages))
	for i, message := range messages {
		producerMessage := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(message.Key),
			Value: sarama.ByteEncoder(message.Value),
		}
		batch[i] = producerMessage
		indexes[producerMessage] = i
	}

	err := p.producer.SendMessages(batch)
	if err == nil {
		return results
	}
	var producerErrors sarama.ProducerErrors
	if !errors.As(err, &producerErrors) {
		return fillErrors(results, fmt.Errorf("send Kafka batch: %w", err))
	}
	matched := false
	for _, producerError := range producerErrors {
		if i, ok := indexes[producerError.Msg]; ok {
			results[i] = fmt.Errorf("send Kafka message: %w", producerError.Err)
			matched = true
		}
	}
	if !matched {
		return fillErrors(results, fmt.Errorf("send Kafka batch: %w", err))
	}
	return results
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func fillErrors(results []error, err error) []error {
	for i := range results {
		results[i] = err
	}
	return results
}
