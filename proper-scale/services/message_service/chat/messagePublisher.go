package chat

import (
	"context"

	"github.com/IBM/sarama"
)

type MessagePublisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

type kafkaPublisher struct {
	producer sarama.SyncProducer
}

func NewPublisher(producer sarama.SyncProducer) MessagePublisher {
	return &kafkaPublisher{
		producer: producer,
	}
}

func (p *kafkaPublisher) Publish(ctx context.Context, topic, key string, value []byte) error {
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	})
	return err
}
