package chat

import (
	"context"

	"github.com/IBM/sarama"
)

const (
	topic = "messages"
)

type MessagePublisher interface {
	Publish(ctx context.Context, roomId string, value []byte) error
}

type kafkaPublisher struct {
	producer sarama.SyncProducer
}

func NewPublisher(producer sarama.SyncProducer) MessagePublisher {
	return &kafkaPublisher{
		producer: producer,
	}
}

// TODO:
// unit tests
// load tests
func (p *kafkaPublisher) Publish(ctx context.Context, roomId string, value []byte) error {
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(roomId),
		Value: sarama.ByteEncoder(value),
	})
	return err
}
