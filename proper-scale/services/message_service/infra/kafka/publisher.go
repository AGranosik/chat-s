package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

const (
	topic = "messages"
)

type kafkaPublisher struct {
	producer sarama.SyncProducer
}

func NewPublisher(brokers []string) (*kafkaPublisher, error) {
	producer, err := newProducer(brokers)

	if err != nil {
		return nil, err
	}
	return &kafkaPublisher{
		producer: producer,
	}, nil
}

// TODO: sequence number somehow
// TODO: get messsage offset id or number or sth that will help order messages for client
// TODO: load balancer per active messages connection
func (p *kafkaPublisher) Publish(ctx context.Context, roomId string, value []byte) error {
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(roomId),
		Value: sarama.ByteEncoder(value),
	})
	return err
}
