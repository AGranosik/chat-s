package app

import (
	"log"

	"github.com/IBM/sarama"
)

type MessageService struct {
}

func NewMessageService() (*MessageService, error) {
	return &MessageService{}, nil
}
func (s *MessageService) Process(msg *sarama.ConsumerMessage) error {
	log.Printf("topic=%s partition=%d offset=%d key=%s value=%s",
		msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
	return nil
}
