package kafka

import (
	"log"

	"github.com/IBM/sarama"
)

func process(msg *sarama.ConsumerMessage) error {
	log.Printf("topic=%s partition=%d offset=%d key=%s value=%s",
		msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
	return nil
}
