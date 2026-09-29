package kafka

import (
	"log"

	"github.com/IBM/sarama"
)

type handler struct {
	process func(*sarama.ConsumerMessage) error
}

func (h *handler) Setup(sarama.ConsumerGroupSession) error { return nil }

func (h *handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if err := h.process(msg); err != nil {

				log.Printf("process failed (partition %d, offset %d): %v",
					msg.Partition, msg.Offset, err)
				continue
			}
			session.MarkMessage(msg, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
