package kafka

import (
	"log"

	"github.com/IBM/sarama"
)

type Handler struct {
	process func(*sarama.ConsumerMessage) error
}

func NewHandler(p func(*sarama.ConsumerMessage) error) *Handler {
	return &Handler{
		process: p,
	}
}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error { return nil }

func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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
