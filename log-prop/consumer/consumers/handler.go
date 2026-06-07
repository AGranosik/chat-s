package consumers

import (
	"log"
	"main/decoders"
	"main/models"
	"time"

	"github.com/IBM/sarama"
)

type consumerGroupHandler struct {
	decoder decoders.Decoder[models.LogEvent]
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("consumer session setup")
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("consumer session cleanup")
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		event, err := h.decoder.Decode(msg.Value)
		if err != nil {
			log.Printf("failed to decode message at offset=%d: %v", msg.Offset, err)
			session.MarkMessage(msg, "")
			continue
		}

		log.Printf(
			"[partition=%d offset=%d] level=%s service=%s message=%q time=%s",
			msg.Partition,
			msg.Offset,
			event.Level,
			event.Service,
			event.Message,
			event.Time.Format(time.RFC3339),
		)

		session.MarkMessage(msg, "")
	}
	return nil
}
