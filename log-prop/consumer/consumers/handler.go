package consumers

import (
	"kafka-consumer/decoders"
	"kafka-consumer/models"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type consumerGroupHandler struct {
	decoder decoders.Decoder[models.LogEvent]
	infoCh  chan models.LogEvent
	warnCh  chan models.LogEvent
	errorCh chan models.LogEvent
}

func newConsumerGroupHandler(decoder decoders.Decoder[models.LogEvent]) *consumerGroupHandler {
	h := &consumerGroupHandler{
		decoder: decoder,
		infoCh:  make(chan models.LogEvent, 100),
		warnCh:  make(chan models.LogEvent, 100),
		errorCh: make(chan models.LogEvent, 100),
	}

	go h.handleInfo()
	go h.handleWarn()
	go h.handleError()

	return h
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

		switch event.Level {
		case "INFO":
			h.infoCh <- event
		case "WARN":
			h.warnCh <- event
		case "ERROR":
			h.errorCh <- event
		default:
			log.Printf("unknown level %q, skipping", event.Level)
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func (h *consumerGroupHandler) handleInfo() {
	for event := range h.infoCh {
		log.Printf("[INFO] service=%s message=%q time=%s",
			event.Service, event.Message, event.Time.Format(time.RFC3339))
	}
}

func (h *consumerGroupHandler) handleWarn() {
	for event := range h.warnCh {
		log.Printf("[WARN] service=%s message=%q time=%s",
			event.Service, event.Message, event.Time.Format(time.RFC3339))
	}
}

func (h *consumerGroupHandler) handleError() {
	for event := range h.errorCh {
		log.Printf("[ERROR] service=%s message=%q time=%s",
			event.Service, event.Message, event.Time.Format(time.RFC3339))
	}
}
