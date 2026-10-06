package kafka

import (
	"context"
	"log"
	"main/app"

	"github.com/IBM/sarama"
)

type MessageHandler interface {
	Handle(ctx context.Context, m app.Message) error
}

type Handler struct {
	handler MessageHandler
}

func NewHandler(h MessageHandler) *Handler {
	return &Handler{
		handler: h,
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
			m := app.Message{Key: msg.Key, Value: msg.Value, Timestamp: msg.Timestamp}
			if err := h.handler.Handle(session.Context(), m); err != nil {
				log.Fatalf("error during consuming: %v", err)
				continue
			}
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
