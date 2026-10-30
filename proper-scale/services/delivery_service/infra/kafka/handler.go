package kafka

import (
	"context"
	"fmt"
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
			m := app.Message{
				Key:       msg.Key,
				Value:     msg.Value,
				Headers:   kafkaHeaders(msg.Headers),
				Timestamp: msg.Timestamp,
			}
			if err := h.handler.Handle(session.Context(), m); err != nil {
				return fmt.Errorf("handle Kafka message %s[%d] at offset %d: %w", msg.Topic, msg.Partition, msg.Offset, err)
			}
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func kafkaHeaders(headers []*sarama.RecordHeader) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	values := make(map[string]string, len(headers))
	for _, header := range headers {
		if header != nil {
			values[string(header.Key)] = string(header.Value)
		}
	}
	return values
}
