package consumers

import (
	"kafka-consumer/decoders"
	"kafka-consumer/models"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type infoJob struct {
	event   models.LogEvent
	msg     *sarama.ConsumerMessage
	session sarama.ConsumerGroupSession
}

type warnJob struct {
	event   models.LogEvent
	msg     *sarama.ConsumerMessage
	session sarama.ConsumerGroupSession
}

type errorJob struct {
	event   models.LogEvent
	msg     *sarama.ConsumerMessage
	session sarama.ConsumerGroupSession
}

type consumerGroupHandler struct {
	decoder decoders.Decoder[models.LogEvent]
	infoCh  chan infoJob
	warnCh  chan warnJob
	errorCh chan errorJob
}

func newConsumerGroupHandler(decoder decoders.Decoder[models.LogEvent]) *consumerGroupHandler {
	h := &consumerGroupHandler{
		decoder: decoder,
		infoCh:  make(chan infoJob, 100),
		warnCh:  make(chan warnJob, 100),
		errorCh: make(chan errorJob, 100),
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
		case "information":
			// is full for some reason
			select {
			case h.infoCh <- infoJob{event: event, msg: msg, session: session}:
			default:
				log.Printf("[WARN] infoCh full, dropping INFO at offset=%d", msg.Offset)
				session.MarkMessage(msg, "")
			}
		case "WARN":
			select {
			case h.warnCh <- warnJob{event: event, msg: msg, session: session}:
			default:
				log.Printf("[WARN] warnCh full, dropping WARN at offset=%d", msg.Offset)
				session.MarkMessage(msg, "")
			}
		case "ERROR":
			select {
			case h.errorCh <- errorJob{event: event, msg: msg, session: session}:
			default:
				log.Printf("[WARN] errorCh full, dropping ERROR at offset=%d", msg.Offset)
				session.MarkMessage(msg, "")
			}
		default:
			log.Fatalf("Not existing log type.")
			session.MarkMessage(msg, "")
		}
	}
	return nil
}

func (h *consumerGroupHandler) handleInfo() {
	for job := range h.infoCh {
		if job.session.Context().Err() != nil {
			log.Printf("[INFO] session expired, message at offset=%d will be redelivered", job.msg.Offset)
			continue
		}

		log.Printf("hehe")

		log.Printf("[INFO] partition=%d offset=%d service=%s message=%q time=%s",
			job.msg.Partition,
			job.msg.Offset,
			job.event.Service,
			job.event.Message,
			job.event.Time.Format(time.RFC3339),
		)

		job.session.MarkMessage(job.msg, "")
	}
}

func (h *consumerGroupHandler) handleWarn() {
	for job := range h.warnCh {
		if job.session.Context().Err() != nil {
			log.Printf("[WARN] session expired, message at offset=%d will be redelivered", job.msg.Offset)
			continue
		}

		log.Printf("[WARN] partition=%d offset=%d service=%s message=%q time=%s",
			job.msg.Partition,
			job.msg.Offset,
			job.event.Service,
			job.event.Message,
			job.event.Time.Format(time.RFC3339),
		)

		job.session.MarkMessage(job.msg, "")
	}
}

func (h *consumerGroupHandler) handleError() {
	for job := range h.errorCh {
		if job.session.Context().Err() != nil {
			log.Printf("[ERROR] session expired, message at offset=%d will be redelivered", job.msg.Offset)
			continue
		}

		log.Printf("[ERROR] partition=%d offset=%d service=%s message=%q time=%s",
			job.msg.Partition,
			job.msg.Offset,
			job.event.Service,
			job.event.Message,
			job.event.Time.Format(time.RFC3339),
		)

		job.session.MarkMessage(job.msg, "")
	}
}

func (h *consumerGroupHandler) close() {
	close(h.infoCh)
	close(h.warnCh)
	close(h.errorCh)
}
