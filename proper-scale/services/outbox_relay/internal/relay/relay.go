package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
	"outbox_relay/internal/kafka"
	"outbox_relay/internal/postgres"
)

type Repository interface {
	NextPending(context.Context, int) ([]postgres.Entry, error)
	MarkPublished(context.Context, string) error
}

type Config struct {
	Topic        string
	BatchSize    int
	MaxRetries   int
	RetryBackoff time.Duration
	PollInterval time.Duration
}

type Relay struct {
	repository Repository
	publisher  kafka.Publisher
	logger     *slog.Logger
	config     Config
}

func New(
	repository Repository,
	publisher kafka.Publisher,
	logger *slog.Logger,
	config Config,
) (*Relay, error) {
	if repository == nil {
		return nil, errors.New("outbox repository is required")
	}
	if publisher == nil {
		return nil, errors.New("Kafka publisher is required")
	}
	if logger == nil {
		return nil, errors.New("logger is required")
	}
	if config.Topic == "" || config.BatchSize < 1 || config.MaxRetries < 1 || config.RetryBackoff <= 0 || config.PollInterval <= 0 {
		return nil, errors.New("relay configuration is invalid")
	}
	return &Relay{
		repository: repository,
		publisher:  publisher,
		logger:     logger,
		config:     config,
	}, nil
}

func (r *Relay) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if err := r.processBatch(ctx); err != nil && ctx.Err() == nil {
			r.logger.Error("outbox relay cycle failed", "error", err)
		}

		timer := time.NewTimer(r.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (r *Relay) processBatch(ctx context.Context) error {
	entries, err := r.repository.NextPending(ctx, r.config.BatchSize)
	if err != nil {
		return err
	}
	var batchErr error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validateEntry(entry); err != nil {
			r.logger.Error("invalid outbox event", "message_id", entry.MessageID, "room_id", entry.RoomID, "room_seq", entry.RoomSeq, "error", err)
			batchErr = errors.Join(batchErr, err)
			continue
		}
		partition, offset, err := r.publishWithRetry(ctx, entry)
		if err != nil {
			r.logger.Error("outbox publish failed", "message_id", entry.MessageID, "room_id", entry.RoomID, "room_seq", entry.RoomSeq, "error", err)
			batchErr = errors.Join(batchErr, err)
			continue
		}
		if err := r.repository.MarkPublished(ctx, entry.MessageID); err != nil {
			r.logger.Error("outbox row could not be marked published", "message_id", entry.MessageID, "room_id", entry.RoomID, "room_seq", entry.RoomSeq, "error", err)
			batchErr = errors.Join(batchErr, err)
			continue
		}
		r.logger.Info("published outbox event", "message_id", entry.MessageID, "room_id", entry.RoomID, "room_seq", entry.RoomSeq, "partition", partition, "offset", offset)
	}
	return batchErr
}

func (r *Relay) publishWithRetry(ctx context.Context, entry postgres.Entry) (int32, int64, error) {
	var lastErr error
	for attempt := 1; attempt <= r.config.MaxRetries; attempt++ {
		partition, offset, err := r.publisher.Publish(ctx, r.config.Topic, entry.RoomID, entry.Event)
		if err == nil {
			return partition, offset, nil
		}
		lastErr = err
		if attempt == r.config.MaxRetries {
			break
		}
		delay := r.config.RetryBackoff * time.Duration(1<<(attempt-1))
		if delay > 5*time.Second {
			delay = 5 * time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, 0, ctx.Err()
		case <-timer.C:
		}
	}
	return 0, 0, fmt.Errorf("Kafka publish failed after %d attempts: %w", r.config.MaxRetries, lastErr)
}

func validateEntry(entry postgres.Entry) error {
	var event messagev1.Event
	if err := json.Unmarshal(entry.Event, &event); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate event: %w", err)
	}
	if event.MessageID != entry.MessageID || event.RoomID != entry.RoomID || event.RoomSeq != entry.RoomSeq {
		return errors.New("event fields do not match outbox row")
	}
	return nil
}
