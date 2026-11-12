package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"outbox_relay/internal/kafka"
	"outbox_relay/internal/postgres"
)

type Repository interface {
	NextPending(context.Context, []int, int) ([]postgres.Entry, error)
	MarkPublished(context.Context, []string) error
}

type Config struct {
	Topic        string
	BatchSize    int
	MaxRetries   int
	RetryBackoff time.Duration
	PollInterval time.Duration
	Buckets      []int
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
	if config.Topic == "" || len(config.Buckets) == 0 || config.BatchSize < 1 || config.MaxRetries < 1 || config.RetryBackoff <= 0 || config.PollInterval <= 0 {
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
		processed, err := r.processBatch(ctx)
		if err != nil && ctx.Err() == nil {
			r.logger.Error("outbox relay cycle failed", "error", err)
		}
		if err == nil && processed >= r.config.BatchSize {
			continue
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

func (r *Relay) processBatch(ctx context.Context) (int, error) {
	entries, err := r.repository.NextPending(ctx, r.config.Buckets, r.config.BatchSize)
	if err != nil {
		return 0, err
	}
	if len(entries) == 0 {
		return 0, nil
	}

	published, batchErr := r.publishWithRetry(ctx, entries)

	if len(published) > 0 {
		ids := make([]string, len(published))
		for i, entry := range published {
			ids[i] = entry.MessageID
		}
		if err := r.repository.MarkPublished(ctx, ids); err != nil {
			r.logger.Error("outbox rows could not be marked published", "count", len(ids), "error", err)
			return len(entries), errors.Join(batchErr, err)
		}
	}

	r.logger.Info("published outbox batch", "fetched", len(entries), "published", len(published))
	return len(entries), batchErr
}

func (r *Relay) publishWithRetry(ctx context.Context, entries []postgres.Entry) ([]postgres.Entry, error) {
	published := make([]postgres.Entry, 0, len(entries))
	remaining := entries
	var lastErr error
	for attempt := 1; len(remaining) > 0; attempt++ {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		results := r.publisher.PublishBatch(ctx, r.config.Topic, toMessages(remaining))
		var succeeded []postgres.Entry
		succeeded, remaining, lastErr = splitResults(remaining, results)
		published = append(published, succeeded...)
		if len(remaining) == 0 {
			return published, nil
		}
		r.logger.Warn("outbox publish attempt failed", "attempt", attempt, "pending", len(remaining), "error", lastErr)
		if attempt == r.config.MaxRetries {
			break
		}
		delay := min(r.config.RetryBackoff*time.Duration(1<<(attempt-1)), 5*time.Second)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return published, ctx.Err()
		case <-timer.C:
		}
	}
	return published, fmt.Errorf("Kafka publish failed for %d outbox rows after %d attempts: %w", len(remaining), r.config.MaxRetries, lastErr)
}

func splitResults(entries []postgres.Entry, results []error) ([]postgres.Entry, []postgres.Entry, error) {
	succeeded := make([]postgres.Entry, 0, len(entries))
	var retry []postgres.Entry
	failedRooms := make(map[string]struct{})
	var firstErr error
	for i, entry := range entries {
		var err error
		if i < len(results) {
			err = results[i]
		} else {
			err = errors.New("publisher returned no result")
		}
		if err != nil {
			failedRooms[entry.RoomID] = struct{}{}
			retry = append(retry, entry)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, failed := failedRooms[entry.RoomID]; failed {
			retry = append(retry, entry)
			continue
		}
		succeeded = append(succeeded, entry)
	}
	return succeeded, retry, firstErr
}

func toMessages(entries []postgres.Entry) []kafka.Message {
	messages := make([]kafka.Message, len(entries))
	for i, entry := range entries {
		messages[i] = kafka.Message{Key: entry.RoomID, Value: entry.Event}
	}
	return messages
}
