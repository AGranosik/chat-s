package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"outbox_relay/internal/config"
	"outbox_relay/internal/kafka"
	"outbox_relay/internal/postgres"
	"outbox_relay/internal/relay"
)

func main() {
	if err := run(); err != nil {
		slog.Error("outbox relay stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("outbox relay stopped cleanly")
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	repository, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer repository.Close()

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("close Kafka producer", "error", err)
		}
	}()

	worker, err := relay.New(repository, producer, logger, relay.Config{
		Topic:        cfg.KafkaTopic,
		BatchSize:    cfg.BatchSize,
		MaxRetries:   cfg.MaxRetries,
		RetryBackoff: cfg.RetryBackoff,
		PollInterval: cfg.PollInterval,
	})
	if err != nil {
		return err
	}

	logger.Info("outbox relay started", "topic", cfg.KafkaTopic, "brokers", cfg.KafkaBrokers, "batch_size", cfg.BatchSize)
	return worker.Run(ctx)
}
