package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL  string
	KafkaBrokers []string
	KafkaTopic   string
	PollInterval time.Duration
	BatchSize    int
	MaxRetries   int
	RetryBackoff time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		DatabaseURL: getEnv("DATABASE_URL", "postgres://chat:chat@localhost:5432/chat?sslmode=disable"),
		KafkaTopic:  getEnv("KAFKA_TOPIC", "messages"),
	}

	for _, broker := range strings.Split(getEnv("KAFKA_BROKERS", "localhost:29092"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
		}
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS must contain at least one broker")
	}
	if strings.TrimSpace(cfg.DatabaseURL) == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if strings.TrimSpace(cfg.KafkaTopic) == "" {
		return Config{}, errors.New("KAFKA_TOPIC is required")
	}
	var err error
	cfg.PollInterval, err = getDuration("POLL_INTERVAL", time.Second)
	if err != nil {
		return Config{}, err
	}
	if cfg.PollInterval > time.Minute {
		return Config{}, errors.New("POLL_INTERVAL must not exceed 1m")
	}
	cfg.RetryBackoff, err = getDuration("RETRY_BACKOFF", 200*time.Millisecond)
	if err != nil {
		return Config{}, err
	}
	if cfg.RetryBackoff > 5*time.Second {
		return Config{}, errors.New("RETRY_BACKOFF must not exceed 5s")
	}
	cfg.BatchSize, err = getInt("BATCH_SIZE", 100, 1, 1000)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxRetries, err = getInt("MAX_RETRIES", 3, 1, 10)
	if err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return duration, nil
}

func getInt(key string, fallback, min, max int) (int, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if parsed < min || parsed > max {
		return 0, fmt.Errorf("%s must be between %d and %d", key, min, max)
	}
	return parsed, nil
}
