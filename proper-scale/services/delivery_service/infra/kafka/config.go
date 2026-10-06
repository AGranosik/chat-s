package kafka

import (
	"time"

	"github.com/IBM/sarama"
)

type Config struct {
	Brokers []string
	GroupID string
	Topics  []string
}

func (c *Config) newSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0 // set to your broker version (or lower)

	// Consumer group
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategySticky(),
	}
	cfg.Consumer.Group.Session.Timeout = 30 * time.Second
	cfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	cfg.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = time.Second

	// Fetching
	cfg.Consumer.Fetch.Min = 1024
	cfg.Consumer.Fetch.Default = 1 << 20
	cfg.Consumer.MaxWaitTime = 500 * time.Millisecond
	cfg.Consumer.MaxProcessingTime = 100 * time.Millisecond // raise if handlers are slow
	cfg.Consumer.IsolationLevel = sarama.ReadCommitted
	cfg.Consumer.Return.Errors = true

	return cfg
}
