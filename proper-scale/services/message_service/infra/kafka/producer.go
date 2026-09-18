package kafka

import (
	"github.com/IBM/sarama"
)

//TODO: asyncproducer

func NewProducer(brokers []string) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll // or WaitForLocal for lower latency
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true // dedupes on broker-side retries
	cfg.Net.MaxOpenRequests = 1    // required when Idempotent is true

	return sarama.NewSyncProducer(brokers, cfg)
}
