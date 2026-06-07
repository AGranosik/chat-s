package consumers

import (
	"context"
	"log"
	"main/configuration"
	"main/decoders"
	"main/models"
	"strings"

	"github.com/IBM/sarama"
)

type KafkaConsumer struct {
	client  sarama.ConsumerGroup
	decoder decoders.Decoder[models.LogEvent]
	topic   string
	groupID string
}

func CreateConsumer(topic, groupID string) (*KafkaConsumer, error) {
	brokers := configuration.GetEnv("KAFKA_BROKERS", "localhost:9092")

	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), groupID, config)
	if err != nil {
		return nil, err
	}

	decoder, err := decoders.NewDecoder[models.LogEvent]()
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		client:  client,
		decoder: decoder,
		topic:   topic,
		groupID: groupID,
	}, nil
}

func (c *KafkaConsumer) Start(ctx context.Context) {
	handler := &consumerGroupHandler{decoder: c.decoder}

	// Handle client-level errors
	go func() {
		for err := range c.client.Errors() {
			log.Printf("consumer error: %v", err)
		}
	}()

	// Consume in background, restarting on rebalance
	go func() {
		for {
			if err := c.client.Consume(ctx, []string{c.topic}, handler); err != nil {
				log.Printf("consume error: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
}

func (c *KafkaConsumer) Close() error {
	return c.client.Close()
}
