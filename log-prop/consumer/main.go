package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "app-logs")
	groupID := getEnv("KAFKA_GROUP_ID", "log-consumer-group")

	log.Printf("Starting consumer | brokers=%s topic=%s group=%s", brokers, topic, groupID)

	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true

	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), groupID, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	handler := &ConsumerGroupHandler{}

	// Consume in background
	go func() {
		for {
			if err := client.Consume(ctx, []string{topic}, handler); err != nil {
				log.Printf("Consume error: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Log consumer errors
	go func() {
		for err := range client.Errors() {
			log.Printf("Consumer error: %v", err)
		}
	}()

	log.Println("Consumer is running. Press Ctrl+C to stop.")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Shutting down...")
	cancel()
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler.
type ConsumerGroupHandler struct{}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session setup")
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session cleanup")
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf(
			"[partition=%d offset=%d key=%s] %s",
			msg.Partition,
			msg.Offset,
			string(msg.Key),
			string(msg.Value),
		)

		// Mark message as processed
		session.MarkMessage(msg, "")
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
