package main

import (
	"context"
	"kafka-consumer/configuration"
	"kafka-consumer/consumers"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	topic := configuration.GetEnv("KAFKA_TOPIC", "app-logs")
	groupID := configuration.GetEnv("KAFKA_GROUP_ID", "log-consumer-group")

	log.Printf("starting consumer | topic=%s group=%s", topic, groupID)

	consumer, err := consumers.CreateConsumer(topic, groupID)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	consumer.Start(ctx)

	log.Println("consumer is running, press Ctrl+C to stop")
	<-ctx.Done()
	log.Println("shutting down...")
}
