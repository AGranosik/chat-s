package main

import (
	"context"
	"log"
	"main/configuration"
	"main/models"
	"main/producers"
	"os"
	"os/signal"
	"time"
)

func main() {
	topic := configuration.GetEnv("KAFKA_TOPIC", "app-logs")

	producer, err := producers.CreateProducer(topic)

	if err != nil {
		log.Fatalf("cannot create producer %v", err)
	}

	defer producer.AsyncClose()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	message := models.LogEvent{
		Level:   "information",
		Message: "some-info",
		Service: "producer",
		Time:    time.Now(),
	}

	producer.Publish(message)

	<-ctx.Done()
}
