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

// point of this service to learnd workinf with kafka and golang channels
func main() {
	const numberMessages = 1_000_000
	const chunk = 1_0000
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

	for i := 0; i < numberMessages; i += chunk {
		go func() {
			for range chunk {
				producer.Publish(message)
			}
		}()
	}

	<-ctx.Done()
}
