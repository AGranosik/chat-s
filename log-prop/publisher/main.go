package main

import (
	"log"
	"main/encoders"
	"main/models"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "app-logs")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Errors = true
	config.Producer.Flush.Messages = 100                     // flush every 100 messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // or every 500ms

	producer, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.AsyncClose()

	done := make(chan struct{})
	// Handle acks and errors in background
	go func() {
		for range producer.Successes() {
			log.Printf("success.")
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.Printf("Failed to send: %v", err)
		}
	}()

	encoder, err := encoders.NewEncoder[models.LogEvent]()

	if err != nil {
		log.Fatalf("Failed to create encoder")
	}

	message, err := encoder.Encode(models.LogEvent{
		Level:   "information",
		Message: "some-info",
		Service: "producer",
		Time:    time.Now(),
	})

	if err != nil {
		log.Fatal("Faile to encode mesage")
	}

	// Send messages — non-blocking
	producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	<-done
}
