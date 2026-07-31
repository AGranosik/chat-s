package producers

import (
	"log"
	"relayservice/app/config"
	"relayservice/app/publisher/encoders"
	"relayservice/app/storage"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	encoder   encoders.Encoder[storage.OutboxEvent]
	publisher sarama.AsyncProducer
	topic     string
}

func CreateProducer(topic string) (*KafkaProducer, error) {
	brokers := config.GetEnv("KAFKA_BROKERS", "localhost:9092")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Errors = true
	config.Producer.Flush.Messages = 100                     // flush every 100 messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // or every 500ms

	producer, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), config)

	if err != nil {
		return nil, err
	}
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

	encoder, err := encoders.NewEncoder[storage.OutboxEvent]()

	if err != nil {
		return nil, err
	}
	return &KafkaProducer{
		encoder:   encoder,
		publisher: producer,
		topic:     topic,
	}, nil
}

func (p *KafkaProducer) Publish(event storage.OutboxEvent) error {
	encodedMessage, err := p.encoder.Encode(event)
	if err != nil {
		return err
	}
	p.publisher.Input() <- &sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.StringEncoder(encodedMessage),
	}

	return nil
}

func (p *KafkaProducer) AsyncClose() {
	p.publisher.AsyncClose()
}
