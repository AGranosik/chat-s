package producers

import (
	"log"
	"main/configuration"
	"main/encoders"
	"main/models"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	encoder   encoders.Encoder[models.LogEvent]
	publisher sarama.AsyncProducer
	topic     string
}

func CreateProducer(topic string) (*KafkaProducer, error) {
	brokers := configuration.GetEnv("KAFKA_BROKERS", "localhost:9092")

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

	encoder, err := encoders.NewEncoder[models.LogEvent]()

	if err != nil {
		return nil, err
	}
	return &KafkaProducer{
		encoder:   encoder,
		publisher: producer,
		topic:     topic,
	}, nil
}

func (p *KafkaProducer) Publish(log models.LogEvent) error {
	encodedMessage, err := p.encoder.Encode(log)
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
