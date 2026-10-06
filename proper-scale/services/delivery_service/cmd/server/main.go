package main

import (
	"context"
	"fmt"
	"log"
	"main/app"
	config "main/cmd"
	"main/infra/kafka"
	"main/infra/rabbitmq"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	consumer, err := createConsumer()

	if err != nil {
		log.Panicf("Cannot create consumer, err: %v", err)
	}

	if err := consumer.Run(ctx); err != nil {
		log.Fatalf("consumer stopped: %v", err)
	}
	log.Println("shut down cleanly")
}

func createConsumer() (*kafka.Consumer, error) {
	rabbitcfg := config.LoadRabbitMqConfig()
	publisher, err := rabbitmq.NewMessagePublisher(rabbitcfg)
	if err != nil {
		return nil, err
	}

	service, err := app.NewMessageHandler(publisher)
	if err != nil {
		return nil, fmt.Errorf("Cannot create message service: %v", err)
	}
	handler := kafka.NewHandler(service)
	consumer, err := kafka.NewConsumer(handler, config.LoadKafkaMessageConfig())
	if err != nil {
		return nil, fmt.Errorf("Cannot create consumer: %v", err)
	}

	return consumer, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
