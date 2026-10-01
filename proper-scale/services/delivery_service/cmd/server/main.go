package main

import (
	"context"
	"fmt"
	"log"
	"main/app"
	"main/infra/kafka"
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
	service, err := app.NewMessageHandler()
	if err != nil {
		return nil, fmt.Errorf("Cannot create message service: %v", err)
	}
	handler := kafka.NewHandler(service)
	consumer, err := kafka.NewConsumer(handler)
	if err != nil {
		return nil, fmt.Errorf("Cannot create consumer: %v", err)
	}

	return consumer, nil
}
