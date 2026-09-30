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
		log.Panicf(err.Error())
	}

	if err := consumer.Run(ctx); err != nil {
		log.Fatalf("consumer stopped: %v", err)
	}
	log.Println("shut down cleanly")
}

func createConsumer() (*kafka.Consumer, error) {
	//TODO: refactor
	// should i inject app layer to infra?
	service, err := app.NewMessageService()
	if err != nil {
		return nil, fmt.Errorf("Cannot create message service: %v", err)
	}
	handler := kafka.NewHandler(service.Process)
	consumer, err := kafka.NewConsumer(handler)
	if err != nil {
		return nil, fmt.Errorf("Cannot create consumer: %v", err)
	}

	return consumer, nil
}
