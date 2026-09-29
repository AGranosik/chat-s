package main

import (
	"context"
	"log"
	"main/infra/kafka"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := kafka.LoadConfig()
	log.Printf("starting consumer: brokers=%v group=%s topics=%v", cfg.Brokers, cfg.GroupID, cfg.Topics)

	if err := kafka.Run(ctx, cfg); err != nil {
		log.Fatalf("consumer stopped: %v", err)
	}
	log.Println("shut down cleanly")
}
