package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// addr := config.GetEnv("HTTP_ADDR", ":8080")
	// dsn := config.GetEnv("DATABASE_URL", "postgres://chat:chat@localhost:5432/chat?sslmode=disable")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	done := make(chan struct{})
	go runWorker(ctx, done)

	<-ctx.Done()
	<-done
	log.Println("service stopped cleanly")
}

func runWorker(ctx context.Context, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("worker: draining and exiting")
			return
		case <-ticker.C:
			doWork()
		}
	}
}

func doWork() {
	log.Println("doing background work...")
}
