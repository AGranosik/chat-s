package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"relayservice/app/config"
	"relayservice/app/services"
	"relayservice/app/storage"
	"syscall"
)

// drain and migratiosn doesnt work
func main() {
	// addr := config.GetEnv("HTTP_ADDR", ":8080")
	db := config.GetEnv("DATABASE_URL", "postgres://chat:chat@localhost:5432/chat?sslmode=disable")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	done := make(chan struct{})
	err := storage.Migrate(ctx, db)
	if err != nil {
		log.Panicln("Cannot migrate.")
	}
	pool, err := storage.Connect(ctx, db)

	if err != nil {
		log.Panicln("Cannot connect to db.")
	}

	store := storage.New(pool)
	relay := services.New(store)

	go relay.Run(ctx)
	<-ctx.Done()
	<-done
	log.Println("service stopped cleanly")
}
