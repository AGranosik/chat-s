package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// addr := config.GetEnv("HTTP_ADDR", ":8080")
	// dsn := config.GetEnv("DATABASE_URL", "postgres://chat:chat@localhost:5432/chat?sslmode=disable")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	<-ctx.Done()
}
