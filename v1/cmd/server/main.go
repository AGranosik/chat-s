package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chat-s/internal/chat"
	"chat-s/internal/config"
	"chat-s/internal/hub"
	"chat-s/internal/outbox"
	"chat-s/internal/storage"
	"chat-s/internal/transport"
)

func main() {
	addr := config.GetEnv("HTTP_ADDR", ":8080")
	dsn := config.GetEnv("DATABASE_URL", "postgres://chat:chat@localhost:5432/chat?sslmode=disable")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Println("applying migrations")
	if err := storage.Migrate(ctx, dsn); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	pool, err := storage.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	store := storage.New(pool)

	h := hub.New()
	go h.Run(ctx)

	svc := chat.NewService(store)

	relay := outbox.NewRelay(store, h)
	go relay.Run(ctx)

	handler := transport.NewRouter(ctx, store, svc, h)
	srv := &http.Server{Addr: addr, Handler: handler}

	go func() {
		log.Printf("starting server | addr=%s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
