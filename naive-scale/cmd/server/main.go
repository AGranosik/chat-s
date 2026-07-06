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

	"naive-scale/internal/chat"
	"naive-scale/internal/config"
	"naive-scale/internal/hub"
	"naive-scale/internal/outbox"
	"naive-scale/internal/storage"
	"naive-scale/internal/transport"
)

func main() {
	addr := config.GetEnv("HTTP_ADDR", ":8080")
	dsn := config.GetEnv("DATABASE_URL", "postgres://chat:chat@localhost:5432/chat?sslmode=disable")
	redisAddr := config.GetEnv("REDIS_ADDR", "localhost:6379")

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

	// Cross-instance fan-out via Redis pub/sub: each replica publishes every
	// broadcast to a room:<id> channel and subscribes to the rooms it hosts, so a
	// message now reaches clients on any instance, not just this one.
	h := hub.New(redisAddr)
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
