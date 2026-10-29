package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
	goredis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	transport "messages/app/transport"
	ws "messages/app/transport/ws"
	env "messages/infra"
	"messages/infra/postgres"
	"messages/infra/redis"
	"messages/service"
)

const (
	httpAddr           = ":8080"
	redisChannelPrefix = "instance:" // channelName() just concatenates, so keep the separator
	redisBufferSize    = 100
	shutdownTimeout    = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := env.GetCfg()

	conn, err := newPresenceConn(cfg.PresenceService)
	if err != nil {
		return fmt.Errorf("presence service connection: %w", err)
	}
	defer conn.Close()

	rdb := newRedisClient()
	defer rdb.Close()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("message database: %w", err)
	}
	defer pool.Close()
	if err := postgres.RunMigrations(ctx, pool, cfg.MigrationsDir); err != nil {
		return fmt.Errorf("message database migrations: %w", err)
	}

	store, err := postgres.NewStore(pool)
	if err != nil {
		return fmt.Errorf("message repository: %w", err)
	}
	ingest, err := service.NewIngestHandler(store)
	if err != nil {
		return fmt.Errorf("ingest handler: %w", err)
	}

	hub, err := ws.NewHub()
	if err != nil {
		return fmt.Errorf("hub: %w", err)
	}

	instance := env.GetEnv("INSTANCE_NAME", cfg.Dial)
	consumer, err := redis.NewConsumer(rdb, redis.ConsumerConfig{
		ChannelPrefix: redisChannelPrefix,
		InstanceName:  instance,
		BufferSize:    redisBufferSize,
	}, hub)
	if err != nil {
		return fmt.Errorf("redis consumer: %w", err)
	}

	wsHandler, err := ws.NewWs(contractsv1.NewUserServiceClient(conn), hub, cfg.Dial, ingest)
	if err != nil {
		return fmt.Errorf("ws handler: %w", err)
	}

	srv := transport.CreateHttpTransport(httpAddr, wsHandler, pool.Ping)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Printf("starting consumer for instance %q", instance)
		return runRedisConsumer(ctx, consumer)
	})

	g.Go(func() error {
		log.Printf("listening on %s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	g.Go(func() error {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})

	return g.Wait()
}

func newPresenceConn(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}),
	)
}

func newRedisClient() *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr: env.GetEnv("REDIS_ADDR", "localhost:6379"),
	})
}

func runRedisConsumer(ctx context.Context, consumer interface{ Run(context.Context) error }) error {
	delay := 250 * time.Millisecond
	for {
		err := consumer.Run(ctx)
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("redis consumer stopped: %v; retrying in %s", err, delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if delay < 5*time.Second {
			delay *= 2
			if delay > 5*time.Second {
				delay = 5 * time.Second
			}
		}
	}
}
