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
	"messages/infra/kafka"
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

	rdb, err := newRedisClient(ctx)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer rdb.Close()

	publisher, err := kafka.NewPublisher([]string{env.GetEnv("KAFKA_BROKERS", "kafka:9092")})
	if err != nil {
		return fmt.Errorf("kafka publisher: %w", err)
	}

	ingest, err := service.NewIngestHandler(publisher)
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

	srv := transport.CreateHttpTransport(":8080", wsHandler)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Printf("starting consumer for instance %q", instance)
		return consumer.Run(ctx)
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

func newRedisClient(ctx context.Context) (*goredis.Client, error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr: env.GetEnv("REDIS_ADDR", "localhost:6379"),
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, err
	}
	return rdb, nil
}
