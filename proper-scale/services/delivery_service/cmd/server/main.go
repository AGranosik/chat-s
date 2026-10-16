package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"main/app"
	env "main/infra"
	"main/infra/kafka"
	"main/infra/redispubsub"
	"os"
	"os/signal"
	"syscall"

	contractsv1 "github.com/AGranosik/chat/contracts/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// No defers here, so log.Fatalf is safe: run() has already cleaned up.
	if err := run(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
	log.Println("shut down cleanly")
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	presenceConn, err := dialGRPC(env.GetGrpcConfig().PresenceService)
	if err != nil {
		return fmt.Errorf("presence grpc client: %w", err)
	}
	defer presenceConn.Close()

	pub, err := redispubsub.NewMessagePublisher(ctx, env.GetRedisConfig())
	if err != nil {
		return fmt.Errorf("redis publisher: %w", err)
	}
	defer pub.Close()

	service, err := app.NewMessageHandler(pub, contractsv1.NewUserServiceClient(presenceConn))
	if err != nil {
		return fmt.Errorf("message handler: %w", err)
	}

	consumer, err := kafka.NewConsumer(kafka.NewHandler(service), env.GetKafkaMessageConfig())
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}

	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("kafka consumer stopped: %w", err)
	}
	return nil
}

func dialGRPC(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
