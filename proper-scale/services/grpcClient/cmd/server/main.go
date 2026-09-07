package main

import (
	"context"
	"log"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// what should be send via grpc?
// clientId, someid where to find that client...
// -- send it with some kind of conn string isnt safe because this may be on different machine
// via kafka?
// chat service publishes to kafka topic

// kafka topic -> relay service -> presence service
// relay service sends message to chat service via grpc

//store clients with ttl, remove method and 'refresh'

func main() {
	conn, err := grpc.NewClient("presence_service:9090", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.DefaultConfig,
	}))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := contractsv1.NewUserServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.Connect(ctx, &contractsv1.ConnectUserRequest{
		ClientId: "abc-123",
		Dial:     "some-value",
	})
	if err != nil {
		log.Fatalf("call failed: %v", err)
	}
	log.Printf("success=%v", resp.GetSuccess())
	time.Sleep(20 * time.Second)
}
