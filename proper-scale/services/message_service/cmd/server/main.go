package main

import (
	"log"
	"messages/app/transport"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("presence_service:9090", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.DefaultConfig,
	}))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := contractsv1.NewUserServiceClient(conn)
	hub := transport.NewWsHub(client)

	transport.CreateHttpTransport(":8080", hub)
}
