package main

import (
	"log"
	transport "messages/app/transport"
	ws "messages/app/transport/ws"
	"messages/chat"
	env "messages/infra"
	"messages/infra/kafka"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	cfg := env.GetCfg()
	conn, err := grpc.NewClient(cfg.PresenceService, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithConnectParams(grpc.ConnectParams{
		Backoff: backoff.DefaultConfig,
	}))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	producer, err := kafka.NewProducer([]string{"kafka:9092"})
	if err != nil {
		log.Fatalf("cannot construct kafka: %v", err)
	}

	publisher := chat.NewPublisher(producer)
	client := contractsv1.NewUserServiceClient(conn)
	chatService := chat.NewChatService(publisher)
	hub, error := ws.NewHub(chatService)
	if error != nil {
		log.Fatalf("hub creation failure: %v", error)
	}
	ws, err := ws.NewWs(client, hub, cfg.Dial)

	if err != nil {
		log.Fatalf("Ws connection not established")
	}

	transport.CreateHttpTransport(":8080", ws)
}
