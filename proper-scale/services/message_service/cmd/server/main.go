package main

import (
	"log"
	transport "messages/app/transport"
	ws "messages/app/transport/ws"
	"messages/chat"

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
	chatService := chat.NewChatService()
	hub, error := ws.NewHub(chatService)
	if error != nil {
		log.Fatalf("hub creation failure: %v", error)
	}
	ws := ws.NewWsHub(client, hub, "chat-server-1:9090")

	transport.CreateHttpTransport(":8080", ws)
}
