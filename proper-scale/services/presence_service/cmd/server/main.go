package main

import (
	"context"
	"log"
	"net"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// server implements the UserServiceServer interface generated from your .proto
type server struct {
	contractsv1.UnimplementedUserServiceServer
}

// GetUser is called whenever a client sends a GetUserRequest
func (s *server) ConnectUser(ctx context.Context, req *contractsv1.ConnectUserRequest) (*contractsv1.UserConnectionResponse, error) {
	log.Printf("received message -> client_id=%s dial=%s", req.GetClientId(), req.GetDial())

	// your actual logic goes here (lookup, validation, etc.)

	return &contractsv1.UserConnectionResponse{
		Success: true,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	contractsv1.RegisterUserServiceServer(grpcServer, &server{})

	// optional but handy for debugging with tools like grpcurl or Postman
	reflection.Register(grpcServer)

	log.Println("gRPC server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
