package main

import (
	"log"
	"net"
	"presenceservice/app"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: "redis:6379", // host:port
		DB:   0,            // use default DB
	})

	grpcServer := grpc.NewServer()
	contractsv1.RegisterUserServiceServer(grpcServer, &app.GrpcConfig{
		Rdb: rdb,
	})

	// optional but handy for debugging with tools like grpcurl or Postman
	reflection.Register(grpcServer)

	log.Println("gRPC server listening on :9090")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
