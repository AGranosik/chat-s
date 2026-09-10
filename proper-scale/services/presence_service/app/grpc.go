package app

import (
	"context"
	"log"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"github.com/redis/go-redis/v9"
)

type GrpcConfig struct {
	contractsv1.UnimplementedUserServiceServer
	Rdb *redis.Client
}

func (s *GrpcConfig) Connect(ctx context.Context, req *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
	log.Printf("received message -> client_id=%s dial=%s", req.GetClientId(), req.GetDial())

	clientId := req.ClientId
	dial := req.GetDial()

	if err := s.Rdb.Set(ctx, clientId, dial, time.Second*10).Err(); err != nil {
		log.Printf("failed to set client %s: %v", clientId, err)
		return &contractsv1.ConnectionResponse{
			Success: false,
		}, err
	}

	return &contractsv1.ConnectionResponse{
		Success: true,
	}, nil
}

func (s *GrpcConfig) Disconnect(ctx context.Context, req *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
	clientId := req.GetClientId()
	log.Printf("Disconnect client: client_id=%s", clientId)

	if err := s.Rdb.Del(ctx, clientId).Err(); err != nil {
		log.Printf("failed to remove client %s: %v", clientId, err)
		return &contractsv1.ConnectionResponse{
			Success: false,
		}, err
	}
	return &contractsv1.ConnectionResponse{
		Success: true,
	}, nil
}
