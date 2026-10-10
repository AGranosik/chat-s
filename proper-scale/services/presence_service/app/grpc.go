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
	roomsId := req.GetRoomsId()
	clientId := req.GetClientId()
	instanceName := req.GetInstanceName()

	log.Printf("received message -> client_id=%s dial=%v", clientId, roomsId)

	pipe := s.Rdb.Pipeline()
	for _, r := range roomsId {
		pipe.HSet(ctx, "room:"+r, clientId, instanceName)
	}
	pipe.SAdd(ctx, clientKey(clientId), time.Now())

	_, err := pipe.Exec(ctx)

	if err != nil {
		return &contractsv1.ConnectionResponse{
			Success: false,
		}, err
	}

	return &contractsv1.ConnectionResponse{
		Success: true,
	}, nil
}

// TODO: add pipe
// TODO: ADD ROOMS ID TO DISCONNECT
// TODO: ping
// TODO: cleanup
func (s *GrpcConfig) Disconnect(ctx context.Context, req *contractsv1.DisconnectUserRequest) (*contractsv1.ConnectionResponse, error) {
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

func clientKey(clientID string) string {
	return "client:" + clientID
}
