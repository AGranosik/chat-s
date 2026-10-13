package app

import (
	"context"
	"log"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"github.com/redis/go-redis/v9"
)

const connTTL = 60 * time.Second

type GrpcConfig struct {
	contractsv1.UnimplementedUserServiceServer
	Rdb *redis.Client
}

func (s *GrpcConfig) Connect(ctx context.Context, req *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
	roomsId := req.GetRoomsId()
	clientId := req.GetClientId()
	instanceName := req.GetInstanceName()

	log.Printf("received message -> client_id=%s dial=%v", clientId, roomsId)

	pipe := s.Rdb.TxPipeline()
	for _, r := range roomsId {
		key := roomKey(r)
		pipe.HSet(ctx, key, clientId, instanceName)
		pipe.HExpire(ctx, key, connTTL, clientId)
	}

	clientKey := clientKey(clientId, instanceName)
	pipe.SAdd(ctx, clientKey, roomsId)
	pipe.Expire(ctx, clientKey, connTTL)
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

// TODO: cleanup
// TODO: code refactor, split into smaller
func (s *GrpcConfig) Disconnect(ctx context.Context, req *contractsv1.DisconnectUserRequest) (*contractsv1.ConnectionResponse, error) {
	clientId := req.GetClientId()
	instanceName := req.GetInstanceName()
	log.Printf("Disconnect client: client_id=%s", clientId)

	ck := clientKey(clientId, instanceName)
	roomsId, err := s.Rdb.SMembers(ctx, ck).Result()

	if err != nil {
		return &contractsv1.ConnectionResponse{
			Success: false,
		}, err
	}

	if len(roomsId) == 0 {
		return &contractsv1.ConnectionResponse{
			Success: true,
		}, nil
	}

	pipe := s.Rdb.TxPipeline()
	pipe.Del(ctx, ck)

	for _, r := range roomsId {
		rk := roomKey(r)
		pipe.HDel(ctx, rk, clientId)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("failed to remove client %s: %v", clientId, err)
		return &contractsv1.ConnectionResponse{Success: false}, err
	}

	return &contractsv1.ConnectionResponse{
		Success: true,
	}, nil
}

func (s *GrpcConfig) Ping(ctx context.Context, req *contractsv1.PingConnectionRequest) (*contractsv1.ConnectionResponse, error) {
	clientId := req.GetClientId()
	instanceName := req.GetInstanceName()
	ck := clientKey(clientId, instanceName)
	roomsId, err := s.Rdb.SMembers(ctx, ck).Result()

	if err != nil {
		return &contractsv1.ConnectionResponse{
			Success: false,
		}, err
	}

	if len(roomsId) == 0 {
		return &contractsv1.ConnectionResponse{
			Success: false,
		}, nil
	}

	pipe := s.Rdb.TxPipeline()
	for _, r := range roomsId {
		key := roomKey(r)
		pipe.HSet(ctx, key, clientId, instanceName)
		pipe.HExpire(ctx, key, connTTL, clientId)
	}

	pipe.Expire(ctx, ck, connTTL)
	_, err = pipe.Exec(ctx)

	if err != nil {
		return &contractsv1.ConnectionResponse{
			Success: false,
		}, err
	}

	return &contractsv1.ConnectionResponse{
		Success: true,
	}, nil

}

func clientKey(clientID, instanceName string) string {
	return "client:" + clientID + "-" + instanceName
}

func roomKey(roomID string) string {
	return "room:" + roomID
}
