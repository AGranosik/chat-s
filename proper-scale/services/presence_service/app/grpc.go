package app

import (
	"context"
	"log"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"github.com/redis/go-redis/v9"
)

const connTTL = 60 * time.Second

type ConnectionService struct {
	contractsv1.UnimplementedUserServiceServer
	Rdb *redis.Client
}

func (s *ConnectionService) Connect(ctx context.Context, req *contractsv1.ConnectUserRequest) (*contractsv1.ConnectionResponse, error) {
	roomsId := req.GetRoomsId()
	clientId := req.GetClientId()
	instanceName := req.GetInstanceName()

	log.Printf("received message -> client_id=%s dial=%v", clientId, roomsId)

	pipe := s.Rdb.TxPipeline()
	saveRooms(ctx, clientId, instanceName, roomsId, pipe)
	saveClient(ctx, clientId, instanceName, roomsId, pipe)
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

// TODO: otbox with ordering and gaps
// TODO : e2e tests
// TODO: load tests
func (s *ConnectionService) Disconnect(ctx context.Context, req *contractsv1.DisconnectUserRequest) (*contractsv1.ConnectionResponse, error) {
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
		pipe.HDel(ctx, rk, ck)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("failed to remove client %s: %v", clientId, err)
		return &contractsv1.ConnectionResponse{Success: false}, err
	}

	return &contractsv1.ConnectionResponse{
		Success: true,
	}, nil
}

func (s *ConnectionService) Ping(ctx context.Context, req *contractsv1.PingConnectionRequest) (*contractsv1.ConnectionResponse, error) {
	clientId := req.GetClientId()
	instanceName := req.GetInstanceName()

	pipe := s.Rdb.TxPipeline()

	ck := clientKey(clientId, instanceName)
	roomsId, _ := s.Rdb.SMembers(ctx, ck).Result()

	saveRooms(ctx, clientId, instanceName, roomsId, pipe)

	pipe.Expire(ctx, ck, connTTL)

	_, err := pipe.Exec(ctx)

	log.Printf("Ping for: %s", clientId)
	if err != nil {
		return &contractsv1.ConnectionResponse{
			Success: false,
		}, err
	}

	return &contractsv1.ConnectionResponse{
		Success: true,
	}, nil

}

func (s *ConnectionService) IsConnected(ctx context.Context, req *contractsv1.ClientsConnectionDestRequest) (*contractsv1.UserConnectionResponse, error) {
	roomID := req.GetRoomId()
	rk := roomKey(roomID)

	fields, err := s.Rdb.HGetAll(ctx, rk).Result()
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	instanceNames := make([]string, 0)
	for _, inst := range fields {
		if _, ok := seen[inst]; ok {
			continue
		}
		seen[inst] = struct{}{}
		instanceNames = append(instanceNames, inst)
	}

	return &contractsv1.UserConnectionResponse{
		InstanceNames: instanceNames,
	}, nil
}

func saveRooms(ctx context.Context, clientID, instanceName string, roomsId []string, pipe redis.Pipeliner) {
	for _, r := range roomsId {
		key := roomKey(r)
		ck := clientKey(clientID, instanceName)
		pipe.HSet(ctx, key, ck, instanceName)
		pipe.HExpire(ctx, key, connTTL, ck)
	}
}

func saveClient(ctx context.Context, clientID, instanceName string, roomsId []string, pipe redis.Pipeliner) {
	ck := clientKey(clientID, instanceName)
	pipe.Del(ctx, ck)
	pipe.SAdd(ctx, ck, roomsId)
	pipe.Expire(ctx, ck, connTTL)
}

func clientKey(clientID, instanceName string) string {
	return clientID + "|" + instanceName
}

func roomKey(roomID string) string {
	return "room:" + roomID
}
