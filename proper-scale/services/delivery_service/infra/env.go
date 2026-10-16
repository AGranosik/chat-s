package env

import (
	"main/infra/kafka"
	"main/infra/redispubsub"
	"os"

	"github.com/AGranosik/chat/contracts/common"
)

type Config struct {
	PresenceService string
}

func GetEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func GetGrpcConfig() Config {
	return Config{
		PresenceService: GetEnv("PresenceService", "presence_service:9090"),
	}
}

func GetKafkaMessageConfig() kafka.Config {
	return kafka.Config{
		Brokers: common.SplitEnv("KAFKA_BROKERS", "localhost:29092"),
		GroupID: common.GetEnv("KAFKA_GROUP_ID", "delivery"),
		Topics:  common.SplitEnv("KAFKA_TOPICS", "messages"),
	}
}

func GetRedisConfig() redispubsub.Config {
	return redispubsub.Config{
		Addr:          os.Getenv("REDIS_ADDR"),
		ChannelPrefix: "instance:",
	}
}
