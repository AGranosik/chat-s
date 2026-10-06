package config

import (
	"main/infra/kafka"
	"main/infra/rabbitmq"

	"github.com/AGranosik/chat/contracts/common"
)

func LoadKafkaMessageConfig() kafka.Config {
	return kafka.Config{
		Brokers: common.SplitEnv("KAFKA_BROKERS", "localhost:29092"),
		GroupID: common.GetEnv("KAFKA_GROUP_ID", "delivery"),
		Topics:  common.SplitEnv("KAFKA_TOPICS", "messages"),
	}
}

func LoadRabbitMqConfig() rabbitmq.Config {
	dial := common.GetEnv("RABBITMQ_URI", "amqp://guest:guest@localhost:5672/")
	return rabbitmq.Config{
		Dial:      dial,
		Exchange:  "messages",
		Kind:      "direct",
		Mandatory: false,
	}
}
