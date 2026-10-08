package env

import "os"

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
