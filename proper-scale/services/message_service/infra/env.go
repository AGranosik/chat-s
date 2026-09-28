package env

import "os"

type Config struct {
	Dial            string
	PresenceService string
}

func GetEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func GetCfg() Config {
	return Config{
		Dial:            GetEnv("DIAL", "chat-server-1"),
		PresenceService: GetEnv("PresenceService", "presence_service:9090"),
	}
}
