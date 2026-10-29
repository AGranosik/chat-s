package env

import "os"

type Config struct {
	Dial            string
	PresenceService string
	DatabaseURL     string
	MigrationsDir   string
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
		DatabaseURL:     GetEnv("DATABASE_URL", "postgres://chat:chat@postgres:5432/chat?sslmode=disable"),
		MigrationsDir:   GetEnv("MIGRATIONS_DIR", "migrations"),
	}
}
