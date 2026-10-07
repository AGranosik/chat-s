package common

import (
	"os"
	"strings"
)

func GetEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func SplitEnv(key, def string) []string {
	return strings.Split(GetEnv(key, def), ",")
}
