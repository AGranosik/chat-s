package config

import "os"

// GetEnv returns the value of key, or fallback when the variable is unset or
// empty. Never panics on a missing optional var (same idiom as ../log-prop).
func GetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
