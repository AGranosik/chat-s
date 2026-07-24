package config

import "testing"

func TestGetEnv(t *testing.T) {
	const key = "NAIVE_SCALE_TEST_VAR"

	t.Run("returns value when set", func(t *testing.T) {
		t.Setenv(key, "value")
		if got := GetEnv(key, "fallback"); got != "value" {
			t.Errorf("GetEnv = %q, want %q", got, "value")
		}
	})

	t.Run("returns fallback when unset", func(t *testing.T) {
		if got := GetEnv("NAIVE_SCALE_DEFINITELY_UNSET", "fallback"); got != "fallback" {
			t.Errorf("GetEnv = %q, want %q", got, "fallback")
		}
	})

	t.Run("returns fallback when empty", func(t *testing.T) {
		t.Setenv(key, "")
		if got := GetEnv(key, "fallback"); got != "fallback" {
			t.Errorf("GetEnv = %q, want %q", got, "fallback")
		}
	})
}
