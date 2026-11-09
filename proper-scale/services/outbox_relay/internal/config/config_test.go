package config

import (
	"os"
	"slices"
	"testing"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
)

func TestLoadAssignsDisjointBucketsPerShard(t *testing.T) {
	const shardCount = 3
	owned := make(map[int]int)
	for shardID := range shardCount {
		t.Setenv("SHARD_COUNT", "3")
		t.Setenv("SHARD_ID", string(rune('0'+shardID)))
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		for _, bucket := range cfg.Buckets {
			if previous, taken := owned[bucket]; taken {
				t.Fatalf("bucket %d owned by shard %d and %d", bucket, previous, shardID)
			}
			owned[bucket] = shardID
		}
	}
	if len(owned) != messagev1.RoomBuckets {
		t.Fatalf("shards own %d buckets, want %d", len(owned), messagev1.RoomBuckets)
	}
}

func TestLoadDefaultOwnsEveryBucket(t *testing.T) {
	t.Setenv("SHARD_COUNT", "")
	t.Setenv("SHARD_ID", "")
	if err := os.Unsetenv("SHARD_COUNT"); err != nil {
		t.Fatal(err)
	}
	if err := os.Unsetenv("SHARD_ID"); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Buckets) != messagev1.RoomBuckets || !slices.Contains(cfg.Buckets, 0) {
		t.Fatalf("default buckets = %d entries, want all %d", len(cfg.Buckets), messagev1.RoomBuckets)
	}
}

func TestLoadRejectsShardIDOutOfRange(t *testing.T) {
	t.Setenv("SHARD_COUNT", "3")
	t.Setenv("SHARD_ID", "3")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for SHARD_ID >= SHARD_COUNT")
	}
}
