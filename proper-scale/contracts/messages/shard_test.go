package messages

import (
	"strconv"
	"testing"
)

func TestRoomBucketIsStableAndInRange(t *testing.T) {
	first := RoomBucket("room-1")
	if first != RoomBucket("room-1") {
		t.Fatal("RoomBucket is not deterministic")
	}
	for _, room := range []string{"", "room-1", "room-2", "a much longer room identifier"} {
		if bucket := RoomBucket(room); bucket < 0 || bucket >= RoomBuckets {
			t.Fatalf("RoomBucket(%q) = %d, want within [0, %d)", room, bucket, RoomBuckets)
		}
	}
}

func TestRoomBucketSpreadsRooms(t *testing.T) {
	seen := make(map[int]struct{})
	for i := range 1000 {
		seen[RoomBucket("room-"+strconv.Itoa(i))] = struct{}{}
	}
	if len(seen) < RoomBuckets/2 {
		t.Fatalf("1000 rooms hit only %d buckets", len(seen))
	}
}
