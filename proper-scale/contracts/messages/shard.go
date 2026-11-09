package messages

import "hash/fnv"

const RoomBuckets = 256

func RoomBucket(roomID string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(roomID))
	return int(hash.Sum32() % RoomBuckets)
}
