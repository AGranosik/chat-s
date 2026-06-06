package encoders

import (
	"encoding/json"
	"fmt"
)

type KafkaEncoder[T any] struct {
}

func NewEncoder[T any]() (*KafkaEncoder[T], error) {
	return &KafkaEncoder[T]{}, nil
}

func (e *KafkaEncoder[T]) Encode(v T) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to encode payload: %w", err)
	}
	return b, nil
}
