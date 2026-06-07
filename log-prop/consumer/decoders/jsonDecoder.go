package decoders

import (
	"encoding/json"
	"fmt"
)

type JSONDecoder[T any] struct{}

func NewDecoder[T any]() (*JSONDecoder[T], error) {
	return &JSONDecoder[T]{}, nil
}

func (d *JSONDecoder[T]) Decode(data []byte) (T, error) {
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return v, fmt.Errorf("failed to decode payload: %w", err)
	}
	return v, nil
}
