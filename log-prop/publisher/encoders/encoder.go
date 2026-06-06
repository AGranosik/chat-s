package encoders

type Encoder[T any] interface {
	Encode(v T) ([]byte, error)
}
