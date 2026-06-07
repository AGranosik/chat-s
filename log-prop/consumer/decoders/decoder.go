package decoders

type Decoder[T any] interface {
	Decode(data []byte) (T, error)
}
