package main

import (
	"messages/app/transport"
)

func main() {
	hub := transport.NewWsHub()

	transport.CreateHttpTransport(":8080", hub)
}
