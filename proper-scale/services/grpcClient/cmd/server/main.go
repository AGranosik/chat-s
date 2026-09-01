package main

import (
	"fmt"
	"time"

	"github.com/andeya/erpc/v7"
)

// Math handler group
type Math struct {
	erpc.CallCtx
}

// Add handles addition requests: /math/add
func (m *Math) Add(arg *[]int) (int, *erpc.Status) {
	// example of reading meta info sent by the client, if any
	erpc.Infof("author meta: %s", m.PeekMeta("author"))
	fmt.Println("received message.")
	var r int
	for _, a := range *arg {
		r += a
	}
	return r, nil
}

func main() {
	defer erpc.FlushLogger()
	go erpc.GraceSignal()

	// Create the server peer.
	srv := erpc.NewPeer(erpc.PeerConfig{
		CountTime:   true,
		ListenPort:  9090,
		PrintDetail: true,
	})

	// Optional: enable TLS
	// srv.SetTLSConfig(erpc.GenerateTLSConfigForServer())

	// Register the Math handler group -> exposes /math/add
	srv.RouteCall(new(Math))

	// Broadcast a push to all connected sessions every 5 seconds.
	go func() {
		for {
			time.Sleep(5 * time.Second)
			srv.RangeSession(func(sess erpc.Session) bool {
				sess.Push(
					"/push/status",
					fmt.Sprintf("this is a broadcast, server time: %v", time.Now()),
				)
				return true // continue ranging over remaining sessions
			})
		}
	}()

	// Start listening and serving. Blocks until shutdown.
	srv.ListenAndServe()
}
