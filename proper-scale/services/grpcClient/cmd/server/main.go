package main

import (
	"fmt"
	"time"

	"github.com/andeya/erpc/v7"
)

// what should be send via grpc?
// clientId, someid where to find that client...
// -- send it with some kind of conn string isnt safe because this may be on different machine
// via kafka?
// chat service publishes to kafka topic

// kafka topic -> relay service -> presence service
// relay service sends message to chat service via grpc

//store clients with ttl, remove method and 'refresh'

type Push struct {
	erpc.PushCtx
}

// Status handles pushes to /push/status
func (p *Push) Status(arg *string) *erpc.Status {
	erpc.Infof("received push: %s", *arg)
	return nil
}

func main() {
	defer erpc.FlushLogger()
	go erpc.GraceSignal()

	// Create the client peer.
	cli := erpc.NewPeer(erpc.PeerConfig{
		CountTime:   true,
		PrintDetail: true,
	})

	// Register a handler for pushes coming FROM the server
	// (eRPC connections are peer-to-peer, so the client can receive
	// pushes just like the server can).
	cli.RoutePush(new(Push))

	// Dial the server.
	sess, stat := cli.Dial("grpc:9090")
	if !stat.OK() {
		erpc.Fatalf("dial error: %v", stat)
	}

	// Call the server's Math.Add handler.
	var result int
	callStat := sess.Call("/math/add", []int{1, 2, 3}, &result).Status()
	if !callStat.OK() {
		erpc.Fatalf("call error: %v", callStat)
	}
	fmt.Println("Add result:", result)

	// Keep the connection open a while to receive push broadcasts
	// from the server before exiting.
	time.Sleep(20 * time.Second)
}
