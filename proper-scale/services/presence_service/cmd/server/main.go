package main

import (
	"fmt"
	"time"

	"github.com/andeya/erpc/v7"
)

// Push handles server-pushed messages, e.g. the "/push/status" broadcast
// sent by the server every 5 seconds.
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
