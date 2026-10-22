package app

import (
	"context"
	"errors"
	"io"
	"log"
	"maps"
	"slices"
	"testing"
	"time"

	contractsv1 "github.com/AGranosik/chat/contracts"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestService(t *testing.T) (*ConnectionService, *miniredis.Miniredis, *redis.Client) {
	t.Helper()

	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })

	mr := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })

	return &ConnectionService{Rdb: rdb}, mr, rdb
}

type failExec struct{ err error }

func (failExec) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (failExec) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h failExec) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if cmd.Name() == "exec" {
				return h.err
			}
		}
		return next(ctx, cmds)
	}
}

func connect(t *testing.T, svc *ConnectionService, clientID, instance string, rooms ...string) {
	t.Helper()
	resp, err := svc.Connect(context.Background(), &contractsv1.ConnectUserRequest{
		ClientId:     clientID,
		InstanceName: instance,
		RoomsId:      rooms,
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("setup Connect(%s,%s,%v) failed: resp=%v err=%v", clientID, instance, rooms, resp, err)
	}
}

func clientRooms(t *testing.T, rdb *redis.Client, clientID, instance string) []string {
	t.Helper()
	rooms, err := rdb.SMembers(context.Background(), clientKey(clientID, instance)).Result()
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	slices.Sort(rooms)
	return rooms
}

func roomFields(t *testing.T, rdb *redis.Client, roomID string) map[string]string {
	t.Helper()
	fields, err := rdb.HGetAll(context.Background(), roomKey(roomID)).Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	return fields
}

func fieldTTL(t *testing.T, rdb *redis.Client, roomID, field string) int64 {
	t.Helper()
	ttls, err := rdb.HTTL(context.Background(), roomKey(roomID), field).Result()
	if err != nil || len(ttls) != 1 {
		t.Fatalf("HTTL: ttls=%v err=%v", ttls, err)
	}
	return ttls[0]
}

func sorted(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}

func TestKeyHelpers(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"clientKey joins client and instance", clientKey("c1", "inst-a"), "c1|inst-a"},
		{"clientKey with empty parts", clientKey("", ""), "|"},
		{"roomKey prefixes room id", roomKey("42"), "room:42"},
		{"roomKey with empty id", roomKey(""), "room:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("got %q, want %q", tc.got, tc.want)
			}
		})
	}
}

func TestConnect_StoresClientRoomsAndRoomMembership(t *testing.T) {
	svc, mr, rdb := newTestService(t)

	resp, err := svc.Connect(context.Background(), &contractsv1.ConnectUserRequest{
		ClientId:     "c1",
		InstanceName: "inst-a",
		RoomsId:      []string{"r1", "r2"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatal("expected Success=true")
	}

	if got, want := clientRooms(t, rdb, "c1", "inst-a"), []string{"r1", "r2"}; !slices.Equal(got, want) {
		t.Fatalf("client rooms = %v, want %v", got, want)
	}
	for _, room := range []string{"r1", "r2"} {
		want := map[string]string{"c1|inst-a": "inst-a"}
		if got := roomFields(t, rdb, room); !maps.Equal(got, want) {
			t.Fatalf("room %s fields = %v, want %v", room, got, want)
		}
	}
	if !mr.Exists("room:r1") || !mr.Exists("room:r2") {
		t.Fatal("expected room hashes to exist")
	}
}

func TestConnect_SetsTTLOnClientKeyAndRoomFields(t *testing.T) {
	svc, mr, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")

	if got := mr.TTL("c1|inst-a"); got != connTTL {
		t.Fatalf("client key TTL = %v, want %v", got, connTTL)
	}

	if got := mr.TTL("room:r1"); got != 0 {
		t.Fatalf("room hash TTL = %v, want none (per-field TTL only)", got)
	}
	if got := fieldTTL(t, rdb, "r1", "c1|inst-a"); got <= 0 || got > int64(connTTL/time.Second) {
		t.Fatalf("room field TTL = %ds, want within (0, %d]", got, int64(connTTL/time.Second))
	}
}

func TestConnect_ExpiresPresenceAfterTTL(t *testing.T) {
	svc, mr, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")

	mr.FastForward(connTTL + time.Second)

	if mr.Exists("c1|inst-a") {
		t.Fatal("client key should have expired")
	}
	if got := roomFields(t, rdb, "r1"); len(got) != 0 {
		t.Fatalf("room field should have expired, got %v", got)
	}
}

func TestConnect_MultipleClientsShareRoom(t *testing.T) {
	svc, _, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")
	connect(t, svc, "c2", "inst-b", "r1")

	want := map[string]string{"c1|inst-a": "inst-a", "c2|inst-b": "inst-b"}
	if got := roomFields(t, rdb, "r1"); !maps.Equal(got, want) {
		t.Fatalf("room fields = %v, want %v", got, want)
	}
}

func TestConnect_SameClientOnDifferentInstancesIsIndependent(t *testing.T) {
	svc, _, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")
	connect(t, svc, "c1", "inst-b", "r2")

	if got, want := clientRooms(t, rdb, "c1", "inst-a"), []string{"r1"}; !slices.Equal(got, want) {
		t.Fatalf("inst-a rooms = %v, want %v", got, want)
	}
	if got, want := clientRooms(t, rdb, "c1", "inst-b"), []string{"r2"}; !slices.Equal(got, want) {
		t.Fatalf("inst-b rooms = %v, want %v", got, want)
	}
}

func TestConnect_ReconnectReplacesClientRoomSet(t *testing.T) {
	svc, _, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1", "r2")
	connect(t, svc, "c1", "inst-a", "r3")

	if got, want := clientRooms(t, rdb, "c1", "inst-a"), []string{"r3"}; !slices.Equal(got, want) {
		t.Fatalf("client rooms = %v, want %v (saveClient must DEL before SADD)", got, want)
	}
}

func TestConnect_ReconnectLeavesStaleRoomEntriesUntilTTL(t *testing.T) {
	svc, mr, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1", "r2")
	connect(t, svc, "c1", "inst-a", "r3")

	if _, ok := roomFields(t, rdb, "r1")["c1|inst-a"]; !ok {
		t.Fatal("expected stale entry in r1 right after reconnect (current behaviour)")
	}

	mr.FastForward(connTTL + time.Second)
	if got := roomFields(t, rdb, "r1"); len(got) != 0 {
		t.Fatalf("stale entry should be gone after TTL, got %v", got)
	}
}

func TestConnect_NoRooms_FailsCurrently(t *testing.T) {
	svc, mr, _ := newTestService(t)

	resp, err := svc.Connect(context.Background(), &contractsv1.ConnectUserRequest{
		ClientId:     "c1",
		InstanceName: "inst-a",
	})
	if err == nil {
		t.Fatal("expected an error for empty rooms")
	}
	if resp == nil || resp.GetSuccess() {
		t.Fatalf("expected non-nil response with Success=false, got %v", resp)
	}
	if mr.Exists("c1|inst-a") {
		t.Fatal("nothing should be written when the transaction is rejected")
	}
}

func TestConnect_RedisFailure(t *testing.T) {
	tests := []struct {
		name   string
		break_ func(*miniredis.Miniredis, *redis.Client)
	}{
		{"server returns errors", func(mr *miniredis.Miniredis, _ *redis.Client) { mr.SetError("ERR boom") }},
		{"pipeline exec fails", func(_ *miniredis.Miniredis, rdb *redis.Client) {
			rdb.AddHook(failExec{err: errors.New("exec failed")})
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, mr, rdb := newTestService(t)
			tc.break_(mr, rdb)

			resp, err := svc.Connect(context.Background(), &contractsv1.ConnectUserRequest{
				ClientId: "c1", InstanceName: "inst-a", RoomsId: []string{"r1"},
			})
			if err == nil {
				t.Fatal("expected error")
			}
			if resp == nil || resp.GetSuccess() {
				t.Fatalf("expected Success=false, got %v", resp)
			}
		})
	}
}

func TestDisconnect_RemovesClientAndItsRoomEntries(t *testing.T) {
	svc, mr, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1", "r2")
	connect(t, svc, "c2", "inst-b", "r1")

	resp, err := svc.Disconnect(context.Background(), &contractsv1.DisconnectUserRequest{
		ClientId: "c1", InstanceName: "inst-a",
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("Disconnect: resp=%v err=%v", resp, err)
	}

	if mr.Exists("c1|inst-a") {
		t.Fatal("client key should be deleted")
	}
	if got, want := roomFields(t, rdb, "r1"), map[string]string{"c2|inst-b": "inst-b"}; !maps.Equal(got, want) {
		t.Fatalf("r1 fields = %v, want %v", got, want)
	}
	if got := roomFields(t, rdb, "r2"); len(got) != 0 {
		t.Fatalf("r2 should be empty, got %v", got)
	}
}

func TestDisconnect_UnknownClientIsNoOpSuccess(t *testing.T) {
	svc, _, rdb := newTestService(t)
	connect(t, svc, "c2", "inst-b", "r1")

	resp, err := svc.Disconnect(context.Background(), &contractsv1.DisconnectUserRequest{
		ClientId: "ghost", InstanceName: "inst-a",
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("Disconnect: resp=%v err=%v", resp, err)
	}
	if got, want := roomFields(t, rdb, "r1"), map[string]string{"c2|inst-b": "inst-b"}; !maps.Equal(got, want) {
		t.Fatalf("unrelated data changed: %v", got)
	}
}

func TestDisconnect_OnlyAffectsTheGivenInstance(t *testing.T) {
	svc, _, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")
	connect(t, svc, "c1", "inst-b", "r1")

	resp, err := svc.Disconnect(context.Background(), &contractsv1.DisconnectUserRequest{
		ClientId: "c1", InstanceName: "inst-a",
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("Disconnect: resp=%v err=%v", resp, err)
	}

	if got, want := roomFields(t, rdb, "r1"), map[string]string{"c1|inst-b": "inst-b"}; !maps.Equal(got, want) {
		t.Fatalf("r1 fields = %v, want %v", got, want)
	}
	if got := clientRooms(t, rdb, "c1", "inst-b"); !slices.Equal(got, []string{"r1"}) {
		t.Fatalf("inst-b connection must be untouched, rooms = %v", got)
	}
}

func TestDisconnect_RedisFailure(t *testing.T) {
	t.Run("SMembers fails", func(t *testing.T) {
		svc, mr, _ := newTestService(t)
		connect(t, svc, "c1", "inst-a", "r1")
		mr.SetError("ERR boom")

		resp, err := svc.Disconnect(context.Background(), &contractsv1.DisconnectUserRequest{
			ClientId: "c1", InstanceName: "inst-a",
		})
		if err == nil {
			t.Fatal("expected error")
		}
		if resp == nil || resp.GetSuccess() {
			t.Fatalf("expected Success=false, got %v", resp)
		}
	})

	t.Run("pipeline exec fails and nothing is removed", func(t *testing.T) {
		svc, mr, rdb := newTestService(t)
		connect(t, svc, "c1", "inst-a", "r1")
		rdb.AddHook(failExec{err: errors.New("exec failed")})

		resp, err := svc.Disconnect(context.Background(), &contractsv1.DisconnectUserRequest{
			ClientId: "c1", InstanceName: "inst-a",
		})
		if err == nil {
			t.Fatal("expected error")
		}
		if resp == nil || resp.GetSuccess() {
			t.Fatalf("expected Success=false, got %v", resp)
		}
		if !mr.Exists("c1|inst-a") {
			t.Fatal("client key must still exist after failed Disconnect")
		}
	})
}

func TestPing_RefreshesClientAndRoomTTLs(t *testing.T) {
	svc, mr, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1", "r2")

	mr.FastForward(40 * time.Second)
	if got := mr.TTL("c1|inst-a"); got != 20*time.Second {
		t.Fatalf("precondition: client TTL = %v, want 20s", got)
	}

	resp, err := svc.Ping(context.Background(), &contractsv1.PingConnectionRequest{
		ClientId: "c1", InstanceName: "inst-a",
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("Ping: resp=%v err=%v", resp, err)
	}

	if got := mr.TTL("c1|inst-a"); got != connTTL {
		t.Fatalf("client TTL after Ping = %v, want %v", got, connTTL)
	}
	for _, room := range []string{"r1", "r2"} {
		if got := fieldTTL(t, rdb, room, "c1|inst-a"); got < 59 || got > 60 {
			t.Fatalf("room %s field TTL after Ping = %ds, want ~60", room, got)
		}
	}
}

func TestPing_KeepsPresenceAliveBeyondOriginalTTL(t *testing.T) {
	svc, mr, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")

	for range 2 {
		mr.FastForward(40 * time.Second)
		resp, err := svc.Ping(context.Background(), &contractsv1.PingConnectionRequest{
			ClientId: "c1", InstanceName: "inst-a",
		})
		if err != nil || !resp.GetSuccess() {
			t.Fatalf("Ping: resp=%v err=%v", resp, err)
		}
	}

	if !mr.Exists("c1|inst-a") {
		t.Fatal("client key expired despite regular pings")
	}
	if _, ok := roomFields(t, rdb, "r1")["c1|inst-a"]; !ok {
		t.Fatal("room field expired despite regular pings")
	}
}

func TestPing_DoesNotRefreshOtherClients(t *testing.T) {
	svc, mr, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")
	connect(t, svc, "c2", "inst-b", "r1")

	mr.FastForward(40 * time.Second)
	if _, err := svc.Ping(context.Background(), &contractsv1.PingConnectionRequest{
		ClientId: "c1", InstanceName: "inst-a",
	}); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	mr.FastForward(30 * time.Second)

	want := map[string]string{"c1|inst-a": "inst-a"}
	if got := roomFields(t, rdb, "r1"); !maps.Equal(got, want) {
		t.Fatalf("room fields = %v, want only %v (c2 should have expired)", got, want)
	}
}

func TestPing_UnknownClientSucceedsWithoutRegistering(t *testing.T) {
	svc, mr, rdb := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")
	mr.FastForward(connTTL + time.Second)

	resp, err := svc.Ping(context.Background(), &contractsv1.PingConnectionRequest{
		ClientId: "c1", InstanceName: "inst-a",
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("Ping: resp=%v err=%v", resp, err)
	}
	if mr.Exists("c1|inst-a") {
		t.Fatal("Ping must not resurrect an expired client")
	}
	if got := roomFields(t, rdb, "r1"); len(got) != 0 {
		t.Fatalf("Ping must not resurrect room membership, got %v", got)
	}
}

func TestPing_RedisFailure(t *testing.T) {
	tests := []struct {
		name   string
		break_ func(*miniredis.Miniredis, *redis.Client)
	}{
		{"server returns errors", func(mr *miniredis.Miniredis, _ *redis.Client) { mr.SetError("ERR boom") }},
		{"pipeline exec fails", func(_ *miniredis.Miniredis, rdb *redis.Client) {
			rdb.AddHook(failExec{err: errors.New("exec failed")})
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, mr, rdb := newTestService(t)
			connect(t, svc, "c1", "inst-a", "r1")
			tc.break_(mr, rdb)

			resp, err := svc.Ping(context.Background(), &contractsv1.PingConnectionRequest{
				ClientId: "c1", InstanceName: "inst-a",
			})
			if err == nil {
				t.Fatal("expected error")
			}
			if resp == nil || resp.GetSuccess() {
				t.Fatalf("expected Success=false, got %v", resp)
			}
		})
	}
}

func TestIsConnected_ReturnsDistinctInstanceNames(t *testing.T) {
	svc, _, _ := newTestService(t)

	connect(t, svc, "c1", "inst-a", "r1")
	connect(t, svc, "c2", "inst-a", "r1")
	connect(t, svc, "c3", "inst-b", "r1")

	resp, err := svc.IsConnected(context.Background(), &contractsv1.ClientsConnectionDestRequest{RoomId: "r1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, want := sorted(resp.GetInstanceNames()), []string{"inst-a", "inst-b"}; !slices.Equal(got, want) {
		t.Fatalf("instances = %v, want %v", got, want)
	}
}

func TestIsConnected_EmptyRoom(t *testing.T) {
	svc, _, _ := newTestService(t)

	resp, err := svc.IsConnected(context.Background(), &contractsv1.ClientsConnectionDestRequest{RoomId: "nobody-here"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || len(resp.GetInstanceNames()) != 0 {
		t.Fatalf("expected empty response, got %v", resp)
	}
}

func TestIsConnected_OnlyReturnsRequestedRoom(t *testing.T) {
	svc, _, _ := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")
	connect(t, svc, "c2", "inst-b", "r2")

	resp, err := svc.IsConnected(context.Background(), &contractsv1.ClientsConnectionDestRequest{RoomId: "r1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := resp.GetInstanceNames(), []string{"inst-a"}; !slices.Equal(got, want) {
		t.Fatalf("instances = %v, want %v", got, want)
	}
}

func TestIsConnected_ExpiredClientsDropOut(t *testing.T) {
	svc, mr, _ := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")
	connect(t, svc, "c2", "inst-b", "r1")

	mr.FastForward(40 * time.Second)

	if _, err := svc.Ping(context.Background(), &contractsv1.PingConnectionRequest{
		ClientId: "c2", InstanceName: "inst-b",
	}); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	mr.FastForward(30 * time.Second)

	resp, err := svc.IsConnected(context.Background(), &contractsv1.ClientsConnectionDestRequest{RoomId: "r1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := resp.GetInstanceNames(), []string{"inst-b"}; !slices.Equal(got, want) {
		t.Fatalf("instances = %v, want %v", got, want)
	}
}

func TestIsConnected_ReflectsDisconnect(t *testing.T) {
	svc, _, _ := newTestService(t)
	connect(t, svc, "c1", "inst-a", "r1")

	if _, err := svc.Disconnect(context.Background(), &contractsv1.DisconnectUserRequest{
		ClientId: "c1", InstanceName: "inst-a",
	}); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	resp, err := svc.IsConnected(context.Background(), &contractsv1.ClientsConnectionDestRequest{RoomId: "r1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetInstanceNames()) != 0 {
		t.Fatalf("expected no instances after Disconnect, got %v", resp.GetInstanceNames())
	}
}

func TestIsConnected_RedisFailure(t *testing.T) {
	svc, mr, _ := newTestService(t)
	mr.SetError("ERR boom")

	resp, err := svc.IsConnected(context.Background(), &contractsv1.ClientsConnectionDestRequest{RoomId: "r1"})
	if err == nil {
		t.Fatal("expected error")
	}
	if resp != nil {
		t.Fatalf("expected nil response on error, got %v", resp)
	}
}
