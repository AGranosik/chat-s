module messages

go 1.26.3

require (
	github.com/AGranosik/chat/contracts v0.0.0
	github.com/AGranosik/chat/contracts/messages v0.0.0
	github.com/gorilla/websocket v1.5.3
	github.com/jackc/pgx/v5 v5.11.0
	golang.org/x/sync v0.22.0
	google.golang.org/grpc v1.83.2
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	go.uber.org/atomic v1.12.0 // indirect
)

require (
	github.com/redis/go-redis/v9 v9.23.0
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/AGranosik/chat/contracts => ../../contracts/protos

replace github.com/AGranosik/chat/contracts/messages => ../../contracts/messages
