# naive-scale — approach #2

The "just add more boxes" reflex: run **three** copies of the same Go process
behind one nginx load balancer, each on a **200m** memory budget (the
single-instance approach ran one process on 600m). Postgres and nginx keep the
single-instance configuration.

The server code is a straight copy of `single-instance` — same REST + websocket
endpoints, same transactional outbox, same in-memory hub. The only difference is
in `docker-compose.yml` and `nginx.conf`: three replicas, round-robined. That is
deliberate. Each instance runs its **own** in-memory hub with **no shared
broadcaster**, so a message persisted on one instance is only broadcast to the
websocket clients connected to *that* instance — clients pinned elsewhere never
see it. Closing that gap (a Redis/Kafka `Broadcaster` behind the same interface)
is what this approach will measure against the single-instance baseline.

```
cmd/server/main.go     wiring + graceful shutdown
internal/config/       GetEnv(key, fallback)
internal/transport/    http.go (REST), ws.go (upgrade)
internal/hub/          per-instance in-memory hub (no cross-instance fan-out)
internal/chat/         service.go — validate→(tx: persist+enqueue); Broadcaster iface
internal/outbox/       relay.go — poll outbox; drain → Broadcaster
internal/storage/      postgres.go, messages.go, outbox.go, rooms.go, users.go
internal/models/       message.go, room.go, user.go
migrations/            0001_init.sql (goose, embedded)
nginx/nginx.conf       load-balances across server1..3
docker-compose.yml     postgres + 3x server (200m each) + nginx
```

## Endpoints

| Method | Path                         | Purpose                                  |
|--------|------------------------------|------------------------------------------|
| GET    | `/healthz`                   | liveness                                 |
| GET    | `/api/rooms`                 | list rooms                               |
| POST   | `/api/rooms`                 | create a room `{"name": ...}`            |
| GET    | `/api/users`                 | list users                               |
| POST   | `/api/users`                 | create a user `{"username": ...}`        |
| GET    | `/api/rooms/{id}/messages`   | room history (`?before=<id>&limit=<n>`)  |
| GET    | `/ws?room=<id>`              | websocket; send `{"user_id","body"}`     |

## Run

```bash
go run ./cmd/server     # run one instance locally (needs Postgres; see docker-compose.yml)
docker compose up       # full stack: postgres + 3 servers + nginx on :80
go build ./...          # sanity build
```

## Tests

```bash
go build ./... && go vet ./...                       # build + vet
go test ./...                                        # unit tests (fast, no Docker)
go test -tags=integration ./internal/integration/   # end-to-end vs throwaway Postgres (needs Docker)
```

Integration tests live behind a `//go:build integration` tag, so the default
`go test ./...` stays fast and DB-free. They spin up a `postgres:16` container
via testcontainers (one per package run) and exercise the real SQL, the
transactional outbox, the polling relay, and a full websocket round-trip. If the
container can't start (e.g. Docker unavailable) the suite fails rather than
skipping — integration tests are opt-in, so a failure to bring up the container
is a real failure.
