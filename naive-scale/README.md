# naive-scale — approach #2

The "just add more boxes" reflex: run **three** copies of the same Go process
behind one nginx load balancer, each on a **200m** memory budget (the
single-instance approach ran one process on 600m). Postgres and nginx keep the
single-instance configuration.

The server code started as a straight copy of `single-instance` — same REST +
websocket endpoints, same transactional outbox — behind three replicas that nginx
round-robins. The gap that split created (a message persisted on one instance
would only reach the clients on *that* instance) is closed by a **Redis pub/sub
`Broadcaster`** behind the same interface: `hub.Broadcast` publishes each event to
a `room:<id>` channel and every instance subscribed to that room re-delivers to
its local clients, so a message now reaches clients on any instance. Measuring
that fan-out against the single-instance baseline is what `loadtest/` is for.

All three instances run their **own** outbox relay against the **same** table, so
a relay must claim rows or it re-broadcasts another's: the relay claims each batch
with `storage.DispatchBatch` (`FOR UPDATE SKIP LOCKED` + mark, in one transaction),
delivering each message exactly once. (The load test caught the pre-fix version
delivering 2–3× — completeness > 100 %; see
[`loadtest/README.md`](loadtest/README.md) "Fixed wrinkle".)

```
cmd/server/main.go     wiring + graceful shutdown
internal/config/       GetEnv(key, fallback)
internal/transport/    http.go (REST), ws.go (upgrade)
internal/hub/          per-instance hub; Redis pub/sub for cross-instance fan-out
internal/chat/         service.go — validate→(tx: persist+enqueue); Broadcaster iface
internal/outbox/       relay.go — poll outbox; drain → Broadcaster (publishes to Redis)
internal/storage/      postgres.go, messages.go, outbox.go, rooms.go, users.go
internal/models/       message.go, room.go, user.go
migrations/            0001_init.sql (goose, embedded)
nginx/nginx.conf       load-balances across server1..3
docker-compose.yml     postgres + redis + 3x server (200m each) + nginx
loadtest/              k6 ws load test (ported from single-instance; 3x200m + Redis)
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
docker compose up       # full stack: postgres + redis + 3 servers + nginx on :80
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

## Load tests

k6 WebSocket load test under [`loadtest/`](loadtest/README.md), ported from
`single-instance` and adapted to the 3×200m + Redis topology. It answers two
questions — the concurrent-connection ceiling and the message-rate ceiling — and
compares them against the single-instance baseline. See
[`loadtest/README.md`](loadtest/README.md) for how to run the sweeps and
[`loadtest/BASELINE.md`](loadtest/BASELINE.md) to record the numbers.

```powershell
docker compose up -d
./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000
./loadtest/summarize.ps1  -Dir loadtest/results/3x200m/conn
```
