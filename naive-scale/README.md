# naive-scale — approach #2

The "just add more boxes" reflex: run **N** copies of the same Go process
behind one nginx load balancer. The compose file currently defines **ten**
instances at **100m / 0.8 CPU** each (the first experiments used three at 200m;
the single-instance approach ran one process on 600m). Postgres and nginx keep the
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
nginx/nginx.conf       REST round-robin; /ws hashed by ?room= (room affinity) across server1..15
docker-compose.yml     postgres + redis + 15x server (100m each) + nginx
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
docker compose up       # full stack: postgres + redis + 15 servers + nginx on :80
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
`single-instance` and adapted to the multi-instance + Redis topology. It answers two
questions — the concurrent-connection ceiling and the message-rate ceiling — and
compares them against the single-instance baseline. See
[`loadtest/README.md`](loadtest/README.md) for how to run the sweeps and
[`loadtest/BASELINE.md`](loadtest/BASELINE.md) for the recorded numbers.

```powershell
docker compose up -d
./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000
./loadtest/summarize.ps1  -Dir loadtest/results/10x100m/conn
```

### Results

Full write-up, with tables and the failure analysis:
[`docs/LOAD_TESTS.md`](docs/LOAD_TESTS.md).

- **7 500 sockets (500 rooms × 15 users), 5 vs 10 instances × 100 MB, room-affinity
  routing:** both are flat and lossless up to ≈ 1 250 sent/s (≈ 18 750 delivered/s,
  p95 ≈ 1.8 s). At ≈ 1 870 sent/s nothing is lost but latency climbs to 10–18 s as
  the outbox queues; at ≈ 2 500 sent/s completeness drops to 78–88 % and ws errors
  start; beyond that delivery collapses. **Doubling the instances did not move the
  ceiling**, so the limit is a resource shared by all instances (Postgres, Redis,
  nginx or the host) — not yet attributed, since no resource metrics were captured.
- **Earlier 3 × 200 MB, round-robin, 10 000 sockets:** ≈ 8 537 delivered msg/s at
  99.1 % completeness vs ≈ 5 542 for single-instance (+54 %); see
  [`loadtest/BASELINE.md`](loadtest/BASELINE.md).
- **Room affinity** (`hash $arg_room consistent` for `/ws` in `nginx/nginx.conf`)
  is now in place, so a room's fan-out is an in-process write; Redis pub/sub is
  kept as the cross-instance bridge for the relay that claims a row.
