# chat-s (v1) — single-instance limit finder

A real-time chat service in Go, built to answer one question:

> **How far can a single instance go?**

The point of this project is not to build the biggest chat in the world — it is to
**find the limits of one process** behind one load balancer. How many concurrent
WebSocket connections can a single Go server hold? How many rooms and messages per
second before latency degrades or handshakes start getting reset? Where does the
ceiling actually sit, and what causes it (file descriptors, accept backlog, memory,
the outbox poll)? Everything here — the deliberate single-instance design, the
nginx front door, the k6 load-test matrix — exists to measure that ceiling on
honest, production-shaped infrastructure.

It is intentionally **single instance, no message queue**. That constraint is the
experiment: a clean in-process design whose limits we can characterize, with a
documented seam to scale out *later* (see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)).

## What it does

- Real-time chat over **WebSocket**: clients join a room and exchange messages live.
- **Postgres** is the system of record; message history is served over REST.
- A **transactional outbox** makes "persist the message" and "broadcast it" atomic,
  so a crash never strands a message.
- **nginx** sits in front as the load balancer and the **only** entry point.

## Quick start

You need [Docker](https://docs.docker.com/get-docker/). Bring up the full stack:

```bash
docker compose up
```

This starts Postgres, the Go server, and nginx. **nginx on `http://localhost:80` is
the only reachable entry point** — the Go server's port is internal to the compose
network and cannot be hit directly (Postgres is also published on `localhost:5432`
for inspection). Check it's alive:

```bash
curl http://localhost:80/healthz      # -> ok
```

### Try it by hand

```bash
# create a room and a user (note their returned ids)
curl -X POST http://localhost:80/api/rooms  -d '{"name":"general"}'
curl -X POST http://localhost:80/api/users  -d '{"username":"alice"}'

# read a room's history (keyset pagination)
curl "http://localhost:80/api/rooms/<room-id>/messages?limit=50"
```

Then open a WebSocket to `ws://localhost:80/ws?room=<room-id>` and send frames:

```json
{ "user_id": "<user-id>", "body": "hello" }
```

Every member of the room receives the stored message back:

```json
{ "id": 42, "room_id": "<room-id>", "user_id": "<user-id>", "body": "hello", "created_at": "2026-06-25T12:00:00Z" }
```

(`body` must be non-empty and at most 4000 bytes; `user_id` is required.)

## Resource budget under test

The compose file pins every container so the numbers mean something. The Go server
is the system under test and is deliberately the tightest budget; Postgres and nginx
have headroom so they are not the first wall.

| service  | memory | CPU | notes                                              |
|----------|-------:|----:|----------------------------------------------------|
| server   | 300 MB |   8 | the SUT; `nofile` 262144, `somaxconn` 65535        |
| postgres | 600 MB |   2 | no volume — a fresh container is an empty database |
| nginx    | 512 MB |   1 | `worker_connections` 65536, `nofile` 262144        |

## Finding the limit — load testing

This is the main event. A [k6](https://k6.io) WebSocket load test lives in
[`loadtest/`](loadtest/README.md) and drives the stack the way a real client
would — **through nginx**. One virtual user = one user = one long-lived socket.
Connections **ramp up gradually** before holding at full load, because opening
thousands of sockets on the same tick swamps the accept backlog and masquerades as
a server ceiling. Throughput runs ramp, settle, send for a fixed window, then keep
sockets open to drain, and count every late arrival toward completeness.

```powershell
# recreate a clean stack and run a small smoke scenario (10 rooms x 5 users)
./loadtest/run-clean.ps1

# larger preset cells (300 and 1000 rooms x 10 users)
./loadtest/run-clean.ps1 -Full

# limit-finding sweeps against the 300 MB server (recreates the stack, wipes the DB)
./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000
./loadtest/run-limits.ps1 -Scenario tput -Rooms 500 -Users 15 -Steps 10,8,6,4,3,2,1

# read a sweep: table, ceiling hint, summary.md / summary.csv
./loadtest/summarize.ps1 -Dir loadtest/results/300m/tput
```

`run-limits.ps1` wipes the stack (`docker compose down -v`) and brings it back up
before every step, waits for the server container and nginx to report ready, and writes one JSON
per step to `loadtest/results/<tag>/<scenario>/`. See
[`loadtest/README.md`](loadtest/README.md) for the parameters and the OS
file-descriptor tuning the bigger cells need.

### Previous result — 300 MB server, 7,500 sockets (500 rooms × 15 users)

Measured with the earlier relay (2 s poll, 100-row batches). The relay now polls
every 1 s with 500-row batches and the sweep is being re-run, so treat the table
as provisional until `LOAD_TEST_ANALYSIS.md` is refreshed.

| send interval | offered/s | delivered/s | complete | WS errors | e2e p50 / p95 |
|--------------:|----------:|------------:|---------:|----------:|--------------:|
| 10 s | 751 | 11,258 | 100.0% | 1 | 1.2 / 1.8 s |
| 6 s | 1,251 | 18,757 | 100.0% | 0 | 1.2 / 1.8 s |
| 4 s | 1,875 | **28,124** | 100.0% | 0 | 4.1 / 7.4 s |
| 3 s | 2,501 | 21,627 | 57.7% | 0 | 35 / 42 s |
| 2 s | 3,694 | 2,946 | 5.3% | 3,149 | 53 / 57 s |

- The ceiling is about **28,100 delivered frames/s** (1,875 sends/s, 15 deliveries
  each) at full completeness; the latency-clean point is about **18,800 frames/s**.
- It does not plateau: past the knee delivered throughput *falls* as offered load
  rises, and at roughly twice the ceiling sockets start erroring. Handshakes stayed
  at 100% throughout, so it is the delivery path, not the connection count.
- The cause of the final collapse (OOM kill at 300 MB vs. the hub's blocking
  broadcast) was not captured. The full table, reasoning and the list of what is
  still unproven are in [`LOAD_TEST_ANALYSIS.md`](LOAD_TEST_ANALYSIS.md).
- There is no connection-only (`conn`) result for the 300 MB budget yet.

## Configuration

Configured via environment variables (each has a safe fallback):

| var            | default                                                        | meaning                          |
|----------------|----------------------------------------------------------------|----------------------------------|
| `HTTP_ADDR`    | `:8080`                                                        | server listen address (internal) |
| `DATABASE_URL` | `postgres://chat:chat@localhost:5432/chat?sslmode=disable`     | Postgres connection string       |

## Development

```bash
go run ./cmd/server          # run locally (needs Postgres; see docker-compose.yml)
go build ./... && go vet ./...
go test ./...                # unit tests — fast, no Docker
go test -tags=integration ./internal/integration/   # end-to-end vs a throwaway Postgres (needs Docker)
```

## Where to read more

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — the design and the reasoning
  behind the single-instance choice, the hub, the outbox, and the scaling seam.
- [`loadtest/README.md`](loadtest/README.md) — how to run and read the load tests.
- [`LOAD_TEST_ANALYSIS.md`](LOAD_TEST_ANALYSIS.md) — measured results, where the
  instance breaks, and what is still unproven.
