# naive-scale

**The easiest way to scale a real-time chat application: run more copies of it.**

This project takes the one-process chat server from [`../single-instance`](../single-instance),
runs **N identical copies** behind **nginx round-robin**, and adds only what is strictly
needed for chat to stay correct across copies:

- **Redis pub/sub** so a message sent on one instance reaches room members connected to the others.
- **`FOR UPDATE SKIP LOCKED`** on the outbox so N relays sharing one table don't deliver a message twice.

There is no sharding, no room affinity, no service split and no new infrastructure beyond Redis.
The question it answers: *how far does the obvious "just add instances" approach get you, and what
does it cost?* Its load-test numbers are the reference that smarter designs (see
[`../proper-scale`](../proper-scale)) have to beat.

> **This is a proof of concept, not a product.** It is one step in an experiment on how to scale a
> real-time chat application: [`single-instance`](../single-instance) is the baseline, `naive-scale`
> (this project) is the "just add instances" approach, and [`proper-scale`](../proper-scale) is the
> redesign. Treat it as a measuring instrument, not production code. There is no authentication
> (the client sends its own `user_id`), no schema migration or upgrade path (the data is wiped
> between runs), no TLS, and the compose file hard-codes credentials and a fixed fleet of 7 servers
> with deliberately tiny resource limits so that capacity limits show up quickly.

## Architecture

```mermaid
flowchart TB
    subgraph fleet["server1 ... serverN: identical Go processes (50 MB / 0.17 CPU each)"]
        S1["server1<br/>REST + WebSocket · hub · outbox relay"]
        S2["server2<br/>REST + WebSocket · hub · outbox relay"]
        SN["serverN<br/>REST + WebSocket · hub · outbox relay"]
    end
    C["Clients / k6<br/>one WebSocket per user"]
    N["nginx :80<br/>round-robin per new connection<br/>no room affinity"]
    PG[("PostgreSQL 16<br/>users · rooms · messages · outbox")]
    R[("Redis 7 pub/sub<br/>one channel per room: room:{id}")]

    C --> N
    N --> S1
    N --> S2
    N --> SN
    S1 <--> PG
    S2 <--> PG
    SN <--> PG
    S1 <--> R
    S2 <--> R
    SN <--> R
```

nginx is the only published entry point for the app. Each server's `:8080` is internal to the
compose network, so clients and load tests always go through the load balancer.

### Life of a message

```mermaid
sequenceDiagram
    participant A as Alice (socket on server1)
    participant S1 as server1
    participant PG as PostgreSQL
    participant S2 as server2
    participant R as Redis
    participant B as Bob (socket on server2)

    A->>S1: ws frame {"user_id", "body"}
    S1->>PG: one tx: insert message + outbox row
    Note over S1,S2: every server's relay polls the shared outbox every 2 s
    S2->>PG: claim up to 100 rows (FOR UPDATE SKIP LOCKED), mark dispatched
    S2->>R: PUBLISH room:{id}
    R-->>S1: message (server1 hosts a room member)
    R-->>S2: message (server2 hosts a room member)
    S1-->>A: echo
    S2-->>B: deliver
```

1. **Ingest.** The sender's socket is pinned to whichever server nginx picked at the handshake.
   That server validates the frame (`internal/chat/service.go`) and writes the message and an
   outbox row in **one transaction**.
2. **Relay.** Every server runs its own relay (`internal/outbox/relay.go`): a 2 s ticker, batches
   of 100. `storage.DispatchBatch` claims rows with `FOR UPDATE SKIP LOCKED`, so the N relays take
   disjoint batches. *Any* relay may dispatch a given message, not necessarily the one that
   ingested it.
3. **Fan-out.** The relay `PUBLISH`es to Redis channel `room:<id>`. Each server's hub
   (`internal/hub/hub.go`) subscribes to a room's channel while it has at least one local member
   and unsubscribes when the last one leaves.
4. **Deliver.** Each subscribed server writes the frame to its local members of that room,
   including the sender's own echo.

### What makes it naive

| Shortcut | Consequence |
| --- | --- |
| Round-robin per connection, no room affinity | A room's members are spread over all servers, so every server subscribes to nearly every room. |
| Redis broadcasts to every subscriber | Every message is received and processed by **all N** servers, even though each one only writes to its own few sockets. This per-message cost grows with N. |
| One outbox table for N relays | Relays compete for the same rows. `SKIP LOCKED` keeps it correct, but all of it lands on the one Postgres. |
| One Postgres for everything | Every ingest and every claim still goes to a single database. |

The only code that differs from `single-instance` is the Redis hub (`internal/hub/hub.go`), the
relay (`internal/outbox/relay.go`) and the locked claim (`internal/storage/outbox.go`).

## Results

The workload was 100 rooms × 15 users = 1,500 held sockets, with the send interval swept until
delivery broke. The servers were 50 MB / 0.17 CPU each, against a single-instance baseline of
150 MB / 0.5 CPU. The full tables, method and reasoning are in
[`LOAD_TEST_ANALYSIS.md`](LOAD_TEST_ANALYSIS.md). Raw results are in `loadtest/results/<N>x50mib/tput/`.

| | single-instance | naive 3× | naive 5× | naive 7× |
| --- | ---: | ---: | ---: | ---: |
| Server CPU (total) | 0.50 | 0.51 | 0.85 | 1.19 |
| Last all-green step | 375 msg/s | 250 msg/s | 500 msg/s | 500 msg/s |
| Raw capacity | 375–400 msg/s | 307–320 msg/s | 452–467 msg/s | 535–605 msg/s |
| vs single-instance | — | −15 to −23% | +13 to +25% | +34 to +61% |

- **Splitting the same budget three ways loses ~20%.** Redis and the per-instance overhead cost
  more than the parallelism buys.
- **Each added instance buys less than the one before.** Server CPU per message fits
  ≈ 1.25 ms + 0.12 ms × N. That is inferred from a two-parameter fit to three points, so it is a
  consistency check, not a measurement. The N term is the "every server receives every message"
  cost from the table above.
- **Extrapolated, not measured:** ~690 msg/s at 10 instances and ~930 at 20. A hard ceiling sits
  near ~1,400 msg/s however many 0.17-CPU instances you add.
- **What does improve at any N is the latency floor.** With N relays polling, the light-load median
  drops from ~0.8 s to ~0.3 s.
- **The next lever is room affinity** (`hash $arg_room consistent` on `chat_ws_backend`). It sends a
  room's sockets to one server, which should remove the N term.

## Getting started

### Prerequisites

- Docker with Compose. The stack and the default load-test runner are containers.
- Go 1.26.3 (`go.mod`), only for running the server or tests outside Docker.
- [k6](https://k6.io/docs/get-started/installation/), only for running load tests natively.
  `run-limits.ps1` uses a containerized `grafana/k6` by default.
- PowerShell, for the load-test runners in `loadtest/`.

### Run the stack

```bash
docker compose up --build
```

This starts Postgres, Redis, `server1`..`server7` and nginx. Each server applies the goose
migrations from `migrations/` on startup.

| Service | Host port | Notes |
| --- | --- | --- |
| nginx | `80` | the app's only entry point (REST + `/ws`) |
| PostgreSQL | `5432` | `chat` / `chat`, database `chat` |
| Redis | `6379` | pub/sub only |
| `server1..7` | — | `:8080` inside the compose network |

### Configuration

Each server reads three environment variables (`cmd/server/main.go`):

| Variable | Default | In `docker-compose.yml` |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | `:8080` |
| `DATABASE_URL` | `postgres://chat:chat@localhost:5432/chat?sslmode=disable` | `...@postgres:5432/chat?sslmode=disable&pool_max_conns=8` |
| `REDIS_ADDR` | `localhost:6379` | `redis:6379` |

### API

| Method | Path | Body / query |
| --- | --- | --- |
| `GET` | `/healthz` | — |
| `GET` / `POST` | `/api/rooms` | `{"name": "..."}` |
| `GET` / `POST` | `/api/users` | `{"username": "..."}` |
| `GET` | `/api/rooms/{id}/messages` | `?before=<message id>&limit=1..200` (default 50) |
| `GET` | `/ws?room=<room id>` | upgrade, then send `{"user_id": "...", "body": "..."}` |

Messages arrive on the socket as `{"id", "room_id", "user_id", "body", "created_at"}`.

### Quick smoke test

With the stack up, the k6 script's defaults (1 room, 2 users) make a short end-to-end check:

```bash
k6 run -e RAMP=5 -e DURATION=10 -e DRAIN=5 -e SEND_INTERVAL=2 loadtest/chat_load.js
```

### Tests

```bash
go test ./...
go test -tags integration ./internal/integration/
```

The integration suite starts its own `postgres:16` and `redis:7-alpine` containers through
testcontainers, so Docker must be running. It includes
`TestOutbox_ConcurrentRelaysDispatchEachRowOnce`, which checks that the N relays never double-deliver.

### Load tests

[`loadtest/README.md`](loadtest/README.md) documents the k6 script and its parameters. The sweeps
in [Results](#results) were run like this, with `-Tag` matching the number of servers in the stack:

```powershell
./loadtest/run-limits.ps1 -Scenario tput -Rooms 100 -Users 15 -Tag 7x50mib -Steps 4,3,2,1.5,1
./loadtest/summarize.ps1  -Dir loadtest/results/7x50mib/tput
```

`run-limits.ps1` wipes and recreates the whole stack (`docker compose down -v`) before every step.

## Project layout

| Path | Contents |
| --- | --- |
| `cmd/server/` | entry point: migrate, connect, start hub + relay + HTTP server |
| `internal/chat/` | message validation; ingest transaction (message + outbox row) |
| `internal/hub/` | per-instance room → sockets map, Redis subscribe/publish, socket read/write pumps |
| `internal/outbox/` | the relay: poll, claim, publish |
| `internal/storage/` | Postgres access: rooms, users, messages, outbox claim, migrations runner |
| `internal/transport/` | REST routes and the WebSocket upgrade |
| `internal/integration/` | end-to-end tests against real Postgres + Redis (`-tags integration`) |
| `migrations/` | goose SQL schema, embedded into the binary |
| `nginx/` | load-balancer config |
| `loadtest/` | k6 script, PowerShell runners and summarizer, results |
| `LOAD_TEST_ANALYSIS.md` | analysis of the 3 / 5 / 7-instance sweeps against single-instance |
