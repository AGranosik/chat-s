# chat-s (v1) — single-instance limit finder

A real-time chat service in Go, built to answer one question:

> **How far can a single instance go?**

The point of this project is not to build the biggest chat in the world. It is to
**find the limits of one process** behind one load balancer. How many concurrent
WebSocket connections can a single Go server hold? How many rooms and messages per
second before latency degrades or sockets start dropping? Where does the ceiling
actually sit, and what causes it: file descriptors, accept backlog, memory, the
outbox poll, the database? Everything here exists to measure that ceiling on
honest, production-shaped infrastructure: the deliberate single-instance design,
the nginx front door, and the k6 load-test matrix.

It is intentionally **single instance, no message queue**. That constraint is the
experiment: a clean in-process design whose limits we can characterize, with a
documented seam to scale out *later* (see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)).

## What it does

- Real-time chat over **WebSocket**: clients join a room and exchange messages live.
- **Postgres** is the system of record, and message history is served over REST.
- A **transactional outbox** makes "persist the message" and "broadcast it" atomic,
  so a crash never strands a message.
- **nginx** sits in front as the load balancer and the **only** entry point.

## Architecture

```mermaid
flowchart LR
    C["Clients / k6<br/>one WebSocket per user"]

    subgraph net["docker compose network"]
        N["nginx :80<br/>1 CPU · 512 MB"]

        subgraph S["Go server :8080 (internal only) · 8 CPU · 300 MB"]
            T["transport<br/>REST + /ws upgrade"]
            RP["ReadPump<br/>1 goroutine per socket"]
            SV["chat.Service<br/>validate + 1 transaction"]
            RL["outbox relay<br/>1 goroutine · 1 s poll · 500-row batches"]
            H["hub<br/>1 goroutine · rooms map"]
            WP["WritePump<br/>1 goroutine per socket<br/>256-slot send channel"]
        end

        PG[("Postgres 16 · 2 CPU · 600 MB<br/>users · rooms · messages · outbox")]
    end

    C <-->|"REST + WebSocket"| N
    N <-->|"proxy_pass"| T
    T -->|"register socket"| H
    T -->|"start pumps"| RP
    RP -->|"incoming frame"| SV
    SV -->|"INSERT message + outbox row"| PG
    T -->|"history, rooms, users"| PG
    RL -->|"claim / mark dispatched"| PG
    RL -->|"Broadcast"| H
    H -->|"blocking send"| WP
    WP -->|"outgoing frame"| N
```

- **One way in.** nginx on `:80` is the only published entry point. The Go server's
  `:8080` is `expose`d to the compose network only, so clients and the load test
  always go through the load balancer. Postgres is also published on
  `localhost:5432` for inspection.
- **Two goroutines per socket.** `ReadPump` reads frames and runs the message's
  Postgres transaction inline. `WritePump` drains a 256-slot channel to the socket
  and pings every 54 s.
- **One hub and one relay for the whole process.** The hub goroutine owns the room
  map and does every fan-out. The relay is the only caller of `Broadcast`.
- **Persist first, broadcast second.** `chat.Service` never touches the hub. It
  writes the message and an outbox row in one transaction, and the relay picks the
  row up on its next poll. A crash between the two steps cannot lose a broadcast,
  and the same poll is the crash-recovery path. Delivery is at-least-once.

### Life of a message

```mermaid
sequenceDiagram
    participant K as Sender socket
    participant RP as ReadPump
    participant PG as Postgres
    participant RL as Relay
    participant H as Hub
    participant WP as WritePump (each member)
    participant M as Room members

    K->>RP: frame with user_id and body
    RP->>PG: BEGIN, INSERT messages, INSERT outbox, COMMIT
    Note over RP,PG: the read goroutine waits here before reading the next frame or pong
    loop drain until a batch comes back short, then wait 1 s
        RL->>PG: claim up to 500 undispatched rows FOR UPDATE
        RL->>H: Broadcast(room, message) for each row
        H->>WP: push onto each member's send channel (blocks when full)
        RL->>PG: UPDATE dispatched_at, COMMIT
    end
    WP->>M: one WebSocket frame per member, sender included
```

Every stage on this path is serial somewhere: one transaction per message on the
sender's read goroutine, one relay, one hub. Those are what the load tests run
into. The design reasoning is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Quick start

You need [Docker](https://docs.docker.com/get-docker/). Bring up the full stack:

```bash
docker compose up
```

This starts Postgres, the Go server, and nginx. **nginx on `http://localhost:80` is
the only reachable entry point.** Check it's alive:

```bash
curl http://localhost:80/healthz      # -> ok
```

### Try it by hand

```bash
# create a room and a user (note their returned ids)
curl -X POST http://localhost:80/api/rooms  -d '{"name":"general"}'
curl -X POST http://localhost:80/api/users  -d '{"username":"alice"}'

# list them
curl http://localhost:80/api/rooms
curl http://localhost:80/api/users

# read a room's history (newest first; pass ?before=<id> for the next page, limit 1..200)
curl "http://localhost:80/api/rooms/<room-id>/messages?limit=50"
```

Then open a WebSocket to `ws://localhost:80/ws?room=<room-id>` and send frames:

```json
{ "user_id": "<user-id>", "body": "hello" }
```

Every member of the room, the sender included, receives the stored message back
within about a second (the outbox poll):

```json
{ "id": 42, "room_id": "<room-id>", "user_id": "<user-id>", "body": "hello", "created_at": "2026-06-25T12:00:00Z" }
```

(`body` must be non-empty after trimming and at most 4000 bytes, and `user_id` is required.)

## Resource budget under test

The compose file pins every container so the numbers mean something. The Go server
is the system under test and has the tightest budget for what it does.

| service  | memory | CPU | notes                                              |
|----------|-------:|----:|----------------------------------------------------|
| server   | 300 MB |   8 | the SUT; `nofile` 262144, `somaxconn` 65535        |
| postgres | 600 MB |   2 | no named volume; a fresh container is an empty DB  |
| nginx    | 512 MB |   1 | `worker_connections` 65536, `nofile` 262144        |

Postgres and nginx are sized so that they are not meant to be the first wall. No
per-container CPU or memory was recorded during the sweeps, so that is not
verified (see [`LOAD_TEST_ANALYSIS.md`](LOAD_TEST_ANALYSIS.md#what-saturates-first)).

## Finding the limit — load testing

This is the main event. A [k6](https://k6.io) WebSocket load test lives in
[`loadtest/`](loadtest/README.md) and drives the stack the way a real client
would: **through nginx**. One virtual user = one user = one long-lived socket.
Connections **ramp up gradually** before holding at full load, because opening
thousands of sockets on the same tick swamps the accept backlog and masquerades as
a server ceiling. Throughput runs ramp, settle, send for a fixed window, then keep
sockets open to drain, and count every late arrival toward completeness.

```powershell
# recreate a clean stack and run a small smoke scenario (10 rooms x 5 users)
./loadtest/run-clean.ps1

# larger preset cells (300 and 1000 rooms x 10 users)
./loadtest/run-clean.ps1 -Full

# limit-finding sweeps against the 300 MB server (wipes the stack before every step)
./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000
./loadtest/run-limits.ps1 -Scenario tput -Rooms 500 -Users 15 -Steps 10,8,6,4,3,2,1

# read a sweep: table, ceiling hint, summary.md / summary.csv
./loadtest/summarize.ps1 -Dir loadtest/results/300m/tput
```

`run-limits.ps1` builds the images once. Before every step it then wipes the stack
(`docker compose down -v`), brings it back up, and waits for the server container
and nginx to report ready, so each step starts with a fresh process and an empty
database. It writes one JSON per step to `loadtest/results/<tag>/<scenario>/`. See
[`loadtest/README.md`](loadtest/README.md) for the parameters and the OS
file-descriptor tuning the bigger cells need.

### Latest result — 300 MB server, 7,500 sockets (500 rooms × 15 users)

This sweep was committed 2026-11-11 to `loadtest/results/300m/tput/`.

| send interval | offered/s | delivered/s | complete | WS errors | e2e p50 / p95 / max |
|--------------:|----------:|------------:|---------:|----------:|--------------------:|
| 10 s | 750 | 11,251 | 100.0% | 1 | 0.69 / 1.04 / 1.26 s |
| 8 s | 938 | 14,075 | 100.0% | 0 | 0.77 / 1.13 / 1.56 s |
| 6 s | 1,250 | 18,754 | 100.0% | 0 | 0.87 / 1.24 / 1.81 s |
| 4 s | 1,873 | **28,099** | 100.0% | 1 | 1.14 / 2.05 / 2.82 s |
| 3 s | 2,501 | 37,510 | 100.0% | 0 | 16.8 / 22.0 / 22.9 s |
| 2 s | 3,744 | 33,564 | 59.8% | 1,455 | 39.6 / 42.1 / 43.1 s |
| 1 s | 7,363 | 4,951 | 4.5% | 4,742 | 28.9 / 54.6 / 58.2 s |

- **Sustainable ceiling: about 28,100 delivered frames/s** (1,873 sends/s × 15
  recipients) at the 4 s step. It is the highest step that passes every k6
  threshold (p95 < 2.5 s, p99 < 3.5 s, WS errors ≤ 38).
- **Latency-clean point: about 18,750 frames/s** (1,250 sends/s), p95 1.24 s. The
  1 s outbox poll sets a floor of about 1.04 s p95.
- The 3 s step is 100% complete but not sustainable. Messages were up to 23 s
  late and arrived only because the senders stopped after 30 s.
- Past the knee the server collapses instead of plateauing. Handshakes were 100% at
  every step. The sockets that drop look like the server's own 60 s pong deadline
  firing: the shortest session in both failed steps is exactly 60.0 s.
- Against the previous sweep, which kept Postgres data between steps, every loaded
  step improved. For example, the 4 s step's p95 went from 3.1 s to 2.0 s.
- There is still no connection-only (`conn`) result for the 300 MB budget.

The full table, the reasoning and what is still unproven are in
[`LOAD_TEST_ANALYSIS.md`](LOAD_TEST_ANALYSIS.md).

## Known limitations

These come from reading the code. Details are in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#known-limitations).

- **One slow socket stalls every room.** The hub's send to a client channel blocks,
  and while it waits no room gets anything.
- **The hub can deadlock.** Suppose a client's write goroutine exits on a write
  error while the hub is blocked on that client's full channel. Nothing drains the
  channel, and the client's unregister waits on the same hub goroutine, so delivery
  stops until a restart. This was not seen in the data, because every step wipes
  the stack.
- **About 6 s to read a pong.** The 60 s read deadline is extended only by pongs,
  and the server pings every 54 s. The read goroutine also runs each message's
  Postgres transaction, so if it falls ~6 s behind, the server drops the socket.
- **No back-pressure or admission control.** Ingest keeps accepting while delivery
  falls behind.
- **The outbox only grows.** Dispatched rows are never deleted.
- **No authentication.** `user_id` comes from the client frame, and any origin may
  open a socket.

## Configuration

Configured via environment variables (each has a safe fallback):

| var            | default                                                        | meaning                          |
|----------------|----------------------------------------------------------------|----------------------------------|
| `HTTP_ADDR`    | `:8080`                                                        | server listen address (internal) |
| `DATABASE_URL` | `postgres://chat:chat@localhost:5432/chat?sslmode=disable`     | Postgres connection string       |

The Postgres pool size is pgx's default, `max(4, NumCPU)`, which is 16 connections
on a 16-CPU Docker engine. Add `pool_max_conns=<n>` to `DATABASE_URL` to change
it. `GOMEMLIMIT` is not set, so the Go GC does not aim for the 300 MB container
limit.

## Development

```bash
go run ./cmd/server          # run locally (needs Postgres; see docker-compose.yml)
go build ./... && go vet ./...
go test ./...                # unit tests: fast, no Docker
go test -tags=integration ./internal/integration/   # end-to-end vs a throwaway Postgres (needs Docker)
```

## Where to read more

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): the design and the reasoning
  behind the single-instance choice, the hub, the outbox, the scaling seam, and
  the known limitations.
- [`loadtest/README.md`](loadtest/README.md): how to run and read the load tests.
- [`LOAD_TEST_ANALYSIS.md`](LOAD_TEST_ANALYSIS.md): measured results, where the
  instance breaks, and what is still unproven.
