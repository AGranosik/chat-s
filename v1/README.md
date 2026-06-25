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
network and cannot be hit directly. Check it's alive:

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

## Finding the limit — load testing

This is the main event. A [k6](https://k6.io) WebSocket load test lives in
[`loadtest/`](loadtest/README.md) and drives the stack the way a real client
would — **through nginx**. One virtual user = one user = one long-lived socket.
Connections **ramp up gradually** before holding at full load, because opening
thousands of sockets on the same tick swamps the accept backlog and masquerades as
a server ceiling.

```powershell
# recreate a clean stack and run a small smoke scenario (10 rooms x 5 users)
./loadtest/run-clean.ps1

# larger preset cells (100 and 300 rooms x 10 users)
./loadtest/run-clean.ps1 -Full

# a specific cell — e.g. push to 10,000 concurrent sockets
./loadtest/run-clean.ps1 -Rooms 100 -Users 100
```

The scenario matrix scales rooms × users/room from a handful up to **10,000
concurrent sockets**. Each run reports handshake time, end-to-end message latency,
throughput, and errors, and writes a JSON summary to `loadtest/results/`. See
[`loadtest/README.md`](loadtest/README.md) for the full matrix, parameters, and the
OS file-descriptor tuning the bigger cells need.

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
- [`docs/PLAN.md`](docs/PLAN.md) — the phased build plan and current progress.
- [`loadtest/README.md`](loadtest/README.md) — how to run and read the load tests.
