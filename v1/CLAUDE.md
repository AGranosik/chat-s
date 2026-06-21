# chat-s / v1

Real-time chat service in Go. **Single instance, no message queue** (by design —
see the scaling seam below). WebSocket for live messages, Postgres for storage,
nginx load balancer in front.

## Start here
- **Design:** `docs/ARCHITECTURE.md`
- **Build order / current progress:** `docs/PLAN.md` — phased checklist; the
  unchecked boxes are "what's next." When resuming work, read the plan and
  continue from the first unchecked item.

## Architecture in one breath
Client ⇄ nginx (`:80`) ⇄ Go server (`:8080`). A message goes
**validate → (one tx: persist to `messages` + enqueue to `outbox`) → the relay
drains the outbox and broadcasts via the in-memory hub** to everyone in the room.
Persist and broadcast are atomic so a crash never strands a message. Live
connection state lives in one process; the hub is Go channels, not Redis/Kafka.

## Layout
```
cmd/server/main.go     wiring + graceful shutdown
internal/config/       GetEnv(key, fallback)
internal/transport/    http.go (REST), ws.go (upgrade)
internal/hub/          hub.go (Run goroutine), client.go (read/write pumps)
internal/chat/         service.go — validate→(tx: persist+enqueue); Broadcaster iface
internal/outbox/       relay.go — poll outbox; drain → Broadcaster
internal/storage/      postgres.go (pgxpool), messages.go (queries), outbox.go
internal/models/       message.go, room.go, user.go
migrations/            NNNN_*.sql (goose)
nginx/nginx.conf
```

## Conventions (match `../log-prop`)
- Package-per-concern; small focused files. Generics where they remove dupe
  (cf. log-prop `Decoder[T]`).
- Config via env with a `GetEnv(key, fallback)` fallback — never panic on a
  missing optional var.
- Standard `log` package, **lowercase** messages: `log.Printf("starting server | addr=%s", addr)`.
- Graceful shutdown with `signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)`.
- Hub broadcast sends **block** rather than drop — losing a message is worse than
  back-pressure (cf. commit "blocking consuming, not dropping messages").

## The scaling seam — keep it intact
`chat.Service` depends on the `Broadcaster` interface, not the concrete hub.
Single-instance = in-memory channel hub. Multi-instance later = Redis/Kafka impl
of the **same** interface (the `log-prop` Kafka work is the reference). Do not
build multi-node fan-out yet, but do not collapse the interface either.

## Commands
```bash
go run ./cmd/server          # run locally (needs Postgres; see docker-compose.yml)
docker compose up            # full stack: postgres + server + nginx
go build ./... && go vet ./...
go test ./...                # unit tests (fast, no Docker)
go test -tags=integration ./internal/integration/   # end-to-end vs throwaway Postgres (needs Docker)
```

Integration tests live in `internal/integration/` behind a `//go:build integration`
tag, so the default `go test ./...` stays fast and DB-free. They spin up a
`postgres:16` container via testcontainers (one per package run), then exercise
the real SQL, the transactional outbox, the polling relay, and a full ws
round-trip. If the container can't start (e.g. Docker unavailable) the suite
fails rather than skipping — integration tests are opt-in, so a failure to bring
up the container is a real failure.

## Working agreements
- Build one PLAN.md phase at a time; each ends in a Verify step — run it.
- `/verify` after a phase, `/code-review` before committing, commit per phase so
  checkboxes and git history stay in sync.
- Use `/scaffold-feature` to add an endpoint or message type so new code follows
  the layering above.
- If the design changes, edit `docs/ARCHITECTURE.md` + `docs/PLAN.md` in the same
  PR — they are the source of truth this file points to.
