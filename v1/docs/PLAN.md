# Implementation plan — chat-s (v1)

A phased, resumable build plan. Each phase is independently shippable and ends
in a **verifiable** state. Check items off as you go; the checkboxes are the
source of truth for "where are we."

See `ARCHITECTURE.md` for the design this plan implements. Scope: **single
instance, no queue.** Anything multi-node is explicitly out of scope (the
`Broadcaster` seam is built, but only the in-memory impl).

---

## Phase 0 — Project skeleton
- [x] `go mod init chat-s` (module `chat-s`)
- [x] Create the directory layout from ARCHITECTURE.md (`cmd/`, `internal/...`)
- [x] `internal/config/env.go` — `GetEnv(key, fallback string) string` (copy idiom from `../log-prop`)
- [x] `cmd/server/main.go` — boots an HTTP server on `:8080`, `GET /healthz` → `200 ok`, graceful shutdown via `signal.NotifyContext`
- [x] **Verify:** `go run ./cmd/server` then `curl localhost:8080/healthz`

## Phase 1 — Postgres + schema
- [x] Add `docker-compose.yml` with a `postgres:16` service (+ healthcheck), mirroring the `log-prop` compose style
- [x] `migrations/0001_init.sql` — `users`, `rooms`, `messages` tables + index `(room_id, created_at desc, id desc)`; `outbox` table (`id`, `room_id`, `payload` jsonb, `created_at`, `dispatched_at`) + index `(dispatched_at, id)`
- [x] `internal/storage/postgres.go` — `pgxpool` connect from env (`DATABASE_URL`), run goose migrations on boot
- [x] `internal/models/` — `Message`, `Room`, `User`
- [x] **Verify:** server boots, applies migrations, `\dt` shows the three tables

## Phase 2 — Message storage
- [x] `internal/storage/messages.go` — `Insert(ctx, tx, msg)` (tx-aware, so it can share a transaction with the outbox) and `History(ctx, roomID, before, limit)` (keyset pagination)
- [x] `internal/storage/outbox.go` — `Enqueue(ctx, tx, event)` (tx-aware), `FetchUndispatched(ctx, limit)`, `MarkDispatched(ctx, ids)`
- [x] REST: `GET /api/rooms/{id}/messages` returns history as JSON
- [x] REST: `POST /api/rooms` / `GET /api/rooms` (minimal room CRUD)
- [x] **Verify:** insert a row by hand, fetch it through the REST endpoint

## Phase 3 — WebSocket hub (the core)
- [x] Add `github.com/gorilla/websocket`
- [x] `internal/hub/hub.go` — `Run()` goroutine owning `rooms map`, `register`/`unregister`/`broadcast` channels; **blocking** broadcast sends (don't drop — same lesson as `log-prop` commit "blocking consuming, not dropping messages")
- [x] `internal/hub/client.go` — read pump + write pump, ping/pong keepalive, buffered send channel
- [x] Define `Broadcaster` interface in `internal/chat`; hub implements it
- [x] `internal/transport/ws.go` — `GET /ws?room=<id>` upgrade → register client
- [x] **Verify:** open two `websocat` clients on the same room; a message from one appears on the other

## Phase 4 — Wire send → persist + enqueue (transactional)
- [x] `internal/chat/service.go` — `HandleIncoming`: validate → **begin tx** → `storage.Insert(msg)` → `storage.Enqueue(outbox event)` → **commit**. No direct `hub.Broadcast` (the relay owns fan-out — see Phase 5)
- [x] Read pump routes inbound frames through `chat.Service`
- [ ] On connect, optionally replay last N messages from history (deferred — clients can use the history REST endpoint)
- [x] **Verify:** a sent message and its outbox row commit together — kill the process between commit and fan-out and confirm the message is still in `messages` **and** in `outbox` as undispatched

## Phase 5 — Outbox relay (reliable broadcast)
- [x] `internal/outbox/relay.go` — single relay goroutine: polls the outbox on a fixed interval (engine-agnostic, no `LISTEN/NOTIFY`); drains `FetchUndispatched` in `id` order, calls `Broadcaster.Broadcast`, then `MarkDispatched`
- [x] `chat.Service` just persists message + outbox row in one tx; the relay picks rows up on its next poll (no commit-time signalling)
- [x] Wire the relay in `cmd/server/main.go` (start with the hub, stop on graceful shutdown)
- [x] At-least-once: relay re-dispatches rows left unmarked after a crash; document that clients de-dupe on message `id`
- [x] **Verify:** message sent over ws is received by peers **and** present via history; restart the process with undispatched outbox rows present and confirm they fan out on boot

## Phase 6 — Load balancer + containerization
- [x] `Dockerfile` (multi-stage build, matches `log-prop/consumer/Dockerfile`)
- [x] `nginx/nginx.conf` — proxy `:80` → server `:8080`, with `Upgrade`/`Connection` headers for ws, single upstream
- [x] Add `server` + `nginx` services to `docker-compose.yml`
- [x] **Verify:** `docker compose up`; connect through nginx (`:80`), chat works end-to-end

## Phase 7 — Hardening (pick as needed)
- [ ] Origin check / auth on ws upgrade (currently open)
- [ ] Per-connection rate limiting and max message size
- [ ] Structured logging + request IDs
- [ ] Graceful drain: stop accepting, flush write pumps on shutdown
- [x] Integration test: spin up Postgres (testcontainers), drive a ws round-trip
  (`internal/integration/`, `//go:build integration`; covers real SQL, the
  transactional outbox, the polling relay, the full ws
  fan-out, history REST, and at-least-once recovery of an undispatched row)

---

## Out of scope (the next milestone, not this one)
- Multi-instance fan-out (Redis Pub/Sub or Kafka-backed `Broadcaster`)
- Sticky sessions / shared session store
- Presence, typing indicators, read receipts
- These reuse the `Broadcaster` seam — see ARCHITECTURE.md "scaling seam". The
  Phase 5 outbox relay is the reliable-publish primitive that milestone needs:
  later, only the relay's `Broadcaster` target changes (hub → bus producer); the
  transactional write side stays as-is.

---

## How to use this plan in future sessions

This file is the contract between you and Claude across sessions. Workflow:

1. **Resume:** start a session and say
   *"Read `v1/docs/PLAN.md` and continue with the first unchecked phase."*
   Claude reads the checkboxes to find where you left off — no need to re-explain.
2. **Build one phase at a time.** Each phase ends in a Verify step; run it before
   moving on. Ask Claude to *"do Phase N and stop at the verify step."*
3. **Check items off** (you or Claude) as they land, and commit per phase so the
   checkbox state and git history agree.
4. **Verify the work:** use `/verify` after a phase to confirm it runs, and
   `/code-review` before committing.
5. **Scaffold consistently:** use the project skill `/scaffold-feature` (see
   `.claude/skills/`) to add a new message type or endpoint without drifting from
   the layering in ARCHITECTURE.md.
6. **Amend the plan, don't abandon it.** If the design changes, edit this file
   in the same PR so it stays the single source of truth. `CLAUDE.md` points
   here, so every future session inherits the update.
