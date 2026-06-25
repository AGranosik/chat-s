# Architecture — chat-s (v1)

Real-time chat service. **Single instance** — no message queue, no
cross-node fan-out. The design keeps a clean seam so we can scale out later
without rewriting the core.

## High-level

```
                       ┌────────────────────────┐
   browsers / clients  │   Load balancer (nginx) │  TLS termination,
   ───────────────────▶│   :80 / :443            │  ws upgrade pass-through,
        WebSocket +    │                         │  single upstream (for now)
        REST           └───────────┬─────────────┘
                                   │ proxy_pass
                                   ▼
                       ┌──────────────────────────┐
                       │   Go server (1 replica)   │
                       │                           │
                       │  transport ── REST + ws   │
                       │      │                    │  validate → persist msg
                       │   chat.Service ───────────────┐  + enqueue outbox (1 tx)
                       │      │                    │   ▼
                       │   storage (pgx)           │ ┌────────────┐
                       │      ▲  │ drain outbox    │ │  Postgres  │ messages, rooms,
                       │      │  ▼                 │ │            │ users, OUTBOX
                       │   relay ──▶ hub           │ └─────┬──────┘
                       │           (in-mem chans)  │       │ poll outbox
                       │              │            │◀──────┘ (~2s interval)
                       └──────────────┼────────────┘
                                      │ broadcast to room members
                                      ▼
                               connected clients
```

**nginx is the only ingress.** The Go server listens on `:8080`, but that port is
**not** published to the host — in `docker-compose.yml` the service uses `expose`,
not `ports`, so it is reachable only by nginx over the internal compose network.
Every client *and* the load test goes through the load balancer on `:80`; there is
no way to bypass it and hit the server directly. This keeps the deployment honest
about its single public entry point and makes the future multi-upstream change a
pure nginx edit.

## Request / message flow

1. **Connect** — client calls `GET /ws?room=<id>`. The transport layer upgrades
   the connection and registers a `*Client` with the `hub` for that room.
2. **Send** — client writes a message frame (JSON) over the socket. The read
   pump hands it to `chat.Service`, which, **in a single Postgres transaction**:
   - validates the payload,
   - inserts it into the `messages` table,
   - inserts a corresponding event into the `outbox` table,
   - commits — so the message and the intent to broadcast land atomically (or
     not at all). `chat.Service` does **not** call the hub directly.
3. **Relay → fan-out** — the outbox relay drains undispatched `outbox` rows in
   order and calls `Broadcaster.Broadcast(roomID, msg)` (the in-memory hub
   today). The hub pushes the message onto every registered client's send
   channel; each client's write pump flushes it to the socket. The relay marks
   the row dispatched only after the broadcast hand-off succeeds. It discovers
   rows by polling the outbox on a fixed interval (~2s) — no database-specific
   signalling, so the same loop works against any storage engine and doubles as
   the crash-recovery path.
4. **History** — `GET /api/rooms/{id}/messages?before=<cursor>` reads from
   Postgres (keyset pagination), independent of the live socket.

## The hub (why channels, why in-memory)

Live connection state lives in one process, so the hub is just Go data
structures guarded by channels — the same pattern explored in `../log-prop`
(register / unregister / broadcast channels, blocking sends so we never silently
drop messages). No Redis, no Kafka, no locks-everywhere.

```
hub
 ├─ register   chan *Client
 ├─ unregister chan *Client
 ├─ broadcast  chan envelope          // {roomID, payload}
 └─ rooms      map[roomID]map[*Client]struct{}   // owned by the hub goroutine
```

A single `hub.Run()` goroutine owns the `rooms` map and selects over the
channels, so there is no shared-memory contention.

## The outbox (why persist and broadcast must be atomic)

Without an outbox, `persist → broadcast` is a dual write: two independent steps
with a gap between them. If the process dies (or the broadcast errors) after the
Postgres commit but before fan-out, the message is durably stored yet never
reaches the live clients in the room — they'd only see it on the next history
fetch or reconnect. That is exactly the "silently drop a message" failure the
hub's blocking sends were chosen to avoid, just moved one step earlier.

The transactional outbox closes the gap:

```
outbox
 ├─ id            bigserial primary key
 ├─ room_id       text/uuid          // partition + ordering key
 ├─ payload       jsonb              // the message event to broadcast
 ├─ created_at    timestamptz default now()
 └─ dispatched_at timestamptz        // NULL until the relay fans it out
                                     // index (dispatched_at, id) for the drain query
```

- **Write side** — `chat.Service` inserts the `messages` row and the `outbox`
  row in the *same* transaction. Either both commit or neither does; there is no
  in-between state.
- **Read side (relay)** — a single relay goroutine (`internal/outbox/relay.go`)
  selects undispatched rows ordered by `id`, calls the `Broadcaster`, then sets
  `dispatched_at`. Ordering by `id` preserves per-room message order. The relay
  is the **only** caller of `Broadcaster`.
- **Latency vs. simplicity** — the relay polls for undispatched rows on a fixed
  interval (every ~2s) rather than relying on `pg_notify`/`LISTEN`. This trades a
  little latency for a design with no database-specific signalling: the write
  side only has to persist the message + outbox row, and the same poll loop
  re-scans after a crash, so a message is never stranded.
- **At-least-once** — if the relay crashes after broadcast but before marking the
  row, it re-dispatches on restart. The hub fan-out is idempotent enough for
  chat (clients can de-dupe on message `id` if needed); we do not need exactly-once.

This keeps the project's single-instance promise — the relay drains to the
in-memory hub, no external broker is introduced — while making the core flow
crash-safe and setting up the multi-node swap below.

## Data model (initial)

| table      | key columns                                            |
|------------|--------------------------------------------------------|
| `users`    | `id`, `username`, `created_at`                         |
| `rooms`    | `id`, `name`, `created_at`                              |
| `messages` | `id`, `room_id`→rooms, `user_id`→users, `body`, `created_at` |
| `outbox`   | `id`, `room_id`, `payload` (jsonb), `created_at`, `dispatched_at` |

`messages` is indexed on `(room_id, created_at desc, id desc)` for history
pagination. `outbox` is indexed on `(dispatched_at, id)` for the relay's drain
query (undispatched rows, in order). The message insert and the outbox insert
happen in one transaction — see "The outbox" above.

