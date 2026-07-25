# ADR: Relay service — outbox drain + fan-out transport

## Context

`proper-scale` splits the naive-scale monolith into services. Today the relay is
embedded in every message-serving instance: each instance polls the shared `outbox`
table (`FOR UPDATE SKIP LOCKED`, batch 100, 2s poll) and calls `hub.Broadcast`, which
**publishes to a Redis `room:<id>` channel**. Every instance subscribes only to the
rooms it currently hosts (interest-based; `naive-scale/internal/hub/hub.go:96/108`) and
delivers to its local websocket clients.

The proposal: extract the relay into its own service whose job is to drain the outbox
and get each message to the message service.

**The framing to correct up front:** there is no single "message service" to send a
message *to*. A room's subscribers are spread across many message_service instances
(nginx distributes websocket connections). Delivery is **one-to-many fan-out**, and the
target set changes every time a client connects/disconnects. So the real question is not
"which message service does the relay call" but "what transport fans a room's message out
to exactly the instances that hold that room."

**Scale targets (assumed — labelled).** Design for large N per the project's standing
rule ("think N instances, not 3"). Single instance ceiling measured earlier: ~10–12k ws
conns, ~10k msg/s. So target envelope: tens of instances, 10^4–10^5 concurrent conns,
low-tens-of-thousands msg/s aggregate, per-room fan-out from a handful to thousands.
Payloads are small chat frames (<4 KB, capped at `maxBodyLen`).

**Ranked quality attributes** (this ranking drives the decision):
1. **Scalability / throughput** — the entire point of the exercise.
2. **Delivery correctness** — at-least-once + per-room ordering (clients de-dupe on id).
3. **Latency** — live chat; sub-second delivery wanted.
4. **Simplicity / operability** — this is a teaching progression; boring infra wins ties.
- **Transport durability is deliberately LOW**: Postgres (`messages` + `outbox`) is the
  source of truth, and a client can backfill missed messages on reconnect. The live
  transport only needs best-effort fan-out, not its own durable log.

## Options considered

**1. Extracted relay tier → Redis pub/sub (interest-based) — evolution of naive-scale.**
A small fixed set of relay replicas (2–3, for HA) drain the outbox with SKIP LOCKED and
`PUBLISH room:<id>`. message_service instances keep the *subscribe* side of today's hub
and deliver to local conns. Relay and message_service never talk directly — only through
Postgres (outbox) and Redis (channel). Scores: scalability good (relay scales with write
rate, conns scale independently); correctness good (SKIP LOCKED = at-least-once; ordering
caveat below); latency good minus poll lag; simplicity high (reuses the proven,
load-tested path). Main risk: single Redis is a fan-out chokepoint at the top of the
envelope → shard rooms across Redis nodes when it bites.

**2. Extracted relay tier → durable broker (NATS JetStream / Kafka).** Relay drains
outbox → publishes to a broker subject/topic per room (or hashed to P partitions);
message_service instances consume, each in its own consumer group so all subscribers get
every message. Adds transport durability, replay, and backpressure. Cost: real
operational weight, and topic-per-room cardinality forces a hash-to-partition scheme with
instances consuming all partitions and filtering — which reintroduces the "every instance
sees traffic it doesn't want" amplification that interest-based pub/sub avoids. Buys
durability we ranked LOW. Justified only if a second consumer appears (search/analytics/
audit) or we need at-least-once *on the transport itself*.

**3. Relay pushes directly to instances via a room→instance registry (gRPC/HTTP).** Relay
looks up which instances host room R (from presence_service or a Redis `room:R:instances`
set) and pushes to each. Targeted, no pub/sub egress. But it hand-rolls exactly what Redis
pub/sub already does for free — the subscribe table *is* the registry — and adds a
distributed registry that must stay consistent across every connect/disconnect, plus
retry/partial-failure/instance-churn handling in the relay. Strictly more moving parts for
the same result. Only worth it past the point where sharded Redis egress is still the wall.

## Decision

**Option 1.** Extract the relay into its own small replica tier; it drains the outbox and
**publishes to Redis `room:<id>`**. Keep the outbox as the correctness boundary. Keep
interest-based subscription on the message_service side. The relay does **not** call the
message service — publish/subscribe removes the routing problem entirely.

Concretely on the two sub-questions asked:
- *Is extracting the relay a good idea?* **Yes** — but as a **small fixed tier (2–3
  replicas), not one-relay-per-ws-instance.** The win is that relay work scales with
  message-write rate while connection-holding scales with conn count; coupling them (as
  naive-scale does) means every ws box also hammers the DB, and poll/SKIP-LOCKED
  contention grows with N. Splitting also gives the relay an independent failure/deploy/
  resource envelope.
- *Message queue, or something else?* **Use pub/sub semantics, not a point-to-point work
  queue.** A work queue (SQS, a classic Rabbit queue, a single Kafka consumer group)
  delivers each message to *one* consumer — wrong for fan-out. Redis pub/sub is one-to-
  many and already load-tested here. Reach for a durable broker only when a real
  requirement we ranked LOW (replay, extra consumers, transport-level at-least-once)
  actually shows up.

## Rationale

Given the ranking (scalability > correctness > latency > simplicity, durability low),
Option 1 keeps the proven path and adds only the one structural change that pays off at
large N — a dedicated, independently scaled relay tier. Option 2's durability is weight we
don't need while the DB is the source of truth and clients backfill. Option 3 rebuilds
Redis's routing by hand.

**What we trade away:** we keep a poll (up to ~2s tail latency until a message is picked
up) and we accept a single Redis as a chokepoint until it's sharded. Both are cheap to
improve later (LISTEN/NOTIFY wake; Redis-cluster room sharding) without changing the shape.

## Consequences

Easier: relay and connection tiers scale, deploy, and fail independently; the relay↔
message_service coupling is only Postgres + Redis (no RPC contract to version); the
load-tested delivery path is preserved.

Harder / to watch:
- **Per-room ordering under concurrent relays.** SKIP LOCKED across several relay replicas
  can interleave two batches of the *same* room out of id-order (naive-scale already has
  this latent risk with per-instance relays). If strict per-room order is required, shard
  the *claim* by `hash(room_id) % relayCount` so all of a room's rows go to one relay; else
  document that clients sort by (created_at, id).
- **Outbox growth.** Dispatched rows accumulate → the undispatched index bloats. Needs a
  reaper (delete/partition dispatched rows) — a task below, not an afterthought.
- **Redis fan-out egress** becomes the throughput wall near the top of the envelope; shard
  rooms across Redis nodes when it does.
- One more deployable to run, monitor, and alert on.

## Risks & de-risking

- *Ordering regression* → decide the requirement now; if strict, shard the claim by room
  and add an integration test that asserts per-room order under 2+ relay replicas.
- *Redis chokepoint* → load-test the relay tier at target msg/s against a single Redis
  first to find the real ceiling before adding sharding; keep the publish keyed by room so
  sharding is a config change, not a rewrite.
- *Poll latency* → measure end-to-end delivery p99; if the 2s tail matters, add
  LISTEN/NOTIFY to wake the relay, keeping the poll as a backstop.
- *At-least-once holds only if publish happens before the outbox row is marked dispatched*
  — preserve the naive-scale invariant (dispatch/publish inside the claim tx, mark+commit
  after) in the extracted relay.

## Revisit when

A second consumer of the message stream appears (search, analytics, audit), or transport-
level replay / durability becomes a requirement, or sharded-Redis egress is still the
measured bottleneck — any of these flips the decision toward Option 2 (NATS JetStream /
Kafka).

## Task list
<!-- Ordered; reversible/online steps first. Each task stands alone for a subagent
     with no prior context. Tests and observability are tasks, not afterthoughts. -->

- [ ] **T1 — Lock the relay↔message_service contract (docs only)**
  - What: Write a one-page contract stating: relay drains `outbox` and `PUBLISH`es the
    JSON message to Redis channel `room:<room_id>`; message_service `SUBSCRIBE`s only to
    rooms it hosts and delivers to local ws conns; the two services share *no* RPC — only
    Postgres (`outbox`) and Redis. Note the at-least-once invariant (publish inside the
    claim tx, mark dispatched + commit after) and that clients de-dupe on message id.
  - Touches: `proper-scale/docs/`.
  - Depends on: none.
  - Done when: the doc exists and names the channel format, the ownership of each table/
    channel, and the ordering decision (strict-per-room? yes/no).

- [ ] **T2 — Port the outbox store into relay_service**
  - What: Implement `storage.Store.DispatchBatch(ctx, limit, dispatch func(OutboxEvent) error)`
    in relay_service by porting `naive-scale/internal/storage/outbox.go` (claim with
    `FOR UPDATE SKIP LOCKED order by id limit $1`, run dispatch inside the tx, mark
    dispatched + commit after). Replace the current stub `outbox.go` (which references an
    unimported `models` and has an unbodied `DispatchBatch`). Add the `OutboxEvent` type and
    a `WithTx` helper on `Store`.
  - Touches: `proper-scale/services/relay_service/app/storage/{outbox.go,store.go}`, a new
    `models` package under relay_service.
  - Depends on: T1.
  - Done when: `go build ./...` passes and a unit test with a fake tx shows a full batch
    then a short batch ends the drain.

- [ ] **T3 — Add a Redis publisher to relay_service**
  - What: Add a thin publisher: `Publish(roomID string, msg models.Message)` that marshals
    the message and `PUBLISH`es to `room:<roomID>` (mirror `hub.Broadcast` in
    `naive-scale/internal/hub/hub.go`). Wire it as the `dispatch` callback passed to
    `DispatchBatch` so publish happens *inside* the claim tx.
  - Touches: `proper-scale/services/relay_service/app/` (new `broadcast`/`redis` pkg),
    `services/relay.go`.
  - Depends on: T2.
  - Done when: an integration test (Postgres + Redis via containers) inserts an outbox row,
    runs one drain, and a Redis subscriber on `room:<id>` receives the exact payload.

- [ ] **T4 — Relay run loop + wiring in main.go**
  - What: Implement `Relay.Run(ctx)` (poll ticker + drain-to-empty, log-and-continue on
    error) ported from `naive-scale/internal/outbox/relay.go`. Wire `cmd/server/main.go`:
    read `DATABASE_URL`/`REDIS_ADDR` from env, open pgx pool + redis client, construct
    Store + publisher + Relay, run until SIGTERM with graceful drain-stop.
  - Touches: `proper-scale/services/relay_service/cmd/server/main.go`, `app/config/env.go`.
  - Depends on: T3.
  - Done when: `docker compose up` starts `relay_service`; posting a message causes a
    subscribed message_service instance to deliver it end-to-end.

- [ ] **T5 — message_service keeps only the subscribe/deliver + ingest sides**
  - What: Implement message_service as: ws transport + hub *consume* side (subscribe to
    hosted rooms, deliver to local conns) + `HandleIncoming` (validate → insert message +
    enqueue outbox in one tx). Remove any relay/drain code from it — draining now belongs
    to relay_service. Reuse the naive-scale hub minus its `Broadcast`/publish path.
  - Touches: `proper-scale/services/message_service/`.
  - Depends on: T1.
  - Done when: message_service builds with no outbox-drain code; an integration test shows
    an inbound frame produces one `messages` row + one `outbox` row and nothing is
    broadcast by message_service itself.

- [ ] **T6 — Decide and enforce per-room ordering**
  - What: Per T1's decision. If strict: shard the claim by `hash(room_id) % relayCount` (or
    claim whole-room batches) so all of a room's rows go to one relay; add an integration
    test running 2 relay replicas that asserts per-room id-order at the subscriber. If not
    strict: document that clients sort by (created_at, id) and add that sort client-side.
  - Touches: relay_service storage/claim query and/or client docs; integration tests.
  - Depends on: T4.
  - Done when: the chosen guarantee has a passing test (strict) or a written contract (not).

- [ ] **T7 — Outbox reaper**
  - What: Add a periodic job (in relay_service or a small cron) that deletes or partitions
    `outbox` rows with `dispatched_at < now() - retention`. Add the retention env knob.
  - Touches: relay_service; possibly a new migration to range-partition `outbox` by time.
  - Depends on: T4.
  - Done when: a test inserts+dispatches rows, runs the reaper, and old rows are gone while
    the undispatched index stays small; retention is configurable.

- [ ] **T8 — Observability for the relay**
  - What: Structured logs with a correlation id per message; metrics for drain batch size,
    drain latency, outbox lag (`now() - oldest undispatched created_at`), publish
    errors, and events/sec. Add a readiness check (DB + Redis reachable) and liveness.
  - Touches: relay_service (metrics endpoint / logging), docker-compose healthcheck.
  - Depends on: T4.
  - Done when: `outbox lag` and `events/sec` are scrapeable and a dashboard/README shows
    how to answer "is the relay keeping up?".

- [ ] **T9 — Load test the relay tier and find the Redis ceiling**
  - What: Reuse the k6 harness pattern from `naive-scale/loadtest`. Drive target msg/s with
    2–3 relay replicas against a single Redis; record outbox lag and delivery p99 vs offered
    load; identify the point where single-Redis egress saturates. Capture a BASELINE.md.
  - Touches: `proper-scale/loadtest/` (new), results dir.
  - Depends on: T4, T8.
  - Done when: a results summary states the sustained msg/s and the measured bottleneck
    (relay CPU, DB claim contention, or Redis egress), informing whether Redis sharding is
    next.

## Diagram

Offered — say the word and I'll add a Mermaid sequence (ingest → outbox → relay drain →
Redis publish → interest-based subscribe → ws delivery) or a component topology to this
file. Not rendered by default to keep the ADR tight.
