# ADR: Room-affinity routing to collapse cross-instance broadcast fan-out

## Context

naive-scale (v2) runs 3 stateless Go instances behind nginx. Today nginx
**round-robins** every connection (`nginx/nginx.conf`), so the members of one room
land on *different* instances. Cross-instance delivery is bridged by Redis pub/sub with
per-room channels (`internal/hub/hub.go`): an instance subscribes to `room:<id>` while it
holds >=1 local member and unsubscribes when the last one leaves; broadcasts `PUBLISH`
to `room:<id>` and every subscribed instance re-fans-out to its local clients.

The broadcast path actually originates from the **outbox relay** (`internal/outbox/relay.go`),
not the ingesting request: a message is persisted with an outbox row in one tx, then
whichever relay claims the row (`FOR UPDATE SKIP LOCKED`, any of the 3 instances) calls
`hub.Broadcast` -> `PUBLISH`.

**The stated problem:** because members are scattered, one message costs
`1 PUBLISH` + delivery to **every instance holding a member** (up to N) + a re-fan-out on
each. As you add instances, a room's members spread across *all* of them, so the
cross-instance amplification grows ~O(N). That is the "enormous write throughput."

### Relationship to the prior v3 ADR (2026-07-08 directed routing)

The v3 ADR correctly established the **fan-out invariant**: total ws-writes =
`msg/s x members-per-message`, invariant to pub/sub topology. It then proposed a location
service with per-instance directed routing — which keeps members **scattered** and replaces
`1 PUBLISH-to-topic` with `K PUBLISH-to-instances`. Same fan-out work, arguably *more* Redis
commands. That is why v3 was judged throughput-neutral.

**This ADR is the different lever.** Room-affinity does not re-address a scattered fan-out;
it **un-scatters** the members so the fan-out becomes an in-process memory write and the
cross-instance Redis hop shrinks to ~1 per message (or 0). It does not beat the invariant —
ws-writes are still `msg x members` — but it moves that work off the network and off Redis.

| | v2 (round-robin) | v3 (directed, scattered) | This ADR (room-affinity) |
| --- | --- | --- | --- |
| Members of a room | on ~all N instances | on ~all N instances | on **1** instance |
| PUBLISH per msg | 1 (fans to <=N subs) | K (one per instance) | **1 (to 1 sub)** or 0 |
| Redis delivery per msg | up to N | up to N | **~1** or 0 |
| Fan-out location | cross-instance | cross-instance | **in-process** |
| Subscribe churn on ramp | high | none | none/low |

### Scale targets (assumed — label as such; refine with real numbers)

- Observed v2 wall (memory / prior runs): **~10-12k concurrent ws**, **~10k msg/s**.
- **Missing number that decides everything: the room-size distribution.** Broadcast cost is
  `msg/s x members`. Affinity helps most when there are *many small/medium* rooms (they
  partition cleanly across instances) and hurts when one room is larger than a single
  instance can hold (hotspot).
- **Also unconfirmed: which resource is the wall** — Redis command/delivery volume,
  gateway ws-write CPU, or the single Postgres primary. Affinity only helps if the wall is
  cross-instance fan-out / Redis. Measure before committing (T1).

### Ranked quality attributes

1. **Reduce cross-instance broadcast work** (the stated pain).
2. **Simplicity / reversibility** — a load-balancer config change beats a new stateful service.
3. Throughput ceiling.
4. Delivery correctness across scale/rebalance events (no silent drop or duplicate).

## The invariant, and what affinity actually changes

You cannot reduce `ws-writes = msg x members` — that is physics of a broadcast. What you
*can* reduce is **where** that fan-out happens and **how much it is amplified across the
network**:

- Round-robin: fan-out is cross-instance, amplified up to N-fold on Redis.
- Affinity: fan-out is a local channel send (`c.send <- payload` in `hub.Run`), Redis
  amplification -> 1 (bridge) or 0 (if the relay that broadcasts also owns the room).

So affinity attacks the *amplification*, not the invariant.

## Options considered

1. **Measure the wall first (baseline / prerequisite).** Instrument Redis command +
   delivery rate and CPU, gateway ws-write rate + CPU, and Postgres write/lock stats; rerun
   the existing sweep. Confirms the wall is cross-instance fan-out (not ws-write CPU or the
   DB primary) before changing routing. Cheap, reversible, decides whether 2/3 can help.
   - Scores: insight *****; risk none. (Same T1/T2 posture as the v3 ADR — still unmet.)

2. **Room-affinity at nginx + Redis as a now-1:1 bridge (recommended core).** Change the
   `/ws` upstream to `hash $arg_room consistent;` so all members of `?room=<id>` land on one
   instance. Fan-out becomes in-process. Redis stays as the relay->owner bridge, but now
   `PUBLISH room:<id>` is delivered to exactly **1** subscriber instead of up to N. Keep
   round-robin for stateless REST.
   - Scores: cross-instance work *****; simplicity ***** (config + no app change for the
     core win); risk: **hot-room hotspot** and **rebalance reconnect** on scale up/down.

3. **Room-affinity + partition the outbox/relay by room ownership (follow-on).** In addition
   to 2, each relay only claims outbox rows for rooms it owns (consistent-hash `room->instance`
   in-process), so the owner broadcasts **locally with no Redis at all**. Removes the last
   cross-instance hop and the shared-outbox lock contention.
   - Scores: cross-instance work ***** (Redis out of the broadcast path); risk: couples
     relay ownership to routing, needs an instance-membership view, and ownership must move
     correctly on rebalance. Do only if the 1:1 Redis bridge or shared-outbox polling is the
     *next* measured wall.

Rejected for throughput: the v3 location service (directed routing over scattered members) —
neutral by the invariant, already ADR'd. Its `user->instance` directory is still the right
tool for **user-directed** features (DMs, presence), just not for the broadcast ceiling.

## Decision

**Do Option 1 (measure), then Option 2 (nginx room-affinity + 1:1 Redis bridge).** Treat
Option 3 as the follow-on, gated on Option 2's post-change measurement showing the Redis
bridge or the shared outbox as the next wall.

Frame the win precisely: affinity collapses **cross-instance amplification** (O(N) ->
O(1)) and moves fan-out in-process. It does **not** raise the raw `msg x members` ceiling and
does **not** relieve the single Postgres primary — those are separate axes (see Risks).

## Rationale

The stated pain is specifically that members are scattered, which is a *routing* choice
(round-robin), not a law. Fixing the routing is a near-config-only change with the largest
reduction in cross-instance work, and unlike v3 it genuinely removes work rather than
re-addressing it. We measure first because affinity only pays off if the wall is Redis /
cross-instance fan-out; if the wall is ws-write CPU or the DB primary, affinity won't move it
and we'd chase the wrong lever.

**What we trade away:** even load spreading. Round-robin gives uniform per-instance load;
affinity makes per-instance load a function of which rooms hash there, so a hot/large room
becomes a hotspot and scale-up reshuffles ~1/N of rooms (a reconnect blip). We accept that
for the broadcast-work win, and bound it (T5, T6).

## Consequences

Positive:
- Per-message Redis fan-out drops from up to N to ~1 (Option 2) or 0 (Option 3).
- Broadcast fan-out becomes an in-memory channel send; no subscribe/unsubscribe churn on the
  broadcast path (each owner subscribes to few rooms, or none in Option 3).
- Adding instances now *partitions* rooms instead of *multiplying* delivery.

Negative / harder:
- **Hotspot:** a room larger than one instance's budget (200 MB) concentrates there and can
  OOM-restart that instance. Round-robin never had this. Needs a policy (T5).
- **Rebalance:** scale up/down remaps ~1/N of rooms; those ws connections drop and reconnect
  to the new owner. Consistent hashing minimizes the set; clients must reconnect cleanly.
- **REST vs WS routing split:** REST has no `?room`, so it must stay round-robin (T3).
- **Does not fix the DB primary:** every message is still `INSERT message + INSERT outbox
  (+ UPDATE dispatch)` against one Postgres. If ingest is the wall, that's a separate shard/
  batch effort (noted, T7).

## Risks & de-risking

- **Optimizing blind** -> T1/T2 confirm the wall is cross-instance fan-out before the change.
- **Hot-room hotspot** -> T5: cap room size, or let a big room span a *bounded* subset of
  instances (secondary shard), or pin big rooms to dedicated instances. Measure per-instance
  memory under a skewed room-size mix.
- **Rebalance reconnect storm** -> `hash ... consistent` (ketama) remaps only ~1/N; verify the
  client reconnects and resumes; consider draining rather than hard-cut on scale-down.
- **Redis bridge still 1:1 per message** -> if that or shared-outbox polling is the next wall,
  do Option 3 (partition the relay by room ownership; local broadcast, no Redis).
- **Affinity doesn't move the ceiling** -> possible if the wall was ws-write CPU or Postgres;
  the A/B in T6 makes that a *result*, not a surprise, and points at the real lever.

## Revisit when

- Measurement (T1/T2) shows the wall is **not** cross-instance fan-out (then affinity is the
  wrong lever — pursue ws-write CPU headroom or Postgres sharding instead), OR
- Post-change measurement shows the 1:1 Redis bridge or shared-outbox polling as the next wall
  (then do Option 3), OR
- A user-directed feature (DMs, @mentions, presence) is prioritized (then the v3
  `user->instance` directory becomes necessary alongside affinity).

## Task list

<!-- Ordered; measure/reversible steps first. Each task stands alone for a subagent with no
     prior context. Tests and instrumentation are tasks, not afterthoughts. -->

- [ ] **T1 — Instrument the broadcast bottleneck**
  - What: add metrics for Redis command rate + pubsub delivery + CPU (`redis-cli INFO`,
    `PUBSUB CHANNELS`), gateway ws-write rate + goroutine/CPU per instance, and Postgres
    write/commit + lock-wait stats. Expose a `/metrics` endpoint on the server.
  - Touches: `internal/hub/hub.go`, `cmd/server/main.go`, `docker-compose.yml` (expose stats).
  - Depends on: none.
  - Done when: a load run yields a time series that attributes the ceiling to one resource
    (Redis vs ws-write CPU vs Postgres).

- [ ] **T2 — Rerun the sweep and classify the wall (go/no-go)**
  - What: run `loadtest/run-limits.ps1` against instrumented v2; use the `loadtest-analysis`
    skill to separate real limits from the known k6 shutdown artifact. Record the verdict in
    this file.
  - Touches: `loadtest/`, `loadtest/results/`.
  - Depends on: T1.
  - Done when: the wall is attributed with evidence. **If it is not cross-instance fan-out /
    Redis, stop here** and open a separate plan for that resource.

- [ ] **T3 — Room-affinity routing at nginx**
  - What: split routing — keep round-robin for REST, route `/ws` by room. Add a second
    upstream using `hash $arg_room consistent;` over `server1..3`, and a `location /ws` that
    `proxy_pass`es to it (preserve the existing Upgrade/Connection/timeouts). Document the
    empty-`$arg_room` case (all bucket to one node — only affects malformed ws URLs).
  - Touches: `nginx/nginx.conf`.
  - Depends on: T2 (go).
  - Done when: repeated `/ws?room=R` connections from different clients all reach the **same**
    instance; REST still round-robins; a room with all members colocated broadcasts with a
    Redis delivery count of ~1 (verify via T1 metrics).

- [ ] **T4 — Confirm/keep the 1:1 Redis bridge correctness**
  - What: verify the existing per-room pub/sub still delivers correctly when a room has exactly
    one owning instance (subscriber count 1) and when a room *transiently* spans two instances
    during a rebalance (no drop, no duplicate). No app change expected; add an integration test
    for the single-owner and the two-owner-transition cases.
  - Touches: `internal/integration/ws_test.go` (or a new test), `internal/hub/hub_test.go`.
  - Depends on: T3.
  - Done when: integration tests pass for single-owner delivery and for a member migrating
    between instances without message loss or duplication.

- [ ] **T5 — Hot-room / hotspot policy**
  - What: decide and implement the guard for a room bigger than one instance can hold. Minimum:
    a configurable max room size enforced at ws upgrade (`internal/transport/ws.go`) returning a
    clear close code. Document the escalation options (bounded multi-instance spread; dedicated
    big-room instances) without building them yet.
  - Touches: `internal/transport/ws.go`, `internal/config/env.go`, README.
  - Depends on: T3.
  - Done when: connecting past the cap is rejected with a clear reason; a skewed room-size load
    run shows no 200 MB instance OOM-restart within the configured cap.

- [ ] **T6 — A/B load test: round-robin vs room-affinity**
  - What: run the same sweep against round-robin (baseline) and affinity with T1 instrumentation;
    compare ceiling, Redis command + delivery rate, ws-write CPU, and per-instance memory skew.
  - Touches: `loadtest/`, `loadtest/results/`, `loadtest/BASELINE.md`.
  - Depends on: T4, T5.
  - Done when: this ADR is updated with the measured delta and a verdict on whether affinity
    moved the ceiling and by how much it cut Redis fan-out.

- [ ] **T7 — (Follow-on, gated) Partition the outbox/relay by room ownership**
  - What: only if T6 shows the 1:1 Redis bridge or shared-outbox polling as the next wall —
    have each relay claim only outbox rows for rooms it owns (consistent-hash `room->instance`
    from an instance-membership view) and broadcast locally with no Redis. Keep the Redis bridge
    behind a flag as fallback and for rebalance-window correctness.
  - Touches: `internal/outbox/relay.go`, `internal/storage/outbox.go`, a small ownership helper,
    `cmd/server/main.go`.
  - Depends on: T6 (gate).
  - Done when: with affinity, a message reaches all room members via purely local fan-out (Redis
    command rate ~0 on the broadcast path) with no loss/duplication, including across a rebalance
    (chaos test).

## Diagram

Before (round-robin) vs after (room-affinity), one message to a 3-member room:

```
BEFORE — members scattered across all instances
  member ws:   n1        n2        n3        (room R spread over 3 nodes)
  relay claims outbox row (any node) --> PUBLISH room:R
                         |
                   Redis fans to 3 subscribers  (delivery x3)
                   /     |      \
                 n1     n2      n3
                 ws     ws      ws            (3 ws-writes, each after a Redis hop)

AFTER — members colocated by hash(room) at nginx
  nginx:  hash $arg_room consistent  ==> all of room R -> n2
  member ws:            n2                       (room R lives on 1 node)
  relay claims outbox row --> PUBLISH room:R
                         |
                   Redis delivers to 1 subscriber (n2)   (delivery x1)
                         |
                        n2 fans out IN-PROCESS:  c.send<-payload x3
                         |
                     3 ws-writes                 (no per-write network hop)

  Option 3 (T7): the relay on n2 owns room R's outbox rows -> broadcasts locally,
  Redis delivery x0.
```

## References
- Routing: `nginx/nginx.conf` (currently round-robin `chat_backend`).
- Broadcast: `internal/hub/hub.go` (per-room subscribe/unsubscribe, `PUBLISH room:<id>`).
- Origin of broadcast: `internal/outbox/relay.go` (`DispatchBatch`, SKIP LOCKED).
- Prior ADR (directed routing, throughput-neutral): `docs/plans/2026-07-08-v3-location-service-routing.md`.
- Load tooling + baseline: `loadtest/`, `loadtest/BASELINE.md`, skill `loadtest-analysis`.
- Memory: k6 handshake failures are a shutdown artifact; ramp-config notes.
