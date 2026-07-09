# ADR: v3 — per-instance directed routing via a location service

## Context

naive-scale is an experiment series finding the scaling limits of a room-based chat
system. Current state:

- **v1**: single instance, local fan-out.
- **v2 (current)**: 3 instances @ 200m behind nginx, Redis pub/sub with **per-room
  channels** (`room:<id>`). An instance subscribes to a room's channel only while it
  holds >=1 local member (`internal/hub/hub.go`), and unsubscribes when the last member
  leaves. Publishing a message goes to `room:<id>`; Redis delivers to exactly the
  instances that hold a member. Observed limit: ~10-12k ws connections, ~10k msg/s
  (commit 4c6f20d).

**Proposed v3**: add a **location service** — a presence registry mapping
`user -> instance`. Each service subscribes to a **single per-instance channel**
(`instance:<id>`). To deliver a message, the sender looks up recipients' instances and
publishes to those instances' channels (directed routing) instead of a room topic.

**Goals stated by the user:**
1. Raise the room-broadcast ceiling.
2. Explore the directed-routing topology (learning, not just throughput).

**Channel model chosen:** per-instance channel (directed), NOT one global firehose.

### Scale targets (assumed — label as such, refine with real numbers)
- Concurrent ws connections: target beyond the ~10-12k observed wall.
- Message rate: beyond ~10k msg/s.
- Fan-out: room size distribution unknown — **this is the missing number that matters
  most.** Broadcast cost is `msg/s x members-per-message`; without the room-size
  distribution we cannot predict the ceiling.

### Ranked quality attributes
1. **Understanding the bottleneck** (this is an experiment — insight > raw numbers).
2. Simplicity / reversibility (don't ossify a wrong guess).
3. Throughput (the stated aim, but constrained by physics below).
4. Reliability of delivery (must not silently drop messages).

## The invariant that drives the decision

Broadcasting to a room costs `msg/s x members-per-message` websocket writes and the
same number of Redis deliveries. **This total is invariant to pub/sub topology.** v2
(topic) and v3 (directed) perform identical fan-out work; directed routing only changes
how delivery is *addressed*, not how much occurs. Consequently:

- Wall = gateway ws-write CPU  -> v3 does not help (same writes).
- Wall = Redis delivery volume -> v3 does not help; it is arguably worse (1 `PUBLISH`
  fanning to K subscribers becomes K `PUBLISH`es unless pipelined).
- Wall = subscribe/unsubscribe churn on the pubsub control plane during ramp -> **v3
  fixes this**: each instance subscribes once to `instance:<self>` and never churns.

**Corollary:** for *room broadcast*, the only routing state needed is
`room -> instances-with-a-member`, which is exactly what Redis pub/sub subscriber lists
already encode implicitly in v2. Maintaining it yourself is a re-implementation whose
sole broadcast-path benefit is removing subscribe churn. The full `user -> instance`
directory is only *required* when the routing key is a user (DMs, presence) — which is
out of scope for the stated goals.

## Options considered

1. **Measure v2's real ceiling first (baseline).** Instrument gateway CPU / ws-write
   rate, Redis CPU + command rate + pubsub delivery, and subscribe/unsubscribe churn.
   Confirm which resource is the wall (memory already suspects a k6 shutdown artifact,
   not a server limit). Cheap, reversible, decides whether v3 can help at all.
   - Scores: insight ***** ; throughput (indirect) ; risk *none*.

2. **Per-instance directed routing + location service (the proposal).** Instances
   subscribe once to `instance:<self>`. Maintain presence (`user -> instance`, TTL +
   heartbeat) and room membership; send path enumerates recipients, groups by instance,
   pipelines one `PUBLISH` per target instance. Enables DMs/presence later.
   - Scores: topology-learning ***** ; throughput ~neutral (see invariant) ; risk:
     new failure modes (stale presence, reconnect race, send-path dependency), and a
     per-message N+1 against Redis if membership isn't pre-aggregated.

3. **Hybrid.** Keep v2 per-room pub/sub for broadcast; add a lightweight presence
   registry *only* for future user-directed (DM) features. Doesn't touch the working
   broadcast path.
   - Scores: simplicity ***** ; throughput neutral ; best if the real future need is
     DMs rather than a higher broadcast ceiling.

## Decision

**Do Option 1 first (measure), then build Option 2 as a scoped experiment** — with the
explicit hypothesis that it moves the ceiling *only if* subscribe churn is the wall.
Frame v3's value as topology learning + churn removal + a foundation for DM/presence,
**not** as a room-broadcast throughput win.

If measurement shows the wall is ws-write CPU or Redis delivery volume, **stop** — v3 is
the wrong lever; pursue room sharding / more instances / smaller fan-out instead.

### Critical design refinement for Option 2

Do **not** do a per-user location lookup on the broadcast hot path — that is a
per-message N+1 (`SMEMBERS room` + N x `GET presence`). Instead maintain
`room:<id>:instances` as a **counted set** (instance -> refcount of local members),
updated on join/leave. Broadcast becomes: read the instance set -> pipelined `PUBLISH`
per instance. That collapses per-message cost from O(members) to O(instances) and keeps
the full `user -> instance` directory only for genuine user-directed messages.

## Rationale

The stated primary aim (raise the broadcast ceiling) is blocked by the fan-out
invariant, so the honest first move is to locate the real bottleneck rather than change
addressing. The secondary aim (explore the topology) is real and worth doing, but should
be built minimally and instrumented as an A/B against v2 so the experiment yields a
clean answer to "did removing subscribe churn move the wall?".

**What we trade away by choosing to measure first:** a little time before writing the
fun new service. **What we trade away if/when we build Option 2:** simplicity and a
proven delivery path, in exchange for owning consistency of presence + membership state
and new failure modes.

## Consequences

Positive:
- A defensible answer to "what actually limits v2" before spending build effort.
- If built, a stable single subscription per instance (no churn) and a presence
  primitive that unlocks DMs / online indicators later.

Negative / harder:
- Option 2 adds a send-path dependency, staleness/reconnect races, and explicit
  membership state to keep consistent.
- Room broadcast gains no throughput unless churn was the wall; possibly more Redis
  commands.

## Risks & de-risking

- **Optimizing blind** -> de-risk with T1/T2 (instrument + rerun) before any topology
  change.
- **Stale presence on crash** -> TTL + heartbeat; treat missing presence as offline.
- **Reconnect-to-different-instance race** (drop/dup) -> session token + last-writer-wins
  on the presence key.
- **Send-path N+1 on Redis** -> pre-aggregate `room:<id>:instances` (design refinement).
- **v3 doesn't move the ceiling** -> expected outcome unless churn-bound; the A/B load
  test (T7) makes that a *result*, not a surprise.

## Revisit when

- Measurement shows subscribe churn is the dominant cost (then Option 2 has a real
  throughput case), OR
- A user-directed feature (DMs, @mentions, presence UI) is prioritized (then Option 2 /
  Option 3's directory becomes *necessary*, not optional).

## Task list

- [ ] **T1 — Instrument v2 bottleneck**
  - What: add metrics for gateway ws-write rate + goroutine/CPU, Redis command rate +
    pubsub channel count + CPU (`redis-cli INFO`, `PUBSUB CHANNELS`), and a counter for
    SUBSCRIBE/UNSUBSCRIBE calls per second in `internal/hub/hub.go`.
  - Touches: `internal/hub/hub.go`, `cmd/server/main.go` (metrics endpoint), docker-compose (expose Redis stats).
  - Depends on: none.
  - Done when: a load run produces a time series showing, at the ceiling, which resource
    saturates (gateway CPU vs Redis CPU vs subscribe-churn rate).

- [ ] **T2 — Re-run the load sweep with instrumentation and classify the wall**
  - What: run the existing sweep (`loadtest/run-*.ps1`) against instrumented v2; use the
    `loadtest-analysis` skill to separate real server limits from the known k6 shutdown
    artifact.
  - Touches: `loadtest/`, `loadtest/results/`.
  - Depends on: T1.
  - Done when: the wall is attributed to one resource with evidence; a go/no-go on v3 is
    recorded in this file. **If wall is ws-write or Redis delivery -> stop here.**

- [ ] **T3 — Per-instance subscription (no directory yet)**
  - What: add each instance subscribing once at startup to `instance:<selfID>` (config or
    hostname-derived ID); keep v2 per-room publish path running in parallel behind a flag.
  - Touches: `internal/hub/hub.go`, `cmd/server/main.go`.
  - Depends on: T2 (only if go).
  - Done when: instance receives on its own channel; existing room broadcast still works;
    no subscribe/unsubscribe churn on the new path.

- [ ] **T4 — Room->instances counted set**
  - What: maintain `room:<id>:instances` in Redis as instance -> local-member refcount;
    increment on register, decrement on unregister, remove instance at zero.
  - Touches: `internal/hub/hub.go`, a small `internal/presence` package.
  - Depends on: T3.
  - Done when: the set accurately reflects which instances hold a member of each room
    under connect/disconnect churn (integration test).

- [ ] **T5 — Directed broadcast path**
  - What: on publish, read `room:<id>:instances` and pipeline one `PUBLISH instance:<i>
    {roomID,payload}` per instance; consumer maps `roomID` -> local clients. Gate behind
    the flag from T3 so v2 remains the fallback.
  - Touches: `internal/hub/hub.go`.
  - Depends on: T4.
  - Done when: a message reaches exactly the local + remote members of the room via the
    directed path, with no duplication, verified by integration test.

- [ ] **T6 — Presence lifecycle hardening**
  - What: TTL + heartbeat on presence/room-instance keys; session token for
    last-writer-wins on reconnect; treat missing entry as offline; timeout on the
    send-path Redis read with a logged fallback.
  - Touches: `internal/presence`, `internal/hub/hub.go`.
  - Depends on: T5.
  - Done when: killing an instance ungracefully drains its entries within the TTL and a
    reconnect to a different instance does not drop or duplicate messages (chaos test).

- [ ] **T7 — A/B load test v2 vs v3**
  - What: run the same sweep against v2 (topic) and v3 (directed) with T1 instrumentation;
    compare ceiling, Redis command rate, and churn.
  - Touches: `loadtest/`, `loadtest/results/`, `loadtest/BASELINE.md`.
  - Depends on: T6.
  - Done when: this ADR is updated with the measured delta and a verdict on whether
    directed routing moved the ceiling.

## Diagram

(Offered — say the word for a Mermaid version.) ASCII of the directed broadcast path:

```
  sender (any instance)
        |
        |  1. read room:<id>:instances  (counted set)
        v
   Redis  --{node-2, node-7}--
        |            |
        | 2. pipelined PUBLISH per instance
        v            v
  PUBLISH        PUBLISH
  instance:node-2  instance:node-7
        |            |
        v            v
   node-2         node-7        (each subscribes ONCE to instance:<self>)
   map roomID->   map roomID->
   local clients  local clients
        |            |
        v            v
     ws writes    ws writes  (fan-out cost identical to v2)
```
```

## References
- v2 hub: `internal/hub/hub.go`
- Load-test tooling: `loadtest/`, skill `loadtest-analysis`
- Memory: k6 handshake failures are a shutdown artifact; ramp config notes.
