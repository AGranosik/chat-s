# naive-scale — load-test summary

What the k6 load tests say about the naive-scale design, across every run that has
a record in the repo. For how to run the tests see
[`../loadtest/README.md`](../loadtest/README.md); for the per-run tables see the
`summary.md` files under `loadtest/results/`.

## The design under test

N identical copies of one Go process behind one nginx. Every copy has its own
in-memory hub and its own outbox relay; Postgres is the shared store and
transactional outbox; Redis pub/sub bridges instances.

```
k6 ──► nginx :80 ──► server1..N ──► Postgres (messages + outbox, 600m / 2 CPU)
        │               ▲  │
        │               │  └─ outbox relay (poll 2 s, 100-row batches,
        │               │      FOR UPDATE SKIP LOCKED) ─► Redis PUBLISH room:<id>
        │               └───────── Redis SUBSCRIBE room:<id> (redis:7, 200m / 1 CPU)
        └─ /ws  : hash $arg_room consistent  (room affinity)
           /api : round-robin
```

Write path of one message: websocket frame → `chat.Service` (one tx: insert message
+ insert outbox row) → some instance's relay claims the row on its next 2 s tick →
`hub.Broadcast` publishes to Redis → every instance subscribed to that room writes
to its local sockets.

Consequences that shape every result below:

- **~2 s latency floor.** The relay polls every 2 s (`internal/outbox/relay.go`), so
  e2e p50 around 1 s and p95 around 1.5–2 s is the healthy state, not a cap.
- **Exactly-once dispatch.** Relays on different instances claim disjoint batches
  (`storage.DispatchBatch`). Before that fix a message was delivered 2–3× and
  completeness read above 100 %; no run summarised here shows that.
- **Fan-out is multiplicative.** One sent message becomes `room_size` delivered
  frames, so *delivered/s = sent/s × users per room*.

## Configurations that were tested

| Tag / catalog | Instances | Per instance | WS routing | Rooms × users | Sockets | Source |
| --- | --- | --- | --- | --- | --- | --- |
| `3x200m` (retired) | 3 | 200 MB | round-robin | 1 000 × 10 | 10 000 | numbers kept in [`../loadtest/BASELINE.md`](../loadtest/BASELINE.md); JSONs deleted |
| `100r5u`, `500r2u`, `400r10u` | 3 | 200 MB | round-robin | 100×5, 500×2, 400×10 | 500 / 1 000 / 4 000 | [`../LOAD_TEST_ANALYSIS.md`](../LOAD_TEST_ANALYSIS.md); JSONs in git history (`09071e2`) |
| `5x100m` | 5 (by tag) | 100 MB, 0.8 CPU | room-affinity | 500 × 15 | 7 500 | `loadtest/results/5x100m/tput/` |
| `10x100m` | 10 | 100 MB, 0.8 CPU | room-affinity | 500 × 15 | 7 500 | `loadtest/results/10x100m/tput/` |

Common settings for the last two: `RAMP=180 s`, `SETTLE=5 s`, `DURATION=30 s`,
`DRAIN=30 s`, k6 run in a container on the compose network, DB wiped per sweep,
servers restarted between steps. `server`→DB pool is `pool_max_conns=8` per
instance. Everything ran on one Windows/Docker Desktop host (AMD Ryzen 7 4800HS,
16 GB) that also runs the load generator, so absolute numbers are only comparable
with each other.

The instance count of `5x100m` comes from the directory tag; the repo does not
record the compose file used for that sweep. The compose and nginx
files in the working tree now run 15 instances (`15x100m`); `10x100m` used
the same files with `server1..10` only.

## Headline results

| Question | Answer | Where |
| --- | --- | --- |
| Healthy operating point, 7 500 sockets | **≈ 1 250 sent/s → ≈ 18 750 delivered/s**, 100 % complete, p95 ≈ 1.8–1.9 s, ≈ 0 ws errors — identical at 5 and 10 instances | `tput_si6` |
| Highest lossless rate, 7 500 sockets | ≈ 1 870–1 880 sent/s → ≈ 28 000 delivered/s, 100 % complete, but p95 10–18 s | `tput_si4` |
| Collapse point | Interval 3 s (2 500 sent/s): 78–88 % complete, 30–37 s p95, ws errors appear | `tput_si3` |
| Did doubling instances 5 → 10 raise the ceiling? | **No.** Same healthy point, same collapse step; only the overloaded step is softer | see below |
| Earlier 3×200 m, round-robin, 10 000 sockets | ≈ 8 537 delivered/s at 99.1 % (+54 % over the single-instance process) | [`BASELINE.md`](../loadtest/BASELINE.md) |

## Sweep: 500 rooms × 15 users (7 500 sockets), 5 vs 10 instances

`Sent/s` is offered messages per second over the 30 s send window; delivered/s is
received frames over the same window; completeness is received ÷ (sent × 15).

| Interval | Sent/s (5 / 10) | Delivered/s (5 / 10) | Complete (5 / 10) | ws errors (5 / 10) | e2e p50 (5 / 10) | e2e p95 (5 / 10) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 s | 750 / 750 | 11 251 / 11 257 | 100 % / 100 % | 1 / 0 | 1.19 / 1.18 s | 1.79 / 1.74 s |
| 8 s | 932 / 938 | 13 976 / 14 070 | 100 % / 100 % | 0 / 3 | 1.16 / 1.23 s | 1.77 / 1.78 s |
| 6 s | 1 249 / 1 250 | 18 729 / 18 755 | 100 % / 100 % | 2 / 2 | 1.19 / 1.30 s | 1.83 / 1.88 s |
| 4 s | 1 879 / 1 871 | 28 183 / 28 060 | 100 % / 100 % | 1 / 0 | **13.2 / 3.2 s** | **18.4 / 10.1 s** |
| 3 s | 2 500 / 2 498 | 32 881 / 29 276 | 87.7 % / 78.1 % | 0 / 540 | 28.1 / 27.4 s | 33.6 / 36.9 s |
| 2 s | 3 733 / 3 709 | 10 425 / 8 682 | 18.6 % / 15.6 % | 1 734 / 3 144 | 46.9 / 31.3 s | 52.7 / 53.2 s |
| 1 s | 7 214 / 7 262 | 3 190 / 5 395 | 2.9 % / 5.0 % | 4 636 / 3 297 | 29.5 / 31.8 s | 55.5 / 55.3 s |

Handshakes were 100 % and connect p95 was 2 ms at every step, so the sockets were
never the problem — delivery was.

### How it breaks

1. **Up to 1 250 sent/s the system is flat.** Delivered/s tracks offered × 15
   exactly and latency sits at the relay's 2 s floor. 5 and 10 instances are
   indistinguishable.
2. **At ≈ 1 870 sent/s it is lossless but queueing.** Nothing is dropped, yet
   latency jumps from ~1.2 s to 10–18 s: the relay can no longer drain the outbox
   within a poll interval, so rows wait. This is the first sign of saturation and
   the only place the instance count visibly matters (p50 13.2 s with 5 instances,
   3.2 s with 10).
3. **At ≈ 2 500 sent/s delivery falls behind the run.** Messages still pending when
   the 30 s drain ends count as lost (78–88 % complete) and websocket errors
   begin.
4. **Beyond that it collapses.** At 3 700+ sent/s only 3–19 % arrives, p95 is
   ~53–55 s and thousands of sockets error out. Delivered/s *falls* as offered
   load rises, which is the signature of an overloaded pipeline spending its
   capacity on failures rather than a clean cap.

### What it does and does not say

- The ceiling did not move when the app tier doubled (5 → 10 instances, 500 → 1 000 MB
  aggregate). Whatever limits it is **shared by all instances** — Postgres
  (outbox claim/mark and message inserts, 600 MB / 2 CPU, 8 pooled connections
  per instance), Redis, nginx, or the host CPU also running k6. The runs recorded
  no CPU, memory, Postgres or Redis metrics, so the wall is **not attributed**.
- Past the healthy point a single-process "just add more boxes" gives little:
  extra instances only soften the overloaded step (lower latency at 4 s, fewer
  errors at 3 s) and do not raise the knee.
- The `5x100m` and `10x100m` sweeps are one run per point, so differences of a few
  percent (e.g. the 1 s step, 2.9 % vs 5.0 %) are noise.

## Earlier small and medium catalogs (3 × 200 MB, round-robin)

Detailed in [`../LOAD_TEST_ANALYSIS.md`](../LOAD_TEST_ANALYSIS.md); condensed here.

| Catalog | Interval | Offered/s | Delivered/s | Complete | ws errors | e2e p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100×5 (500 sockets) | 10 s | 50 | 250 | 100 % | 0 | 1.8 s |
| | 4 s | 125 | 627 | 100 % | 0 | 1.7 s |
| | 1 s | 500 | 2 500 | 100 % | 0 | 1.5 s |
| 500×2 (1 000 sockets) | 10 s | 100 | 200 | 100 % | 0 | 1.8 s |
| | 4 s | 250 | 499 | 100 % | 0 | 1.7 s |
| | 1 s | 1 000 | 2 001 | 100 % | 0 | 1.5 s |
| 400×10 (4 000 sockets) | 4 s | 841 | 7 242 | 86.1 % | 1 483 | 9.6 s |
| | 3 s | 1 334 | 13 337 | 100 % | 0 | 21.6 s |
| | 2 s | 1 579 | 8 164 | 51.7 % | 3 201 | 22.5 s |

- Up to 1 000 sockets / 1 000 sent/s everything is complete at the latency floor.
- At 4 000 sockets the only complete run (3 s) has a 21.6 s p95, so it is a
  throughput observation, not an operating point. The 4 s step losing messages
  while the 3 s step does not shows run-to-run variance at the edge.

The first 3 × 200 MB sweep (1 000 rooms × 10 users, 10 000 sockets, post
duplicate-dispatch fix) found a healthy point of 862 sent/s → 8 537 delivered/s at
99.1 % (p95 1.46 s) and a collapse at 1 300 sent/s (70 %, p95 26 s). Its full table
is in [`../loadtest/BASELINE.md`](../loadtest/BASELINE.md), including the
comparison with single-instance (5 542 delivered/s on the same 600 MB aggregate).

## Across the whole series

| Configuration | Sockets | Healthy delivered/s | Lossless-but-slow / collapse |
| --- | ---: | ---: | --- |
| 3 × 200 MB, round-robin | 10 000 | 8 537 (862 sent/s) | collapse from 1 300 sent/s |
| 3 × 200 MB, round-robin | 4 000 | n/a (no clean run) | 1 334 sent/s complete but p95 21.6 s |
| 5 × 100 MB, room-affinity | 7 500 | 18 729 (1 249 sent/s) | 1 879 sent/s queueing; 2 500 loses |
| 10 × 100 MB, room-affinity | 7 500 | 18 755 (1 250 sent/s) | 1 871 sent/s queueing; 2 498 loses |

Sockets, room size, memory, CPU limits and routing all changed between rows, so
this is a trend, not a controlled comparison. The one clean contrast is 5 vs 10
instances, where only the instance count changed.

## Caveats

- One host runs the stack and the k6 generator; CPU contention affects the highest
  rates most.
- Single run per point, no repeats, so small differences are not significant.
- No resource telemetry was captured, so bottlenecks are inferred from completeness
  and latency shape only.
- The `3x200m` JSONs and the `100r5u` / `500r2u` / `400r10u` JSONs are no longer in the
  working tree (the latter remain in git history); their numbers survive in
  `BASELINE.md` and `LOAD_TEST_ANALYSIS.md`.
- `loadtest/README.md` and `BASELINE.md` still describe the retired 3 × 200 MB
  setup and its default `-Tag 3x200m`; `run-limits.ps1` now defaults to
  `-Tag 15x100m`.

## Reproducing the 7 500-socket sweep

```powershell
./loadtest/run-limits.ps1 -Scenario tput -Rooms 500 -Users 15 -Steps 10,8,6,4,3,2,1
./loadtest/summarize.ps1  -Dir loadtest/results/15x100m/tput
```

Each run recreates the stack and **wipes the database**; each step takes about
four to five minutes (ramp + settle + send + drain).

## Suggested next measurements

1. Record `docker stats`, Postgres wait events / active connections and Redis
   `INFO` during the 4 s step to attribute the shared wall.
2. Repeat the 6 s, 4 s and 3 s steps three times each to size the run-to-run noise.
3. Run a `conn` sweep (no sends) — the connection ceiling of the 100 MB instances
   has never been measured; the tput sweeps only prove ≥ 7 500 sockets.
