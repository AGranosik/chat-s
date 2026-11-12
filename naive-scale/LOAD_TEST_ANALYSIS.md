**Configuration under test** (from `docker-compose.yml`)

| Service | Memory | CPU |
| --- | ---: | ---: |
| Go server, per instance (the system under test) | 50 MB | 0.17 |
| PostgreSQL 16 | 600 MB | 2.5 |
| Redis 7 (cross-instance fan-out) | 200 MB | 1.0 |
| nginx | 512 MB | 2.5 |

Three sweeps ran. Only the number of server instances changes between them:

| Sweep | Instances | Server memory (total) | Server CPU (total) | Sockets per instance |
| --- | ---: | ---: | ---: | ---: |
| `3x50mib` | 3 | 150 MB | 0.51 | 500 |
| `5x50mib` | 5 | 250 MB | 0.85 | 300 |
| `7x50mib` | 7 | 350 MB | 1.19 | ~214 |

`3x50mib` has the single-instance budget (150 MB, 0.5 CPU) split three ways. `5x50mib` and `7x50mib` add instances on top of that. Across all containers the CPU limits add up to 6.5–7.2 of the Docker VM's 16 CPUs, and k6 runs without a limit. The 3-instance compose file was never committed. Commit `33e5b0a` records the 50 MB / 0.17 CPU template with five servers, so I assume the 3-instance run used the same template without `server4` and `server5`.

**How this differs from single-instance.** The request path is the same code except in three places (`internal/hub/hub.go`, `internal/outbox/relay.go`, `internal/storage/outbox.go`):

- **nginx round-robins every socket.** `chat_ws_backend` has no `hash`, so each room's 15 sockets are spread over all the instances: about 5 per instance at N = 3, 3 at N = 5 and 2–3 at N = 7. That means every instance has members of nearly every room, and it subscribes to all 100 `room:<id>` channels.
- **Fan-out goes through Redis.** The relay `PUBLISH`es each message. That is one synchronous round trip per message, made inside the claim transaction. Redis then pushes the message to every subscribed instance, and each instance delivers it to its local members. Every message is therefore received by all N instances, but its 15 websocket writes are split among them.
- **N relays share one outbox.** Each instance runs its own relay: a 2 s ticker, batches of 100, rows claimed with `FOR UPDATE SKIP LOCKED`. Single-instance instead polls 1 s after each drain, in batches of 500. Each instance also has its own Postgres pool (`pool_max_conns=8`).

**Workload.** The workload is the same as in single-instance: 100 rooms × 15 users = 1,500 held sockets. A 180 s ramp is followed by a 5 s settle, a 30 s send window (185–215 s) and a 30 s drain, and the sockets close at 245 s. Only the send interval changes between steps. Before every step `run-limits.ps1` wipes the whole stack (`docker compose down -v`, Postgres and Redis included). The 7-instance sweep ran only the 4 s to 1 s steps.

### 3 instances: 150 MB / 0.51 CPU in total, the single-instance budget

| Send interval | Offered msg/s | Frames received / expected | Delivery time | Delivered frames/s | Complete | WS errors | E2E min / p50 / p95 / max | k6 thresholds | State |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 10 s | 150.1 | 67,530 / 67,530 | 30 s | 2,251 | 100.0% | 0 | 0.02 / 0.79 / 1.42 / 1.63 s | pass | light load |
| **6 s** | **250.1** | **112,530 / 112,530** | **30 s** | **3,751** | **100.0%** | **0** | **0.02 / 0.68 / 1.30 / 2.16 s** | **pass** | **last green step, ~80% of capacity** |
| 4 s | 375.3 | 168,870 / 168,870 | 36.7 s | 4,602 | 100.0% | 0 | 0.39 / 4.62 / 6.24 / 6.69 s | latency | backlog grows, cleared in drain |
| **3 s** | **500.0** | **225,015 / 225,015** | **47.0 s** | **4,789** | **100.0%** | **0** | **0.15 / 14.08 / 16.25 / 16.99 s** | **latency** | **LIMIT: peak delivery; backlog grows, cleared in drain** |
| 2 s | 750.1 | 262,951 / 337,545 | 60 s (cap) | 4,383 | 77.9% | 0 | 0.49 / 28.45 / 35.84 / 37.22 s | latency | backlog outlived the drain |
| 1.5 s | 1,000.1 | 229,619 / 450,045 | 60 s (cap) | 3,827 | 51.0% | 17 | 0.48 / 35.17 / 43.51 / 44.97 s | latency, WS errors | first socket drops |
| 1 s | 1,474.0 | 131,751 / 663,300 | 60 s (cap) | 2,196 | 19.9% | 791 | 1.01 / 32.96 / 51.30 / 52.14 s | latency, WS errors | collapse |
| 0.5 s | 2,930.8 | 108,090 / 1,318,860 | 60 s (cap) | 1,802 | 8.2% | 1,262 | 0.89 / 27.67 / 52.79 / 56.42 s | latency, WS errors | collapse |

### 5 instances: 250 MB / 0.85 CPU in total

| Send interval | Offered msg/s | Frames received / expected | Delivery time | Delivered frames/s | Complete | WS errors | E2E min / p50 / p95 / max | k6 thresholds | State |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 10 s | 150.0 | 67,515 / 67,515 | 30 s | 2,251 | 100.0% | 0 | 0.01 / 0.45 / 1.07 / 1.17 s | pass | light load |
| 6 s | 250.0 | 112,515 / 112,515 | 30 s | 3,751 | 100.0% | 0 | 0.01 / 0.34 / 0.76 / 0.99 s | pass | light load |
| 4 s | 375.4 | 168,915 / 168,915 | 30 s | 5,631 | 100.0% | 0 | 0.01 / 0.44 / 0.81 / 1.19 s | pass | comfortable, ~80% of capacity |
| **3 s** | **500.1** | **225,030 / 225,030** | **30 s** | **7,501** | **100.0%** | **0** | **0.06 / 1.00 / 2.45 / 3.17 s** | **pass** | **last green step, at capacity** |
| **2 s** | **750.1** | **337,545 / 337,545** | **48.1 s** | **7,011** | **100.0%** | **0** | **0.11 / 12.44 / 16.23 / 18.15 s** | **latency** | **LIMIT: backlog grows, cleared in drain** |
| 1.5 s | 1,000.1 | 406,509 / 450,045 | 60 s (cap) | 6,775 | 90.3% | 0 | 0.14 / 23.79 / 31.80 / 35.83 s | latency | backlog outlived the drain |
| 1 s | 1,499.3 | 347,439 / 674,670 | 60 s (cap) | 5,791 | 51.5% | 151 | 0.29 / 33.46 / 42.75 / 44.82 s | latency, WS errors | collapse, first socket drops |
| 0.5 s | 2,919.6 | 238,774 / 1,313,805 | 60 s (cap) | 3,980 | 18.2% | 1,083 | 0.30 / 27.58 / 50.54 / 51.75 s | latency, WS errors | collapse |

### 7 instances: 350 MB / 1.19 CPU in total

| Send interval | Offered msg/s | Frames received / expected | Delivery time | Delivered frames/s | Complete | WS errors | E2E min / p50 / p95 / max | k6 thresholds | State |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 4 s | 375.3 | 168,870 / 168,870 | 30 s | 5,629 | 100.0% | 1 | 0.01 / 0.32 / 0.55 / 0.84 s | pass | light load |
| **3 s** | **500.2** | **225,105 / 225,105** | **30 s** | **7,504** | **100.0%** | **0** | **0.01 / 0.34 / 0.54 / 0.86 s** | **pass** | **last green step, no queueing** |
| **2 s** | **750.1** | **337,545 / 337,545** | **39.6 s** | **8,517** | **100.0%** | **0** | **0.08 / 3.69 / 8.28 / 9.63 s** | **latency** | **LIMIT: backlog grows, cleared in drain** |
| 1.5 s | 1,000.4 | 448,511 / 450,165 | 55.9 s | 8,025 | 99.6% | 12 | 0.23 / 13.09 / 21.06 / 25.89 s | latency, WS errors | backlog cleared; first socket drops |
| 1 s | 1,500.5 | 544,769 / 675,240 | 60 s (cap) | 9,080 | 80.7% | 36 | 0.16 / 25.28 / 34.97 / 37.82 s | latency, WS errors | backlog outlived the drain, no collapse yet |

The columns are computed exactly as in `single-instance/LOAD_TEST_ANALYSIS.md`:

- **Delivered frames/s** is frames received ÷ delivery time.
- **Delivery time** is 30 s when a step passes the p95 threshold, and 30 s plus the slowest message's latency when it fails it. It is capped at 60 s, because k6 closes the sockets 30 s after sending stops.
- **Expected frames** is messages sent × 15. It includes the sender's own echo.

These values match `deliver_s` and `recv_per_s` in each sweep's `summary.md`. In all 21 steps, all 1,500 handshakes succeeded and connect p95 was 2.1–2.5 ms. The k6 thresholds are p95 < 2.5 s, p99 < 3.5 s and at most 8 WS errors.

At the 7-instance 1.5 s step the backlog cleared before the cap (max latency 25.9 s). The 1,654 missing frames there fit the 12 sockets that dropped (inferred).

### Where the limits are

| Limit | 3 × 0.17 CPU | 5 × 0.17 CPU | 7 × 0.17 CPU |
| --- | --- | --- | --- |
| **Last all-green step** | **6 s: 250 msg/s → 3,751 frames/s, p95 1.30 s** | **3 s: 500 msg/s → 7,501 frames/s, p95 2.45 s** | **3 s: 500 msg/s → 7,504 frames/s, p95 0.54 s** |
| Raw delivery capacity | 307–320 msg/s → 4,600–4,800 frames/s (4 s and 3 s steps) | 452–467 msg/s → 6,775–7,011 frames/s (2 s and 1.5 s steps) | 535–605 msg/s → 8,025–9,080 frames/s (2 s, 1.5 s and 1 s steps) |
| Comfortable rate (measured) | none: the 6 s step (250 msg/s) is already ~80% of capacity | 375 msg/s (4 s step), p95 0.81 s | 500 msg/s (3 s step), p95 0.54 s, 83–93% of capacity |
| Knee | between 250 and 375 msg/s (p50 jumps from 0.68 s to 4.62 s) | between 500 and 750 msg/s (p50 1.00 s → 12.4 s); p95 already climbs from 0.81 s to 2.45 s between 375 and 500 | between 500 and 750 msg/s (p50 0.34 s → 3.69 s) |
| Socket drops begin | between 750 and 1,000 msg/s (0 errors, then 17) | between 1,000 and 1,499 msg/s (0, then 151) | between 750 and 1,000 msg/s (0, then 12) |
| Delivery falls as load rises | from 750 msg/s: 4,789 → 4,383 → 3,827 → 2,196 → 1,802 frames/s | from 750 msg/s: 7,501 → 7,011 → 6,775 → 5,791 → 3,980 frames/s | not reached by 1,500 msg/s, which still delivered 9,080 frames/s |
| Connection ceiling | not measured | not measured | not measured |
| Memory (50 MB each) | no sign of a limit in the green steps | same | same |

**Verdict: naive scale-out costs capacity at the same budget, and each instance you add buys less than the one before.**

- **3 instances** sustain ~250 msg/s and top out at ~310–320 msg/s (~4,600–4,800 frames/s). That is about 20% below one 0.5-CPU process.
- **5 instances** top out at ~450–470 msg/s (~6,800–7,000 frames/s).
- **7 instances** top out at ~535–605 msg/s (~8,000–9,100 frames/s).

The 5-instance 3 s step is not a safe operating point. It sits just above the capacity measured at the 2 s step, its p95 is only 49 ms under the threshold, and it passes only because the send window lasts 30 s.

Planning at two-thirds of capacity, as single-instance does, gives ~210, ~300 and ~380 msg/s for 3, 5 and 7 instances.

`summarize.ps1`'s hint picks the highest delivery that is ≥ 98% complete and within the error budget. It ignores latency, so it reports raw capacity, not a sustainable rate: 4,789 frames/s (3 instances, 3 s step, fails latency), 7,501 (5 instances, 3 s step, green) and 8,517 (7 instances, 2 s step, fails latency).

### Each instance adds less than the one before (inferred)

| Instances | Server CPU (total) | Raw capacity | Per instance | Per CPU | Server CPU per accepted message |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 (single-instance, no Redis) | 0.50 | 375–400 msg/s | 375–400 | 750–800 | 1.25–1.33 ms |
| 3 | 0.51 | 307–320 msg/s | 102–107 | 600–630 | 1.59–1.66 ms |
| 5 | 0.85 | 452–467 msg/s | 90–93 | 530–550 | 1.82–1.88 ms |
| 7 | 1.19 | 535–605 msg/s | 76–86 | 450–510 | 1.97–2.22 ms |

Compared with 3 instances, 5 and 7 instances have 67% and 133% more CPU but only 47% and 82% more capacity (midpoints 313 → 460 → 570 msg/s).

The server CPU per message fits **≈ 1.25 ms + 0.12 ms × N**. That model predicts 317, 459 and 569 msg/s for 3, 5 and 7 instances, against the measured midpoints of 313, 460 and 570. It fits two parameters to three points, so it is a consistency check, not proof.

- **The fixed 1.25 ms** matches single-instance's own cost per message (1.25–1.33 ms): the ingest transaction, the relay's claim, and the 15 websocket writes.
- **The 0.12 ms × N** is a cost every instance pays for every message. Under round-robin every instance is subscribed to every room, so Redis pushes each message to all N instances. On each one, go-redis parses it, `consume` wraps it in an envelope, and the hub loop looks up the room. Only the 15 socket writes are divided among the instances.
- **A fixed per-process overhead cannot explain the diminishing returns.** Costs such as the runtime, GC and the relay's 2 s ticks would take the same slice of CPU from each instance, so capacity would still grow linearly with N. Only a per-message cost that grows with N gives diminishing returns. The other cost in the code that grows with N is N relays competing for the same outbox rows, but that one lands on Postgres, not on the servers. How server CPU splits between these costs was not measured.

**At large N (extrapolated, not measured):** capacity ≈ 0.17 · N / (1.25 + 0.12 · N) msg per ms. That gives ≈ 690 msg/s at 10 instances and ≈ 930 at 20. No number of 0.17-CPU instances gets past ≈ 0.17 / 0.12 ms ≈ 1,400 msg/s. From 10 to 20 instances, doubling the fleet buys about 35%. At 10 instances the per-instance tax (1.2 ms) equals the useful work (1.25 ms). Break-even with the single 0.5-CPU process is at about 4 instances (0.68 CPU). Postgres or Redis may bend before any of this.

### Why sockets drop: the same 60 s pong deadline (inferred)

The mechanism is the one described in `single-instance/LOAD_TEST_ANALYSIS.md`. The read goroutine runs `HandleIncoming`, one Postgres transaction per frame, and the client's pong queues behind that work. When the goroutine falls more than about 6 s behind, the 60 s read deadline passes and the server closes the socket. `internal/hub/client.go` is identical in both projects.

- **The shortest session is 60.0–60.4 s in every step with drops:** 60.25, 60.03 and 60.08 s for 3 instances (1.5 s, 1 s, 0.5 s); 60.03 and 60.00 s for 5 instances (1 s, 0.5 s); 60.43 and 60.30 s for 7 instances (1.5 s, 1 s). In every healthy step it is 65.0 s, and the healthy session distribution (min 65.0 / median 155.0 / max 244.9 s) is identical to single-instance.
- **The extra k6 iterations add up.** Dropped VUs idle in 1 s loops, so the extra iterations should equal the socket-seconds lost, and they do. At 3 instances / 1 s, 13,100 socket-seconds were lost and there were 13,066 extra iterations. At 5 / 1 s the numbers are 3,480 and 3,426, at 5 / 0.5 s they are 21,165 and 21,158, and at 7 / 1 s they are 855 and about 830.
- **Timing.** The average dropped socket lost 16.6 s at 3 / 1 s, 23.0 s at 5 / 1 s and 23.8 s at 7 / 1 s. That puts the mean drop at about 221–228 s, 6–13 s after sending stopped. Single-instance dropped at 223 s and 228 s.
- **Some drops cannot be restarts.** Restarting one instance would cut all of its sockets at once: about 500, 300 or 214 of them for 3, 5 or 7 instances. The 151 drops at 5 / 1 s and the 12 and 36 drops with 7 instances are fewer than one instance holds, so they are not whole-instance restarts. The larger counts (791 and 1,262 with 3 instances, 1,083 with 5) could include a restart. `RestartCount` and `OOMKilled` were not recorded.

With 7 instances, sockets start to drop at a lower overload (~1.8× capacity) than with 3 or 5 (~3×), but in much smaller numbers. 12 errors against a budget of 8 is marginal, and run-to-run variance is unknown.

### Compared with single-instance

| | single-instance 1 × 150 MB / 0.5 CPU | naive 3 × 50 MB / 0.17 | naive 5 × 50 MB / 0.17 | naive 7 × 50 MB / 0.17 |
| --- | ---: | ---: | ---: | ---: |
| Server CPU (total) | 0.50 | 0.51 | 0.85 | 1.19 |
| Last all-green step | 4 s (375 msg/s) | 6 s (250 msg/s) | 3 s (500 msg/s) | 3 s (500 msg/s) |
| p95 at the last green step | 2.37 s | 1.30 s | 2.45 s | 0.54 s |
| Raw capacity | 375–400 msg/s | 307–320 msg/s | 452–467 msg/s | 535–605 msg/s |
| Capacity vs single-instance | — | −15 to −23% | +13 to +25% | +34 to +61% |
| p50 at light load | 0.76 s (10 s step) | 0.79 s (10 s step) | 0.45 s (10 s step) | 0.32 s (4 s step) |
| p95 at the 4 s step (375 msg/s) | 2.37 s | 6.24 s | 0.81 s | 0.55 s |
| Delivered frames/s at the 1 s step | 3,881 | 2,196 | 5,791 | 9,080 |
| First socket drops | 1 s: 1,483 msg/s, 415 (28%) | 1.5 s: 1,000 msg/s, 17 (1%) | 1 s: 1,499 msg/s, 151 (10%) | 1.5 s: 1,000 msg/s, 12 (0.8%) |
| Drops at the 1 s step | 415 (28%) | 791 (53%) | 151 (10%) | 36 (2.4%) |
| Shortest session once collapsed | 60.0 s | 60.0 s | 60.0 s | 60.3 s |

At the same budget, the three-way split loses one step of headroom. The single process passes at 375 msg/s, while 3 instances already fail it with a 6.2 s p95. Beating one 0.5-CPU process takes about 4 instances of 0.17 CPU, roughly 1.4× the CPU. 7 instances use 2.4× the CPU and deliver about 1.5× the messages. Scale-out does deliver two things: a lower latency floor (more relays polling) and a more graceful overload once there is enough spare capacity (7 instances still deliver more at 1,500 msg/s than at 750).

## Conclusions

- **Splitting the single-instance budget across 3 instances plus Redis costs ~20% of capacity:** ~310–320 msg/s against ~375–400. The last green step falls from 4 s (375 msg/s) to 6 s (250 msg/s).
- **Adding instances helps, but less each time.** 5 instances (0.85 CPU) reach ~450–470 msg/s and 7 instances (1.19 CPU) reach ~535–605 msg/s. Per CPU, capacity drops from ~615 to ~540 to ~480 msg/s.
- **The cost that grows with N is round-robin plus Redis fan-out.** Every instance receives every message, which adds ~0.12 ms of server CPU per message for each instance (inferred from the fit). Extrapolated, 10 instances reach ~690 msg/s, 20 reach ~930, and the ceiling sits near ~1,400 msg/s however many 0.17-CPU instances are added. Room affinity (`hash $arg_room consistent` on `chat_ws_backend`, which was removed for this sweep) would put each room on one instance, so each message would reach one instance. That should remove the N term and is the next lever to test.
- **The latency floor is the one thing naive scale-out improves at any N.** N relays polling one outbox cut the median wait at light load from ~0.8 s to ~0.3 s.
