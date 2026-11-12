**Configuration under test** (from `docker-compose.yml`)

| Service | Memory | CPU |
| --- | ---: | ---: |
| Go server (the system under test) | 150 MB | 0.5 |
| PostgreSQL 16 | 600 MB | 2.5 |
| nginx | 512 MB | 2.5 |

The infrastructure around the server got 5× its CPU, so the server should be the first thing to bend.

**Workload.** 100 rooms × 15 users = 1,500 held sockets. The scenario is a 180 s ramp (about 8 new sockets/s), a 5 s settle, a 30 s send window (185–215 s into the scenario) and a 30 s drain. Sockets close at 245 s. Only the send interval changes between steps, and the steps ran in the order shown. Before every step `run-limits.ps1` wipes the whole stack (`docker compose down -v`), so each step starts with a fresh server process and an empty database.

| Send interval | Offered msg/s | Frames received / expected | Delivery time | Delivered frames/s | Complete | WS errors | E2E min / p50 / p95 / max | k6 thresholds | State |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 10 s | 150.0 | 67,500 / 67,500 | 30 s | 2,250 | 100.0% | 0 | 0.11 / 0.76 / 1.28 / 1.40 s | pass | light load, already queueing |
| 6 s | 250.1 | 112,545 / 112,545 | 30 s | 3,752 | 100.0% | 0 | 0.41 / 1.27 / 1.88 / 2.18 s | pass | queueing, bounded |
| **4 s** | **374.5** | **168,525 / 168,525** | **30 s** | **5,618** | **100.0%** | **0** | **0.74 / 1.69 / 2.37 / 2.49 s** | **pass** | **at capacity** |
| 3 s | 500.1 | 225,060 / 225,060 | 40.2 s | 5,599 | 100.0% | 0 | 0.89 / 8.22 / 10.01 / 10.19 s | latency | backlog grows, cleared in drain |
| **2 s** | **750.1** | **337,530 / 337,530** | **56.3 s** | **5,999** | **100.0%** | **0** | **0.87 / 21.90 / 25.75 / 26.26 s** | **latency** | **backlog grows, cleared in drain** | **LIMIT**
| 1.5 s | 1,000.3 | 299,748 / 450,120 | 60 s (cap) | 4,996 | 66.6% | 0 | 0.91 / 32.14 / 39.21 / 40.06 s | latency | backlog outlived the drain |
| 1 s | 1,483.2 | 232,848 / 667,455 | 60 s (cap) | 3,881 | 34.9% | 415 | 1.20 / 38.17 / 46.83 / 47.51 s | latency, WS errors | collapse |
| 0.5 s | 2,920.2 | 157,847 / 1,314,075 | 60 s (cap) | 2,631 | 12.0% | 1,255 | 2.10 / 27.71 / 51.85 / 54.09 s | latency, WS errors | collapse |

**Delivered frames/s = frames received ÷ delivery time.** For example, at the 2 s step that is 337,530 ÷ 56.3 s = 5,999/s. `Complete` is frames received ÷ frames expected. Expected is messages sent × 15 users per room, and it includes the sender's own echo.

`Delivery time` is how long frames actually kept arriving:
- **30 s** when the step passes the p95 latency threshold. The server kept up, so everything arrived inside the 30 s send window.
- **30 s + the slowest message's latency** when the step fails it. Messages queued up and kept arriving after sending stopped. At the 3 s step that is 30 + 10.2 s. At the 2 s step it is 30 + 26.3 s.
- **60 s at most (cap)**, because k6 closes the sockets 30 s after sending stops. Frames still queued at that point are never delivered, which is why the last three steps are incomplete.

These values match `deliver_s` and `recv_per_s` in `summary.md`. The delivery time past the threshold is an estimate. The k6 summary keeps totals and percentiles, not a timestamp per frame, so it assumes the slowest messages were among the last ones sent. That is very likely, because the relay reads the queue oldest-first. Because of the 60 s cap, the 2 s step's rate cannot be below 337,530 ÷ 60 s = 5,626/s.

In every step all 1,500 handshakes succeeded and connect p95 was 2.3–2.4 ms. The k6 thresholds are p95 < 2.5 s, p99 < 3.5 s, and WS errors ≤ 8. That error budget is 0.5% of sockets and absorbs k6's end-of-test noise.

### Where the limits are

| Limit | Value | Status |
| --- | --- | --- |
| **Sustainable message rate** | **~375 msg/s in → ~5,600 frames/s out** (4 s step) | found: the last step that passes every threshold |
| Raw delivery capacity | 375–400 msg/s → 5,600–6,000 frames/s | found: measured from how fast the backlog cleared at 3 s and 2 s |
| Comfortable operating rate | ~250 msg/s → ~3,750 frames/s (6 s step), p95 1.9 s | about two-thirds of capacity |
| Knee | between 375 and 500 msg/s | p50 jumps from 1.7 s to 8.2 s |
| Socket drops begin | between 1,000 and 1,483 msg/s (about 2.7–4× capacity) | 0 errors at 1.5 s, 415 at 1 s |
| Connection ceiling | not reached, not measured | 1,500 sockets were admitted cleanly at every step; no `conn` sweep was run at 150 MB |
| Memory (150 MB) | not the limit in this sweep | no sign of a restart or OOM kill (see below) |

**Verdict: the message-rate limit is about 375 accepted messages/s, or about 5,600 delivered frames/s at fan-out 15, for one 0.5-CPU server.** The 4 s step is not a safe operating point. Its offered rate sits on the measured capacity line, its p95 is only 128 ms under the threshold, and its slowest message took 2.49 s. Plan around ~250 msg/s.

`summarize.ps1` used to report **11,251 delivered/s at the 2 s step**, because it divided 337,530 frames by 30 s although delivery took 56.3 s. It now divides by the real delivery time and reports 5,999/s. Its hint still points at the 2 s step, because the hint only checks completeness and socket errors, not latency. That makes 5,999/s the raw delivery capacity, not a sustainable rate.

### How it broke

1. **Not a connection limit.** Handshakes stayed at 100%, with connect p95 at 2.4 ms or less, through the collapse. The ramp is about 8 sockets/s, so this is not a thundering herd.
2. **Queueing from the first step.** The fastest message gets slower as load rises: 107 ms, then 412 ms, then 739 ms. p50 goes from 0.76 s to 1.27 s to 1.69 s between 150 and 375 msg/s. Every message waits longer, so the relay's drain cycle itself is getting slower, not just the tail. For comparison, the 8-CPU server held p95 at the 1.04 s poll floor at 750 msg/s. The 0.5-CPU server is already past that floor (p95 1.28 s) at 150 msg/s.
3. **The queue grows without bound past capacity (3 s and 2 s).** If messages arrive at rate λ and the server delivers at rate μ < λ, the backlog grows for the whole 30 s, and the last message waits about (λ − μ) · 30 / μ. The slowest message waited 10.2 s at 500 msg/s and 26.3 s at 750 msg/s, which solves to **μ ≈ 375 msg/s and μ ≈ 400 msg/s**. Dividing frames by the real delivery window gives the same result: 5,599 and 5,999 frames/s. Both steps are 100% complete only because the senders stopped after 30 s and the 30 s drain was long enough to clear the backlog. Under continuous load, latency would keep growing by about 0.33 s (3 s step) and 0.88 s (2 s step) for every second of sending.
4. **The backlog outlives the drain (1.5 s).** 66.6% complete with zero socket errors. Nothing was dropped by the server. About 10,000 messages were still queued when k6 closed the sockets at 245 s. Everything that did arrive was 32–40 s late.
5. **Collapse (1 s and 0.5 s).** Offering more load delivers less: ~6,000 frames/s at 750 msg/s, then 5,000, 3,900 and 2,600 frames/s. 415 sockets (28%) drop at 1 s, and 1,255 (84%) at 0.5 s, against a budget of 8. Part of the lower frame rate comes from dropped sockets no longer receiving. The rest fits ingest competing with delivery for the same 0.5 CPU (inferred). Every incoming message costs a Postgres transaction on the sender's read goroutine, so the more k6 offers, the less CPU is left for the relay, the hub and the socket writes.

### Why sockets drop: the 60 s pong deadline (inferred)

The disconnects have the same signature as in the 300 MB sweep, and they fit the server's own read deadline rather than a crash.

- **The mechanism in the code.** The read deadline is set to 60 s at connect and is extended only when a pong is read (`internal/hub/client.go`). The server pings every 54 s, so each pong has to be *read* within about 6 s. The read goroutine also runs `HandleIncoming` for every frame, one Postgres transaction at a time. The client's pong sits in the same TCP stream behind that client's unprocessed messages. When the goroutine is more than about 6 s behind, the deadline passes and the server closes the socket.
- **The shortest session is exactly 60.0 s** at 1 s (60.05 s) and at 0.5 s (60.03 s). In every healthy step it is 65.0 s, because the last socket connects at about 180 s and k6 closes it at 245 s. 60 s is `pongWait`. These are the last sockets to connect: their initial deadline fell at 215–240 s, just after sending stopped, when each read goroutine's queue is longest.
- **The longest session shrinks to 235.0 s at 0.5 s** (244.9 s when healthy). The earliest sockets were all cut about 10 s before k6 would have closed them, which fits their fourth ping landing at about 216–226 s.
- **Timing.** The dropped sockets lost on average 22.4 s of session at 1 s and 17.0 s at 0.5 s. That puts the mean drop at about 223 s and 228 s, 8–13 s after sending stopped. The extra k6 iterations confirm it: dropped VUs idle in 1 s loops, and the iteration counts rise by about 9,300 and 21,400, matching the lost seconds.
- **It is not a process restart.** At 1 s the median session is 155.0 s, the same as in every healthy step, so most sockets lived their full session. A restart or OOM kill would end every socket at the same instant. At 0.5 s the session distribution also does not fit a single cut-off: a restart at 235 s would leave at most ~6% of sessions above 223 s, and 10% are. `RestartCount` and `OOMKilled` were not recorded, though.

The server does not log read timeouts (gorilla's `IsUnexpectedCloseError` is false for them) or write errors, so its logs cannot confirm the mechanism today.

### What saturates first: the server's 0.5 CPU (inferred)

No `docker stats` were captured. The other candidates are ruled out by the earlier 300 MB / 8 CPU sweep, which used the same code on the same host:

- **Postgres and nginx.** With *less* CPU (2 and 1), they carried 1,873 commits/s and 28,100 frames/s. Here they have 2.5 CPU each, and the knee is at ~375 commits/s and ~5,600 frames/s, five times below what they have already handled.
- **The k6 generator.** It received 28,100 frames/s on the same machine.
- **The serial relay and hub.** One relay goroutine and one hub goroutine moved ≥1,873 msg/s when the process had 8 CPUs.
- **Memory.** There is no sign of a restart (above).

That leaves the server's CPU, cut 16× from 8 to 0.5. The implied budget is 0.5 CPU-s ÷ 375–400 msg/s ≈ **1.25–1.33 ms of server CPU per accepted message**, or about 83–89 µs per delivered frame. That budget covers the ingest transaction (BEGIN, two INSERTs and a COMMIT through pgx), the relay's share of claiming, decoding, re-encoding and stamping, 15 hub channel sends and 15 websocket frame writes. The 0.5-CPU quota is also enforced by CFS throttling (50 ms of CPU per 100 ms period), which can add stalls of up to tens of milliseconds at any load. That was not measured either.

### Compared with the 300 MB / 8 CPU sweep

The earlier sweep is in commit `4641e1b`. Its full analysis is in this file's git history.

| | 300 MB / 8 CPU (2026-11-11) | 150 MB / 0.5 CPU (2026-11-12) |
| --- | ---: | ---: |
| Sockets | 7,500 (500 × 15) | 1,500 (100 × 15) |
| Postgres / nginx CPU | 2 / 1 | 2.5 / 2.5 |
| Last all-green step | 4 s | 4 s |
| Sustainable sends/s | 1,873 | 375 |
| Delivered frames/s | 28,099 | 5,618 |
| p95 at the last green step | 2.05 s | 2.37 s |
| p95 at the 10 s step | 1.04 s (750 msg/s) | 1.28 s (150 msg/s) |
| First socket drops | 2 s step: 3,744 msg/s, 19% of sockets | 1 s step: 1,483 msg/s, 28% of sockets |
| Shortest session once collapsed | 60.0 s | 60.0 s |

The server has 16× less CPU but only 5× less throughput, so the 8-CPU server was not CPU-bound. Its wall was somewhere else. The earlier analysis suspected the 2-CPU Postgres or the serial relay, but never confirmed it. Both sweeps top out at the 4 s step only because their socket counts differ by the same 5× as their throughput. The overload failure is the same pong-deadline drop in both, but here it starts at ~4× the sustainable rate, against ~2× at 8 CPU.

## Conclusions

- **There is a hard limit: ~375–400 msg/s in, ~5,600–6,000 frames/s out, for one 0.5-CPU server at fan-out 15.** The sustainable figure is the 4 s step (374.5 msg/s, p95 2.37 s), and it is right on the edge. ~250 msg/s leaves headroom.
- **Fan-out dominates the cost.** Each accepted message becomes 15 socket writes, so the sustainable *send* rate is 15× lower than the frame rate.
- **Completeness alone is not a safe capacity test.** The 3 s and 2 s steps are 100% complete, but their latency grows for as long as the senders keep going. Bounded maximum latency is the right test. `summarize.ps1`'s hint ignores latency, so it points at the 2 s step (raw capacity) rather than the 4 s step (sustainable).
- **Past the knee the server degrades instead of plateauing.** Delivered frames fall from ~6,000/s to ~2,600/s as offered load rises, and at about 4× capacity sockets start to drop on the 60 s pong deadline. A real deployment needs back-pressure or admission control in front of this design, and a read path that does not block on the database.
- **For scaling comparisons**, treat ~375 msg/s (~5,600 frames/s) as the per-server number for a 0.5-CPU instance at fan-out 15.

### Not measured

- Per-container CPU and memory during a step (`docker stats`), and the server's CFS throttling (`cpu.stat`), so the CPU attribution is inferred.
- `docker inspect chat-server --format '{{.RestartCount}} {{.State.OOMKilled}}'` after each step.
- The outbox backlog over time: `select count(*) from outbox where dispatched_at is null`.
- Why each socket closed: read timeouts and write errors are not logged.
- The connection-only (`conn`) ceiling at 150 MB.
- A step between 4 s and 3 s (for example 3.5 s, ~430 msg/s) to pin the knee more tightly, and repeat runs to measure run-to-run variance.

Reproduce:

```powershell
./loadtest/run-limits.ps1 -Scenario tput -Rooms 100 -Users 15 -Steps 10,6,4,3,2,1.5,1,0.5
./loadtest/summarize.ps1 -Dir loadtest/results/150m/tput
```
