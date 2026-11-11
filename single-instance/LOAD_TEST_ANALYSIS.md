# Load-test analysis — single instance

**Architecture.** One Go process behind nginx. Every socket has a read goroutine and a write goroutine. The read goroutine runs each incoming message's Postgres transaction inline (message row + outbox row). One relay goroutine claims undispatched outbox rows in 500-row batches, hands each row to the single hub goroutine for fan-out, marks the batch dispatched, and polls again 1 s after a drain finishes (`internal/outbox/relay.go`). nginx is the only entry point. Diagram: [README → Architecture](README.md#architecture).

**How to read the numbers.** Each k6 virtual user holds one socket and sends at the configured interval during a 30-second send window. Sockets then stay open for a further 30-second drain.
- `Offered/s` is messages k6 wrote ÷ 30 s. k6 counts these on the client side, so they don't show what the server committed.
- `Delivered/s` is received frames ÷ 30 s.
- `Complete` is received ÷ (sent × users per room), so it includes the sender's own echo.
- Latency runs from k6 send to k6 receive. It covers ingest, the outbox poll, the relay, the hub, the socket write, nginx and k6 itself, and k6 cannot tell which stage the time went to.

Each VU connects once and never reconnects, so a socket that drops stops both sending and receiving for the rest of the step. These numbers come from one machine with the load generator on the same host. They are not capacity guarantees.

## 300 MB server — sweep committed 2026-11-11

Source: `loadtest/results/300m/tput/` (step JSONs and `summary.md`, commit `4641e1b`).

**Configuration under test** (from `docker-compose.yml`)

| Service | Memory | CPU |
| --- | ---: | ---: |
| Go server (the system under test) | 300 MB | 8 |
| PostgreSQL 16 | 600 MB | 2 |
| nginx | 512 MB | 1 |

**Workload.** 500 rooms × 15 users = 7,500 held sockets. The run uses a 180 s ramp (about 42 new sockets/s), a 5 s settle, a 30 s send window (185–215 s into the scenario) and a 30 s drain, and sockets close at 245 s. Only the send interval changes between steps, and the steps ran in the order shown. Before every step `run-limits.ps1` wipes the whole stack (`docker compose down -v`). Each step therefore starts with a fresh server process and an empty database.

| Send interval | Offered/s | Delivered/s | Complete | WS errors | E2E p50 / p95 / max | k6 thresholds | State |
| ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 10 s | 750.1 | 11,251 | 100.0% | 1 | 691 / 1,042 / 1,263 ms | pass | at the poll floor |
| 8 s | 938.3 | 14,075 | 100.0% | 0 | 772 / 1,126 / 1,562 ms | pass | at the poll floor |
| 6 s | 1,250.3 | 18,754 | 100.0% | 0 | 867 / 1,242 / 1,808 ms | pass | light queueing |
| 4 s | 1,873.2 | 28,099 | 100.0% | 1 | 1,142 / 2,045 / 2,821 ms | pass | queueing, bounded |
| 3 s | 2,500.7 | 37,510 | 100.0% | 0 | 16,762 / 21,973 / 22,864 ms | latency | complete only thanks to the drain |
| 2 s | 3,744.4 | 33,564 | 59.8% | 1,455 | 39,574 / 42,144 / 43,115 ms | latency, WS errors | collapsed |
| 1 s | 7,362.7 | 4,951 | 4.5% | 4,742 | 28,940 / 54,565 / 58,166 ms | latency, WS errors | collapsed |

Handshake success was 100% and connect p95 was 2 ms in every step. The k6 thresholds are p95 < 2.5 s, p99 < 3.5 s, and WS errors within the teardown budget. That budget is 0.5% of sockets (38) and absorbs k6's end-of-test noise. Every step down to 3 s stays inside it.

### Verdict

- **Sustainable ceiling: about 28,100 delivered frames/s** (1,873 sends/s × 15 recipients) at the 4 s step. It is the highest step that passes every k6 threshold: 100% complete, p95 2.0 s, slowest message 2.8 s.
- **Latency-clean operating point: about 18,750 delivered frames/s** (1,250 sends/s) at the 6 s step, with p95 1.24 s. The 1 s poll sets a floor of about 1.04 s p95 (10 s step).
- **Completeness read-off (`summarize.ps1` hint): about 37,500 frames/s** at the 3 s step. This is not a sustainable rate. p50 is 16.8 s and the slowest message took 22.9 s. Nothing was lost only because the senders stopped after 30 s and the backlog cleared inside the 30 s drain.
- **The knee is between 1,873 and 2,501 sends/s.** Past it the server degrades instead of plateauing.

### How it broke

1. **Not a connection limit.** Handshakes stayed at 100% with 2 ms connect p95 through the collapse, and all 7,500 sockets were admitted at every step. The ramp is slow, so this is not a thundering herd.
2. **Queueing starts (6 s → 4 s).** Completeness stays at 100%, but p95 moves off the floor: 1.04 s at 10 s, 1.24 s at 6 s, 2.05 s at 4 s. Delivery is late, not lost.
3. **The backlog outlives the send window (3 s).** All 1,125,300 frames arrived, but over up to 52.9 s (30 s of sending + 22.9 s max latency). That averages about 21,300 frames/s, or roughly 1,420 messages/s fanned out to 15. The 4 s step averaged about 25,700 frames/s over its 32.8 s, so offering more load delivered *less* per second. The latency distribution is skewed toward the maximum (p50 is 73% of max), which means delivery fell behind early in the window rather than slowing evenly.
4. **Collapse (2 s and 1 s).** Delivery falls to 59.8% and 4.5%. 1,455 and 4,742 sockets error out (19% and 63%), far above the 38-error budget, so these are real disconnects. At 2 s nearly everything that arrived was 37–43 s late (p50 39.6 s, p90 41.9 s, max 43.1 s). Delivery ran a near-constant ~40 s behind, so only messages sent in roughly the first 20 s of the window could arrive before the sockets closed. k6 still wrote 99.9% and 98.2% of its target sends, but that is a client-side count. It does not show that the server committed them.
5. **The 2 s collapse is not a process restart.** A restart would end every session at once. The median socket session there is 155.0 s, the same as in every healthy step. At 1 s the median is 136.2 s, so many sockets dropped well before the end. Only 63% of sockets errored, which argues against a single restart of the whole process. Restart count and OOM state were not recorded, though, so a late restart at 1 s cannot be ruled out.

### Why sockets drop: the 60 s pong deadline (inferred)

The disconnects match the server's own read deadline rather than a crash or a client-side problem.

- **The signature.** The shortest socket session in both collapsed steps is exactly 60.0 s, in this sweep and in the previous one. In every healthy step the shortest is 65.0 s: the last socket connects at about 180 s and is closed by k6 at 245 s. 60 s is `pongWait` in `internal/hub/client.go`.
- **The mechanism in the code.** The read deadline is set to 60 s at connect and is extended only when a pong is read. The server pings every 54 s, so each pong has to be *read* within about 6 s. The read goroutine also runs `HandleIncoming` for every frame, one Postgres transaction at a time, and the client's pong sits in the same TCP stream behind that client's earlier messages. If the goroutine is more than ~6 s behind when a ping goes out, the deadline passes, the next read fails, and the server closes the socket.
- **The timing.** Compared with the healthy steps, the dropped sockets lost on average 21.6 s of session (2 s step) and 14.6 s (1 s step). That puts the mean drop at about 223 s and 230 s, 8–15 s after sending stopped at 215 s, when the backlog is at its largest.
- **It is the read side, not k6's receive side.** At 1 s, k6 received only ~5,000 frames/s, yet that step dropped the most sockets. The extra load there is on ingest: 7,363 sends/s, each one a transaction on the sender's read goroutine.

Every socket is pinged at least once inside the loaded 60 s. If the mechanism is right, the absence of drops at 3 s means ingest stayed less than ~6 s behind there. Most of that step's 17–23 s wait then accrued *after* the commit: the poll, the relay, the hub, the write goroutines, nginx and k6. The server doesn't log read timeouts (gorilla's `IsUnexpectedCloseError` is false for them) or write errors, so server logs cannot confirm this today.

### What saturates first

The data puts the first wall on the delivery side and a second one on ingest:

- **Delivery first (between 1,873 and 2,501 sends/s).** Everything after the commit is serial or shared. The relay is one goroutine that claims, hands off and stamps one 500-row batch at a time, and each hand-off waits on the single hub goroutine. The hub makes 15 blocking channel sends per message. Each write goroutine issues one write per frame, through an nginx limited to 1 CPU.
- **Ingest second (by 3,744 sends/s).** Each message costs a Postgres transaction (two inserts and a commit) on the sender's read goroutine. It goes through a pool of 16 connections into a Postgres capped at 2 CPUs. The pool size is pgx's default, `max(4, NumCPU)`, and Docker on this machine reports 16 CPUs.
- **A saturated Postgres would explain both.** The relay's claim and stamp queries share Postgres's 2 CPUs with ~2,500 ingest commits/s. If Postgres saturates, the relay slows first because it is one serial consumer, while ingest still has 16 connections working in parallel. That fits the order of failure. No CPU figures were recorded, though, so it is a hypothesis, as are the other candidates: the hub, nginx, and the k6 process on the same host.

### Compared with the previous sweep

The previous sweep used the same server code and the same 1 s / 500-row relay. The only difference is method: it restarted only the server between steps, so Postgres kept every earlier step's rows. Before the 4 s step that was about 88,000 messages plus as many outbox rows, and about 144,000 of each before the 3 s step.

| Send interval | Previous: complete / errors / p95 / max | This sweep: complete / errors / p95 / max |
| ---: | --- | --- |
| 10 s | 100.0% / 1 / 1.06 / 1.47 s | 100.0% / 1 / 1.04 / 1.26 s |
| 8 s | 100.0% / 0 / 1.09 / 1.55 s | 100.0% / 0 / 1.13 / 1.56 s |
| 6 s | 100.0% / 0 / 1.50 / 2.41 s | 100.0% / 0 / 1.24 / 1.81 s |
| 4 s | 100.0% / 4 / 3.10 / 3.82 s | 100.0% / 1 / 2.05 / 2.82 s |
| 3 s | 100.0% / 2 / 26.98 / 27.83 s | 100.0% / 0 / 21.97 / 22.86 s |
| 2 s | 37.9% / 1,930 / 45.34 / 45.62 s | 59.8% / 1,455 / 42.14 / 43.12 s |
| 1 s | 3.5% / 2,979 / 54.74 / 57.15 s | 4.5% / 4,742 / 54.57 / 58.17 s |

The light steps are unchanged, and every loaded step is better. The 4 s step now passes the k6 latency thresholds (p95 3.1 → 2.0 s), and the 2 s step delivers 59.8% instead of 37.9%. That fits a carry-over effect from a growing database, but with one run per point its size is not established. The 1 s step dropped more sockets this time (4,742 vs 2,979), so the detail of the collapse does not reproduce exactly.

### Not measured

- Per-container CPU and memory during a step (`docker stats`), so the saturated component is unknown.
- `docker inspect chat-server --format '{{.RestartCount}} {{.State.OOMKilled}}'` after each step.
- The outbox backlog over time: `select count(*) from outbox where dispatched_at is null`.
- Why each socket closed: read timeouts and write errors are not logged.
- The connection-only (`conn`) ceiling at 300 MB.

Reproduce:

```powershell
./loadtest/run-limits.ps1 -Scenario tput -Rooms 500 -Users 15 -Steps 10,8,6,4,3,2,1
./loadtest/summarize.ps1 -Dir loadtest/results/300m/tput
```

## Conclusions

- Latency sits at the poll floor (~0.7 s p50 / ~1.04–1.13 s p95) up to 940 sends/s and rises gently to 2.0 s p95 at 1,873 sends/s. At 2,501 sends/s it jumps to 22 s.
- **Delivered frame rate and completeness alone are not a safe capacity criterion.** The 3 s step is 100% complete at 37,500 delivered/s, yet its messages were up to 23 s late, and the server averaged only ~21,300 frames/s while clearing them. Completeness depends on the drain window, so bounded maximum latency is the better test.
- **Past the knee the server degrades instead of plateauing.** Offering more load reduces what is delivered. At about 2× the sustainable send rate, a fifth of the sockets drop, and at 4× nearly two-thirds do. The drops are most consistent with the 60 s pong deadline firing on read goroutines stuck behind their own Postgres transactions. Any real deployment needs back-pressure or admission control in front of this design, and a read path that does not block on the database.
- Fan-out dominates the cost. Each accepted message becomes 15 socket writes, so the sustainable *send* rate (~1,900/s) is far lower than the delivered frame rate suggests.
- Wiping Postgres before each step made every loaded step better. Sweeps that reuse a database across steps understate the later steps.
- These are single-machine, single-run-per-point numbers with the generator sharing the host. They show where this design bends, not an absolute capacity.
