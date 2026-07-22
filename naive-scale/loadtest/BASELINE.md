# Load-test baseline — naive-scale: 3 instances @ 200 MB each

> Template. Run the sweeps (`run-limits.ps1`), then paste `summarize.ps1` output
> into the tables below. The point is a fixed, recorded reference that diffs
> cleanly against `single-instance/loadtest/BASELINE.md` (same 600 MB aggregate
> budget, split three ways). Fill every `_____`.

## System under test

| | |
| --- | --- |
| Instances | **3** (`server1..3`, nginx round-robin) |
| Server memory budget | **200 MB each** (600 MB aggregate) — `docker-compose.yml` `x-server` anchor |
| Cross-instance fan-out | **Redis pub/sub** (`room:<id>` channels); `redis:7` @ 200 MB |
| Server `nofile` | 262144 (per instance) |
| nginx `worker_connections` / `worker_rlimit_nofile` | 65536 / 262144 |
| nginx memory | 512 MB |
| `net.core.somaxconn` | 65535 |
| Generator | containerized k6 (`grafana/k6`, on the compose network) |
| Host (CPU / RAM / OS) | AMD Ryzen 7 4800HS / 16 GB / Windows 11 Pro (Docker Desktop) |
| Outbox poll interval | ~2 s (latency floor), per instance |
| Commit / date | `6065401` / 2026-07-09 |

## 1. WS connection ceiling

`./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000,40000,60000`
then `./loadtest/summarize.ps1 -Dir loadtest/results/3x200m/conn`.

The ceiling is the largest socket count with handshake% ≥ 99 and ws_errors
within the teardown budget (default 0.5 % of sockets; see
`summarize.ps1 -WsErrTolerancePct`), below the point where a **200 MB instance**
OOM-restarts. Sockets are spread ~evenly across the three instances by nginx, so
the per-instance socket count is roughly the total ÷ 3.

**Connection ceiling: not measured yet** — the `conn` sweep hasn't been run for
this stack (no `results/3x200m/conn/`); the `tput` sweep below held 10 000
sockets with 100 % handshakes at every step, so the ceiling is **≥ 10 000**.

<!-- paste summary.md from results/3x200m/conn here -->
| file | sockets | room_size | handshake_pct | ws_errors | conn_p95_ms | e2e_p50_ms | e2e_p95_ms | sent_per_s | recv_per_s | complete_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | | | | | | |

Notes (where/how it broke — handshake failures vs a 200 MB OOM restart vs latency): _____

## 2. Message rate at that ceiling

Set `-Rooms`/`-Users` so `Rooms × Users` = the connection ceiling from §1, then
sweep the send interval:
`./loadtest/run-limits.ps1 -Scenario tput -Rooms ___ -Users ___ -Steps 5,2,1,0.5,0.25`
then `./loadtest/summarize.ps1 -Dir loadtest/results/3x200m/tput`.

The ceiling is the highest delivered `recv_per_s` with completeness ≥ 98 % **and
not far above 100 %** (see caveat) and ws_errors within the teardown budget.

**Message-rate ceiling: ≈ 8 537 delivered msg/s at 10 000 sockets** (1 000 rooms
× 10 users, send interval 6 s ≈ 862 sent/s offered, completeness 99.1 %).

<!-- paste summary.md from results/3x200m/tput here -->
| file | sockets | room_size | handshake_pct | ws_errors | conn_p95_ms | e2e_p50_ms | e2e_p95_ms | sent_per_s | recv_per_s | complete_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tput_si20 | 10000 | 10 | 100 | 13 | 3 | 868 | 1613 | 227.3 | 2264.6 | 99.6 |
| tput_si15 | 10000 | 10 | 100 | 15 | 3 | 773 | 1410 | 316.8 | 3134.4 | 98.9 |
| tput_si10 | 10000 | 10 | 100 | 11 | 5 | 761 | 1416 | 499.4 | 4978.5 | 99.7 |
| tput_si9 | 10000 | 10 | 100 | 29 | 8 | 734 | 1580 | 559.4 | 5573 | 99.6 |
| tput_si8 | 10000 | 10 | 100 | 7 | 12 | 698 | 1617 | 636.2 | 6345.9 | 99.8 |
| tput_si7 | 10000 | 10 | 100 | 14 | 20 | 737 | 1526 | 730.7 | 7279.6 | 99.6 |
| tput_si6 | 10000 | 10 | 100 | 14 | 14 | 748 | 1456 | 861.6 | 8536.8 | 99.1 |
| tput_si4 | 10000 | 10 | 100 | 36 | 25 | 1484 | 25973 | 1300.8 | 9100 | 70 |
| tput_si3p5 | 10000 | 10 | 100 | 16 | 12 | 5812 | 62863 | 1511.8 | 6370.5 | 42.1 |
| tput_si3 | 10000 | 10 | 100 | 24 | 11 | 0 | 0 | 1745.5 | 0 | 0 |
| tput_si2p5 | 10000 | 10 | 100 | 23 | 5 | 0 | 0 | 2121.7 | 0 | 0 |

Notes (first bottleneck — completeness drop / unbounded latency / ws errors /
**duplicate dispatch inflating completeness**): **completeness drop.** Every step
is green through si6 (≥ 98.9 %, ws_errors ≤ 36 = within the 0.5 % teardown
budget, handshakes 100 % throughout). At si4 (~1 300 sent/s offered) delivery
stops tracking the offered rate: 9 100 recv/s at only **70 %** completeness with
e2e p95 blowing out to ~26 s, then delivery collapses entirely (0 % at si3). The
relay → Redis → fan-out pipeline is what saturates, not connections. Completeness
stays ≤ 100 % everywhere — this sweep is post-`DispatchBatch`-fix, no duplicate
dispatch inflating the numbers.

## 3. vs single-instance (the 1-vs-N diff)

```powershell
./loadtest/summarize.ps1 -Dir loadtest/results/3x200m/tput `
  -Compare ../single-instance/loadtest/results/600m/tput
```

| metric | single-instance (600m) | naive-scale (3×200m) |
| --- | --- | --- |
| Connection ceiling (sockets) | not re-run | not run yet (`conn` sweep pending) |
| Message-rate ceiling (delivered msg/s) | ≈ 5 542 @ 98.7 % | **≈ 8 537 @ 99.1 %** (+54 %) |

Verdict (did splitting the same budget across 3 processes + Redis help, hurt, or
wash — after accounting for duplicate dispatch?): **helped, ~54 % more delivered
msg/s** — and both sweeps are post-fix (completeness ≤ 100 %), so the compare is
honest. Naive-scale also degrades more gracefully: one step past its ceiling it
still delivers 9 100 msg/s at 70 % completeness where single-instance had already
collapsed to 0. The win comes from splitting the ws fan-out work (the expensive
part) across three processes; the shared Postgres outbox was not the bottleneck
at these rates.

**The round-robin caveat:** this ceiling is for *scattered* rooms. nginx
round-robins each connection, so a 10-member room's sockets land on all three
instances and every message is delivered by Redis to every instance — the
cross-instance amplification grows ~O(N) with instance count. Sending all of a
room's connections to the **same instance** (nginx `hash $arg_room consistent`)
would collapse that fan-out to an in-process write with ~1 Redis delivery per
message; that's the next experiment, see
[`docs/plans/2026-07-09-room-affinity-routing.md`](../docs/plans/2026-07-09-room-affinity-routing.md).

## Caveats

- **Duplicate dispatch (fixed).** All three instances poll the same outbox. This
  used to deliver a message 2–3× (`complete_pct` > 100 %, inflated `recv_per_s`)
  because the relay marked rows dispatched in a separate statement from the read.
  It's now fixed — the relay claims each batch with `storage.DispatchBatch`
  (`FOR UPDATE SKIP LOCKED` + mark, one tx), dispatching each row exactly once.
  **A completeness > 100 % here means the JSONs predate the fix** (the on-disk
  `tput/` sweep does); re-run and it should sit at ≤ 100 %.
- The generator (k6) runs on the same Docker host as the stack, so it competes for
  CPU — most relevant to the throughput test. Fine for a *relative* 1-vs-N
  comparison on this machine if kept consistent; a separate load box gives cleaner
  absolutes. Record the host specs above.
- Delivery latency floor is the ~2 s outbox poll, not a throughput limit — judge
  throughput by completeness, not raw latency.
