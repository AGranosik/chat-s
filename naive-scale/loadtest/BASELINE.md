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
| Host (CPU / RAM / OS) | _____ |
| Outbox poll interval | ~2 s (latency floor), per instance |
| Commit / date | _____ |

## 1. WS connection ceiling

`./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000,40000,60000`
then `./loadtest/summarize.ps1 -Dir loadtest/results/3x200m/conn`.

The ceiling is the largest socket count with handshake% ≥ 99 and ws_errors
within the teardown budget (default 0.5 % of sockets; see
`summarize.ps1 -WsErrTolerancePct`), below the point where a **200 MB instance**
OOM-restarts. Sockets are spread ~evenly across the three instances by nginx, so
the per-instance socket count is roughly the total ÷ 3.

**Connection ceiling: _____ concurrent websockets (total across 3 instances).**

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

**Message-rate ceiling: _____ delivered msg/s at _____ sockets.**

<!-- paste summary.md from results/3x200m/tput here -->
| file | sockets | room_size | handshake_pct | ws_errors | conn_p95_ms | e2e_p50_ms | e2e_p95_ms | sent_per_s | recv_per_s | complete_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | | | | | | |

Notes (first bottleneck — completeness drop / unbounded latency / ws errors / **duplicate dispatch inflating completeness**): _____

## 3. vs single-instance (the 1-vs-N diff)

```powershell
./loadtest/summarize.ps1 -Dir loadtest/results/3x200m/tput `
  -Compare ../single-instance/loadtest/results/600m/tput
```

| metric | single-instance (600m) | naive-scale (3×200m) |
| --- | --- | --- |
| Connection ceiling (sockets) | _____ | _____ |
| Message-rate ceiling (delivered msg/s) | _____ | _____ |

Verdict (did splitting the same budget across 3 processes + Redis help, hurt, or
wash — after accounting for duplicate dispatch?): _____

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
