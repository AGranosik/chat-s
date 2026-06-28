# Load-test baseline — single instance @ 600 MB

> Template. Run the sweeps (`run-limits.ps1`), then paste `summarize.ps1` output
> into the tables below. The point is a fixed, recorded reference so a later
> multi-instance run is a drop-in diff. Fill every `_____`.

## System under test

| | |
| --- | --- |
| Instances | 1 |
| Server memory budget | **600 MB** (`docker-compose.yml` `server.deploy.resources.limits.memory`) |
| Server `nofile` | 262144 |
| nginx `worker_connections` / `worker_rlimit_nofile` | 65536 / 262144 |
| nginx memory | 512 MB |
| `net.core.somaxconn` | 65535 |
| Generator | containerized k6 (`grafana/k6`, on the compose network) |
| Host (CPU / RAM / OS) | _____ |
| Outbox poll interval | ~2 s (latency floor) |
| Commit / date | _____ |

## 1. WS connection ceiling

`./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000,40000,60000`
then `./loadtest/summarize.ps1 -Dir loadtest/results/600m/conn`.

The ceiling is the largest socket count with handshake% ≥ 99 and ws_errors = 0
(below the point where the 600 MB server OOM-restarts).

**Connection ceiling: _____ concurrent websockets.**

<!-- paste summary.md from results/600m/conn here -->
| file | sockets | room_size | handshake_pct | ws_errors | conn_p95_ms | e2e_p50_ms | e2e_p95_ms | sent_per_s | recv_per_s | complete_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | | | | | | |

Notes (where/how it broke — handshake failures vs OOM restart vs latency): _____

## 2. Message rate at that ceiling

Set `-Rooms`/`-Users` so `Rooms × Users` = the connection ceiling from §1, then
sweep the send interval:
`./loadtest/run-limits.ps1 -Scenario tput -Rooms ___ -Users ___ -Steps 5,2,1,0.5,0.25`
then `./loadtest/summarize.ps1 -Dir loadtest/results/600m/tput`.

The ceiling is the highest delivered `recv_per_s` with completeness ≥ 98 % and
ws_errors = 0.

**Message-rate ceiling: _____ delivered msg/s at _____ sockets.**

<!-- paste summary.md from results/600m/tput here -->
| file | sockets | room_size | handshake_pct | ws_errors | conn_p95_ms | e2e_p50_ms | e2e_p95_ms | sent_per_s | recv_per_s | complete_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | | | | | | |

Notes (first bottleneck — completeness drop / unbounded latency / ws errors): _____

## Caveats

- The generator (k6) runs on the same Docker host as the stack, so it competes
  for CPU — most relevant to the throughput test. Fine for *relative* 1-vs-N
  comparison on this machine if kept consistent; a separate load box would give
  cleaner absolute numbers. Record the host specs above.
- Delivery latency floor is the ~2 s outbox poll, not a throughput limit — judge
  throughput by completeness, not raw latency.
- The N-instance comparison needs the multi-instance Broadcaster (the Redis/Kafka
  scaling seam), which is not implemented yet. When it is, re-run the same sweeps
  with `-Tag <N>-instance` and diff with `summarize.ps1 -Compare`.
