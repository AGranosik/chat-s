# Load-test baseline — proper-scale

Record the machine and run date with every sweep. Use the same host and k6 settings when comparing this stack with the single-instance and naive-scale variants.

## System under test

| | |
| --- | --- |
| Message-service instances | 3 behind nginx, WebSocket room affinity enabled |
| Message-service memory | 165 MB each, 495 MB aggregate |
| Presence service | 80 MB limit |
| Delivery service | 80 MB limit |
| Outbox relay | 100 MB limit |
| PostgreSQL | 600 MB limit |
| Redis | 200 MB limit |
| Kafka | no memory limit configured |
| nginx | 512 MB limit; `worker_connections` 65536; `nofile` 262144 |
| Message-service `nofile` / `somaxconn` | 262144 / 65535 |
| Generator | containerized k6 on the Compose network by default |
| Host (CPU / RAM / OS) | _____ |
| Outbox poll interval | 1 s default |
| Commit / date | _____ |

The 495 MB message-service budget is lower than the 600 MB used by the other variants. Proper-scale has another 260 MB in its presence, delivery, and relay services, plus PostgreSQL, Redis, Kafka, and nginx. This is not a same-total-container-memory comparison; report the service budgets with the results.

## 1. WebSocket connection ceiling

Run `./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000,40000,60000`, then summarize `loadtest/results/3x165m-proper/conn`.

**Connection ceiling: _____ concurrent WebSockets.**

| file | sockets | room_size | handshake_pct | ws_errors | conn_p95_ms | e2e_p50_ms | e2e_p95_ms | sent_per_s | recv_per_s | complete_pct |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| | | | | | | | | | | |

Notes on where it broke: _____

## 2. Message-rate ceiling at fixed sockets

Set `Rooms × Users` to the connection ceiling from section 1, then sweep `SEND_INTERVAL`. The default example tests 10,000 sockets in 1,000 rooms of 10 users.

**Message-rate ceiling: _____ delivered msg/s at _____ sockets and _____% completeness.**

| file | sockets | room_size | handshake_pct | ws_errors | conn_p95_ms | e2e_p50_ms | e2e_p95_ms | sent_per_s | recv_per_s | complete_pct |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| | | | | | | | | | | |

Notes on the first bottleneck: _____

## 3. Comparison

Compare the same send-interval steps with `summarize.ps1 -Compare` and record the machine, commit, aggregate message-service memory, and supporting service limits for each run.

| metric | single-instance | naive-scale | proper-scale |
| --- | ---: | ---: | ---: |
| Connection ceiling | _____ | _____ | not measured |
| Delivered msg/s at ≥98% completeness | _____ | _____ | not measured |
| Completeness at ceiling | _____ | _____ | not measured |
| Message-service aggregate memory | 600 MB | 600 MB | 495 MB |

## Caveats

- E2E latency covers PostgreSQL acceptance, the outbox poll, Kafka publication and consumption, presence lookup, Redis publication, and WebSocket delivery. It is expected to exceed the single-instance and naive-scale paths.
- The k6 generator runs on the same Docker host by default and competes for CPU. Keep the generator placement consistent for relative comparisons.
- Room members are assigned to one message-service instance by nginx's consistent hash. The reported connection ceiling is fleet-wide; record the per-container memory and CPU when interpreting a failing step.
- A completeness value above 100% can indicate duplicate delivery. Check the Kafka consumer and delivery path before treating the additional receives as throughput.
