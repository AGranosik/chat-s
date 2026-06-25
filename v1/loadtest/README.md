# Load tests (k6)

WebSocket load test for the chat server. One k6 VU == one user == one long-lived
socket; each user sends one message every `SEND_INTERVAL` seconds. Connections
**ramp up over `RAMP` seconds** rather than all opening at once, then hold at full
load for `DURATION` seconds — opening N sockets on the same tick swamps the accept
backlog and most handshakes get reset, which masquerades as a server ceiling.

## Scenario matrix

| rooms | users/room | total VUs (= sockets) |
|------:|-----------:|----------------------:|
| 1     | 2 / 5 / 10 | 2 / 5 / 10            |
| 10    | 2 / 5 / 10 | 20 / 50 / 100         |
| 100   | 2 / 5 / 10 | 200 / 500 / 1000      |
| 1000  | 2 / 5 / 10 | 2000 / 5000 / 10000   |

## Prerequisites

- [k6](https://k6.io/docs/get-started/installation/) on `PATH`.
- The stack running and reachable through nginx. nginx (`http://localhost:80`) is
  the **only** published entry point — the Go server's `:8080` is internal to the
  compose network (`expose`, not `ports`), so the test always goes through the load
  balancer, exactly like a real client. There is no direct-to-server path to
  bypass.
  ```bash
  docker compose up        # nginx on :80 — the only reachable entry point
  ```

## Run

Single scenario:
```bash
k6 run -e ROOMS=10 -e USERS=5 -e RAMP=150 -e DURATION=30 loadtest/chat_load.js
```

Full matrix (writes `loadtest/results/<scenario>.json`):
```powershell
./loadtest/run-matrix.ps1
./loadtest/run-matrix.ps1 -Rooms 1,10 -Users 2,5 -Ramp 60 -Duration 60   # subset
```

## Parameters (env vars)

| var             | default                  | meaning                          |
|-----------------|--------------------------|----------------------------------|
| `ROOMS`         | `1`                      | number of rooms                  |
| `USERS`         | `2`                      | users per room                   |
| `RAMP`          | `150`                    | ramp connections up over N seconds |
| `DURATION`      | `30`                     | hold-at-full-load length, seconds |
| `SEND_INTERVAL` | `20`                     | one message every N seconds      |
| `HTTP_BASE`     | `http://localhost:80`    | nginx entry point (`WS_BASE` derived); the server is not directly reachable |

Total wall-clock per run is `RAMP + DURATION` (plus a ~15s graceful stop). The
default `RAMP=150 / DURATION=30` gives a ~3-minute active window that ramps for
2.5 min then holds for the last 30s. For the bigger cells raise `RAMP` further so
the per-second connect rate stays sane — e.g. 3000 sockets over the 150s ramp is
20 conn/s; bump to `RAMP=300` for ~10 conn/s.

## What it measures

- `msg_e2e_latency` — send→receive time per message. **This includes the outbox
  relay's poll interval (~2s).** The thresholds (`p95<2.5s`, `p99<3.5s`) assume
  the default 2s poll; loosen them if you change `pollInterval` in
  `internal/outbox/relay.go`.
- `ws_connecting` — websocket handshake time (`p95<1s`).
- `msgs_sent` / `msgs_received` — throughput counters.
- `ws_errors` + a handshake check (`checks rate>0.99`).

## Notes

- `setup()` creates the rooms and users over REST first (FK constraints require
  them) and shares their UUIDs with every VU. Users are reused across rooms.
- The biggest cell opens **10,000 sockets from the k6 host**. On Linux/macOS
  raise the descriptor limit (`ulimit -n 65535`) before running it; the server
  and its OS need headroom for the same count. Start small and climb the matrix.
- A non-zero k6 exit code means a threshold was breached — `run-matrix.ps1`
  reports it and continues to the next cell.
