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
- The stack running and reachable. By default the test points at nginx
  (`http://localhost:80`), which is the port `docker compose` publishes. Override
  `HTTP_BASE` to hit the Go server directly (`http://localhost:8080`, e.g. when
  running `go run ./cmd/server`):
  ```bash
  docker compose up        # nginx on :80 — default target
  # or: go run ./cmd/server  (binds :8080; set HTTP_BASE=http://localhost:8080)
  ```

## Run

Single scenario:
```bash
k6 run -e ROOMS=10 -e USERS=5 -e RAMP=30 -e DURATION=120 loadtest/chat_load.js
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
| `RAMP`          | `30`                     | ramp connections up over N seconds |
| `DURATION`      | `120`                    | hold-at-full-load length, seconds |
| `SEND_INTERVAL` | `20`                     | one message every N seconds      |
| `HTTP_BASE`     | `http://localhost:80`    | server base URL (`WS_BASE` derived) |

Total wall-clock per run is `RAMP + DURATION` (plus a ~15s graceful stop). For the
bigger cells raise `RAMP` so the per-second connect rate stays sane — e.g. 3000
sockets over a 30s ramp is 100 conn/s; bump to `RAMP=120` for ~25 conn/s.

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
