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
| `SEND_INTERVAL` | `20`                     | one message every N seconds (fractional ok; `0` = send nothing) |
| `MODE`          | `tput`                   | `conn` = hold sockets, send nothing; `tput` = send and measure |
| `HTTP_BASE`     | `http://localhost:80`    | nginx entry point (`WS_BASE` derived); the server is not directly reachable |

Total wall-clock per run is `RAMP + DURATION` (plus a ~15s graceful stop). The
default `RAMP=150 / DURATION=30` gives a ~3-minute active window that ramps for
2.5 min then holds for the last 30s. For the bigger cells raise `RAMP` further so
the per-second connect rate stays sane — e.g. 3000 sockets over the 150s ramp is
20 conn/s; bump to `RAMP=300` for ~10 conn/s.

## Finding the limits (single instance @ 600 MB)

`run-limits.ps1` + `summarize.ps1` answer two coupled questions for a server
pinned to a **600 MB** budget (`docker-compose.yml`): how many websockets fit,
and what message rate fits at that count. The infra ceilings were raised so the
**Go process**, not nginx or fds, is what bends first (nginx `worker_connections
65536`, container `nofile 262144`) — otherwise you only rediscover the old ~8192
nginx wall. Record results in [`BASELINE.md`](BASELINE.md).

**The generator is a containerized k6** on the compose network by default, so the
Windows ephemeral-port range (~16k) never caps the test below the server. Each
`run-limits.ps1` invocation recreates a clean stack and restarts the server
between steps (fresh heap per step).

1. **Connection ceiling** — hold sockets, send nothing; the largest all-green
   step is the limit:
   ```powershell
   ./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000,40000,60000
   ./loadtest/summarize.ps1  -Dir loadtest/results/600m/conn
   ```
2. **Message rate at that ceiling** — set `-Rooms`/`-Users` so `Rooms × Users`
   equals the connection ceiling you found, then sweep the send interval:
   ```powershell
   ./loadtest/run-limits.ps1 -Scenario tput -Rooms 400 -Users 25 -Steps 5,2,1,0.5,0.25
   ./loadtest/summarize.ps1  -Dir loadtest/results/600m/tput
   ```
   The ceiling is the highest delivered `recv_per_s` at completeness ≥ 98 % and
   `ws_errors` within the teardown budget (`summarize.ps1 -WsErrTolerancePct`,
   default 0.5 % of sockets — absorbs k6's end-of-test teardown noise). (Judge
   throughput by **completeness**, not latency — the
   ~2 s outbox poll is a fixed latency floor, not a throughput cap.)

`summarize.ps1 -Compare <dir>` puts two sweeps side by side — the future
1-instance vs N-instance view (the N-instance run needs the multi-instance
Broadcaster, not built yet).

**`-NativeK6`** runs the host `k6` instead of the container. Native k6 on Windows
draws from the dynamic port range (~16k sockets); to push past that, widen it as
admin first: `netsh int ipv4 set dynamicport tcp start=10000 num=55000`.

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
