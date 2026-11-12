# Load tests (k6)

WebSocket load test for the chat server. One k6 VU == one user == one long-lived
socket; each user sends one message every `SEND_INTERVAL` seconds. Connections
**ramp up over `RAMP` seconds** rather than all opening at once, then hold at full
load for `DURATION` seconds — opening N sockets on the same tick swamps the accept
backlog and most handshakes get reset, which masquerades as a server ceiling.

## Scenario presets

| script | rooms | users/room | sockets |
|--------|------:|-----------:|--------:|
| `run-clean.ps1` (default) | 10 | 5 | 50 |
| `run-clean.ps1 -Full` | 300 / 1000 | 10 | 3,000 / 10,000 |
| `run-matrix.ps1` (default) | 10 / 100 / 100 | 5 / 10 / 100 | every rooms × users pair, 50 – 10,000 |
| `run-limits.ps1 -Scenario tput` | `-Rooms` (100) | `-Users` (50) | 5,000 by default; the committed sweep used 500 × 15 |
| `run-limits.ps1 -Scenario conn` | sockets ÷ `-ConnUsers` | `-ConnUsers` (2) | each `-Steps` value |

`run-matrix.ps1` lists 100 rooms twice in its defaults, so the 100-room cells run
twice and the second run overwrites the first one's JSON.

## Prerequisites

Throughput runs do not send during connection ramp-up: they ramp, settle, send
for `DURATION`, then count arrivals during `DRAIN` toward delivery completeness.
This keeps early sends from being judged against room members that have not
connected yet and prevents the final send burst from being cut off at test end.

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

Full matrix (writes `loadtest/results/rooms<R>_users<U>.json`):
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
| `SETTLE`        | `5`                      | wait after ramp before sending in `tput` mode |
| `DURATION`      | `30`                     | hold-at-full-load length, seconds |
| `DRAIN`         | `30`                     | keep sockets open after sending stops in `tput` mode |
| `SEND_INTERVAL` | `20`                     | one message every N seconds (fractional ok; `0` = send nothing) |
| `MODE`          | `tput`                   | `conn` = hold sockets, send nothing; `tput` = send and measure |
| `HTTP_BASE`     | `http://localhost:80`    | nginx entry point (`WS_BASE` derived); the server is not directly reachable |

Throughput wall-clock per run is `RAMP + SETTLE + DURATION + DRAIN` (plus a short
ramp-down); connection-only mode skips settle and drain. The default
`RAMP=150 / SETTLE=5 / DURATION=30 / DRAIN=30` takes about 3.6 minutes. For the bigger cells raise `RAMP` further so
the per-second connect rate stays sane — e.g. 3000 sockets over the 150s ramp is
20 conn/s; bump to `RAMP=300` for ~10 conn/s.

## Finding the limits (single instance @ 300 MB)

`run-limits.ps1` + `summarize.ps1` answer two coupled questions for a server
pinned to a **300 MB / 8 CPU** budget (`docker-compose.yml`; Postgres 600 MB / 2 CPU,
nginx 512 MB / 1 CPU): how many websockets fit, and what message rate fits at that
count. The infra ceilings were raised so the **Go process**, not nginx or fds, is
what bends first (nginx `worker_connections 65536`, container `nofile 262144`).

**The generator is a containerized k6** on the compose network by default, so the
Windows ephemeral-port range (~16k) never caps the test below the server.
`run-limits.ps1` builds the images once, then before **every step** wipes the
stack (`docker compose down -v`: containers and volumes, so Postgres starts empty)
and brings it back up, waiting until the server container and nginx both answer
`/healthz`. Every step therefore starts from a fresh process and an empty DB;
`-NoClean` wipes only before the first step. Results go to
`loadtest/results/<Tag>/<scenario>/`; the default tag is `150m`.

1. **Connection ceiling** — hold sockets, send nothing; the largest all-green
   step is the limit:
   ```powershell
   ./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000,40000,60000
   ./loadtest/summarize.ps1  -Dir loadtest/results/150m/conn
   ```
2. **Message rate at a fixed socket count** — set `-Rooms`/`-Users` so
   `Rooms x Users` equals the connection count to test, then sweep the send
   interval:
   ```powershell
   ./loadtest/run-limits.ps1 -Scenario tput -Rooms 500 -Users 15 -Steps 10,8,6,4,3,2,1
   ./loadtest/summarize.ps1  -Dir loadtest/results/150m/tput
   ```
   The ceiling is the highest delivered `recv_per_s` at completeness >= 98 % and
   `ws_errors` within the teardown budget (`summarize.ps1 -WsErrTolerancePct`,
   default 0.5 % of sockets — absorbs k6's end-of-test teardown noise). Judge
   throughput by **completeness and tail latency**, not by delivered rate alone:
   the ~1 s outbox poll is a fixed latency floor, but a p95 well above it means the
   relay is already queueing.

   `recv_per_s` is received frames ÷ `deliver_s`, the time delivery actually
   took. When a step's p95 is under `-LatencyP95Ms` (default 2500, the same as the
   k6 threshold), `deliver_s` is the `DURATION` send window. Otherwise messages
   queued and kept arriving after sending stopped. `deliver_s` is then `DURATION`
   plus the slowest message's latency, capped at `DRAIN`, because sockets close at
   the end of the drain.

`summarize.ps1 -Compare <dir>` puts two sweeps side by side.

**`-NativeK6`** runs the host `k6` instead of the container. Native k6 on Windows
draws from the dynamic port range (~16k sockets); to push past that, widen it as
admin first: `netsh int ipv4 set dynamicport tcp start=10000 num=55000`.

## Results — throughput (single instance @ 300 MB)

Measured on this machine with the generator on the same host, so treat the
absolutes as *relative*. Full table, diagnosis and open questions in
[`../LOAD_TEST_ANALYSIS.md`](../LOAD_TEST_ANALYSIS.md).

Sweep committed 2026-11-11 (`results/300m/tput/`): 7,500 sockets (500 rooms x
15 users), send interval 10 -> 1 s, stack wiped before every step.

- **Sustainable ceiling ~ 28,100 delivered msg/s** (1,873 sends/s x 15
  recipients) at the 4 s step: 100 % complete, p95 2.0 s, max 2.8 s, every k6
  threshold green. The **latency-clean point is ~ 18,750 msg/s** (1,250 sends/s,
  p95 1.24 s).
- **The 3 s step is complete but not sustainable:** 37,510 msg/s delivered, but
  p95 22 s. The backlog only cleared because senders stopped after 30 s.
- **Past that it collapses:** at 3,744 sends/s delivery is 59.8 % complete with
  1,455 socket errors (19 % of sockets). At 7,363 sends/s it is 4.5 % complete
  with 4,742 errors (63 %).
- Handshakes were 100 % and connect p95 2 ms in every step, so the limit is not the
  connection count. The drops look like the server's 60 s pong deadline: the
  shortest session in both failed steps is exactly 60.0 s.
- **Not measured yet:** the `conn` ceiling at 300 MB, and which component
  saturates first. Capture `docker stats` and `docker inspect chat-server --format
  '{{.RestartCount}} {{.State.OOMKilled}}'` per step on the next run.

## What it measures

- `msg_e2e_latency` — send→receive time per message. **This includes the outbox
  relay's poll interval (~1s).** The thresholds (`p95<2.5s`, `p99<3.5s`) assume
  the default 1s poll; loosen them if you change `pollInterval` in
  `internal/outbox/relay.go`.
- `ws_connecting` — websocket handshake time (`p95<1s`).
- `msgs_sent` / `msgs_received` — throughput counters.
- `ws_errors` + a handshake check (`checks rate>0.99`).
- Throughput runs also record sent/received counters; `summarize.ps1` derives
  delivery completeness = received / (sent x users per room).

## Notes

- `setup()` creates the rooms and users over REST first (FK constraints require
  them) and shares their UUIDs with every VU. Users are reused across rooms.
- The biggest cell opens **10,000 sockets from the k6 host**. On Linux/macOS
  raise the descriptor limit (`ulimit -n 65535`) before running it; the server
  and its OS need headroom for the same count. Start small and climb the matrix.
- A non-zero k6 exit code means a threshold was breached — `run-matrix.ps1`
  reports it and continues to the next cell.
