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
`loadtest/results/<Tag>/<scenario>/`; the default tag is `300m`.

1. **Connection ceiling** — hold sockets, send nothing; the largest all-green
   step is the limit:
   ```powershell
   ./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000,40000,60000
   ./loadtest/summarize.ps1  -Dir loadtest/results/300m/conn
   ```
2. **Message rate at a fixed socket count** — set `-Rooms`/`-Users` so
   `Rooms x Users` equals the connection count to test, then sweep the send
   interval:
   ```powershell
   ./loadtest/run-limits.ps1 -Scenario tput -Rooms 500 -Users 15 -Steps 10,8,6,4,3,2,1
   ./loadtest/summarize.ps1  -Dir loadtest/results/300m/tput
   ```
   The ceiling is the highest delivered `recv_per_s` at completeness >= 98 % and
   `ws_errors` within the teardown budget (`summarize.ps1 -WsErrTolerancePct`,
   default 0.5 % of sockets — absorbs k6's end-of-test teardown noise). Judge
   throughput by **completeness and tail latency**, not by delivered rate alone:
   the ~1 s outbox poll is a fixed latency floor, but a p95 well above it means the
   relay is already queueing.

`summarize.ps1 -Compare <dir>` puts two sweeps side by side.

**`-NativeK6`** runs the host `k6` instead of the container. Native k6 on Windows
draws from the dynamic port range (~16k sockets); to push past that, widen it as
admin first: `netsh int ipv4 set dynamicport tcp start=10000 num=55000`.

## Results — throughput (single instance @ 300 MB)

Measured on this machine with the generator on the same host, so treat the
absolutes as *relative*. Full table, diagnosis and open questions in
[`../LOAD_TEST_ANALYSIS.md`](../LOAD_TEST_ANALYSIS.md).

Earlier sweep (2 s relay poll, 100-row batches; re-run in progress against the
1 s / 500-row relay): 7,500 sockets (500 rooms x 15 users), send interval
10 -> 1 s.

- **Message-rate ceiling ~ 28,100 delivered msg/s** (1,875 sends/s x 15
  recipients) at 100 % completeness, but p95 is already 7.4 s there. The
  **latency-clean point is ~ 18,800 msg/s** (1,250 sends/s, p95 1.8 s).
- **Past the knee it degrades instead of plateauing:** at 2,500 sends/s delivery
  falls to 21,627 msg/s (57.7 % complete, p95 42 s); at 3,700 sends/s it is 5 %
  complete with ~3,100 socket errors (42 % of sockets).
- Handshakes were 100 % and connect p95 2 ms in every step, so the limit is the
  delivery path (outbox relay -> single hub goroutine -> socket writes), not the
  connection count.
- **Not measured yet:** the `conn` ceiling at 300 MB, and whether the collapse is
  an OOM kill. Capture `docker stats` and `docker inspect chat-server --format
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
