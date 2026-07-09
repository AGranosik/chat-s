# Load tests (k6) — naive-scale

WebSocket load test for the chat server, ported from `single-instance/loadtest`
and adapted to the naive-scale topology: **three server instances behind nginx,
200 MB each (600 MB aggregate)**, with cross-instance fan-out over **Redis
pub/sub**. The test itself is unchanged in spirit — one k6 VU == one user == one
long-lived socket; each user sends one message every `SEND_INTERVAL` seconds.
Connections **ramp up over `RAMP` seconds** rather than all opening at once, then
hold at full load for `DURATION` seconds — opening N sockets on the same tick
swamps the accept backlog and most handshakes get reset, which masquerades as a
server ceiling.

The generator drives nginx (`http://localhost:80`) exactly like a real client;
it never talks to an instance directly. nginx round-robins each new socket across
`server1..3`, so the members of a room end up spread across all three instances —
which is the whole point of testing the multi-instance path.

## What's different from single-instance

Same k6 script, same two questions (connection ceiling, message-rate ceiling).
The adaptations are all in the runner and the read-off:

- **`run-limits.ps1`** restarts `server1 server2 server3` between steps (not a
  single `server`) and resolves the compose network from `chat-server-1`. Default
  `-Tag` is `3x200m`.
- **Delivery path.** A message is persisted on whichever instance the sender is
  pinned to, its outbox relay picks it up on the **~2 s poll** (unchanged latency
  floor), and `hub.Broadcast` **publishes to Redis** `room:<id>`; every instance
  subscribed to that room re-delivers to its local clients. Net effect on the
  test: the same `p95<2.5s / p99<3.5s` latency bounds still apply (the 2 s poll
  dominates; the Redis hop is small).
- **Fixed wrinkle — duplicate dispatch (why old results read > 100 %).** All three
  instances run their **own** outbox relay against the **same** `outbox` table.
  Originally the relay used `FetchUndispatched` + `MarkDispatched` as two separate
  autocommitted statements, so under load two or three relays fetched the same
  rows before any marked them dispatched and each published to Redis — the row was
  **delivered 2–3×**, surfacing as **`complete_pct` above 100 %** (up to ~300 %)
  and an inflated `recv_per_s`. This is now fixed: the relay claims each batch with
  `storage.DispatchBatch` (`SELECT … FOR UPDATE SKIP LOCKED` + mark, in one tx), so
  the relays take disjoint batches and each row is dispatched exactly once (proven
  by `TestOutbox_ConcurrentRelaysDispatchEachRowOnce`). **The `tput` JSONs on disk
  predate the fix** — if you see 124 % completeness there, that's why; re-run the
  `tput` sweep and completeness should settle at ≤ 100 %, making the 1-vs-N
  `recv_per_s` compare honest.

## Prerequisites

- [k6](https://k6.io/docs/get-started/installation/) on `PATH` (only needed for
  `-NativeK6`; `run-limits.ps1` uses a containerized k6 by default).
- The stack running and reachable through nginx. nginx (`http://localhost:80`) is
  the **only** published entry point — each Go server's `:8080` is internal to the
  compose network (`expose`, not `ports`), so the test always goes through the
  load balancer, exactly like a real client.
  ```bash
  docker compose up        # postgres + redis + server1..3 + nginx on :80
  ```

## Run

Single scenario:
```bash
k6 run -e ROOMS=10 -e USERS=5 -e RAMP=150 -e DURATION=30 loadtest/chat_load.js
```

Full matrix (native k6; writes `loadtest/results/<scenario>.json`):
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
| `HTTP_BASE`     | `http://localhost:80`    | nginx entry point (`WS_BASE` derived); no instance is directly reachable |

Total wall-clock per run is `RAMP + DURATION` (plus a ~15 s graceful stop). The
default `RAMP=150 / DURATION=30` ramps for 2.5 min then holds for the last 30 s.
For the bigger cells raise `RAMP` so the per-second connect rate stays sane —
e.g. 3000 sockets over a 150 s ramp is 20 conn/s; bump to `RAMP=300` for ~10.

## Finding the limits (3 instances @ 200 MB each)

`run-limits.ps1` + `summarize.ps1` answer the two coupled questions for the
**3×200 MB** stack: how many websockets fit, and what message rate fits at that
count. The infra ceilings are raised so the **Go processes**, not nginx or fds,
bend first (nginx `worker_connections 65536`, container `nofile 262144`). Record
results in [`BASELINE.md`](BASELINE.md).

**The generator is a containerized k6** on the compose network by default, so the
Windows ephemeral-port range (~16k) never caps the test below the servers. Each
`run-limits.ps1` invocation recreates a clean stack and restarts all three
instances between steps (fresh heap per step, per instance).

1. **Connection ceiling** — hold sockets, send nothing; the largest all-green
   step is the limit:
   ```powershell
   ./loadtest/run-limits.ps1 -Scenario conn -Steps 5000,10000,20000,40000,60000
   ./loadtest/summarize.ps1  -Dir loadtest/results/3x200m/conn
   ```
2. **Message rate at that ceiling** — set `-Rooms`/`-Users` so `Rooms × Users`
   equals the connection ceiling you found, then sweep the send interval:
   ```powershell
   ./loadtest/run-limits.ps1 -Scenario tput -Rooms 400 -Users 25 -Steps 5,2,1,0.5,0.25
   ./loadtest/summarize.ps1  -Dir loadtest/results/3x200m/tput
   ```
   The ceiling is the highest delivered `recv_per_s` at completeness ≥ 98 % (and,
   with the dispatch fix in place, ≤ ~100 % — a value well over 100 % means you're
   on a pre-fix build; see the wrinkle above) with `ws_errors` within the teardown
   budget. Judge throughput by **completeness**, not latency — the ~2 s outbox poll
   is a fixed latency floor, not a cap.

**`-NativeK6`** runs the host `k6` instead of the container. Native k6 on Windows
draws from the dynamic port range (~16k sockets); to push past that, widen it as
admin first: `netsh int ipv4 set dynamicport tcp start=10000 num=55000`.

## Comparing against the single-instance baseline

The point of this port is the **1-vs-N diff**. Run the same sweep here, then put
it next to the single-instance numbers (in the sibling project):

```powershell
./loadtest/summarize.ps1 -Dir loadtest/results/3x200m/tput `
  -Compare ../single-instance/loadtest/results/600m/tput
```

Because the aggregate budget is the same (3×200m ≈ 600m), the honest question is
whether spreading it across three processes + Redis buys more delivered
throughput than one fat process — **once the duplicate-dispatch inflation is
accounted for or fixed**. Until then, compare connection ceilings (unaffected by
duplicates) freely, but read the tput compare with the wrinkle in mind.

## What it measures

- `msg_e2e_latency` — send→receive time per message. Includes the outbox relay's
  ~2 s poll (plus a small Redis hop). Thresholds (`p95<2.5s`, `p99<3.5s`) assume
  the default 2 s poll; loosen them if you change `pollInterval` in
  `internal/outbox/relay.go`.
- `ws_connecting` — websocket handshake time (`p95<1s`).
- `msgs_sent` / `msgs_received` — throughput counters. With the dispatch fix
  `received` should track `sent × room_size` (completeness ≤ ~100 %); a large
  excess means duplicate delivery (a pre-fix build — see the wrinkle above).
- `ws_errors` + a handshake check (`checks rate>0.99`).

## Notes

- `setup()` creates the rooms and users over REST first (FK constraints require
  them) and shares their UUIDs with every VU. Users are reused across rooms.
- The biggest cell opens **10,000 sockets from the k6 host**. On Linux/macOS raise
  the descriptor limit (`ulimit -n 65535`) before running it; the servers and
  their OS need headroom for the same count. Start small and climb the matrix.
- A non-zero k6 exit code means a threshold was breached — the runners report it
  and continue to the next cell.
