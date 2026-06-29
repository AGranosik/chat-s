---
name: loadtest-analysis
description: Analyze k6 load-test results for chat-s — read a sweep dir under loadtest/results/, find the connection or message-rate ceiling, diagnose HOW it broke (thundering-herd handshakes / k6 shutdown artifact / 600 MB OOM restart / latency floor / completeness drop), and optionally fill BASELINE.md. Use when asked to analyze/interpret load-test results, find the limit, or compare two sweeps. Reads results only — it does not run the sweeps.
---

# loadtest-analysis

Interpret a finished k6 sweep and answer the two questions the harness exists to
answer (see `loadtest/README.md`): **how many websockets fit**, and **what
message rate fits at that count**, for the single instance pinned to **600 MB**.

This skill is **read-only analysis**. It does *not* run the load — that's
`run-limits.ps1`, which recreates the docker stack, takes minutes per step, and
**wipes the DB**. If the results dir you need doesn't exist yet, stop and tell
the user the exact `run-limits.ps1` command to produce it; don't kick off a sweep
on your own.

## Inputs
A sweep directory of k6 `--summary-export` JSONs, one per step:
`loadtest/results/<tag>/<scenario>/` (e.g. `loadtest/results/600m/conn`,
`.../600m/tput`). Scenario is `conn` (hold sockets, send nothing) or `tput` (fix
sockets, sweep the send interval). If the user names a tag/scenario, build the
path; otherwise list what's under `loadtest/results/` and ask which sweep.

## Steps

1. **Summarize.** `summarize.ps1` already encodes the canonical read-off — run it
   first and read its table + the green/yellow `hint:` line. Run from `v1/`.
   ```powershell
   ./loadtest/summarize.ps1 -Dir loadtest/results/600m/conn
   ./loadtest/summarize.ps1 -Dir loadtest/results/600m/tput
   ./loadtest/summarize.ps1 -Dir <baseline> -Compare <other>   # 1-vs-N side by side
   ```
   It also writes `summary.md` / `summary.csv` next to the JSONs. Each row gives:
   `sockets`, `room_size`, `handshake_pct`, `ws_errors`, `conn_p95_ms`,
   `e2e_p50_ms`/`e2e_p95_ms`, `sent_per_s` (offered), `recv_per_s` (delivered),
   `complete_pct` = received / (sent × room_size).

2. **Read off the ceiling** using the same rules as the summarizer:
   - **conn sweep** → connection ceiling = the **largest socket count** that
     stayed green: `handshake_pct ≥ 99` **and** `ws_errors = 0`, below the point
     where the 600 MB server OOM-restarts.
   - **tput sweep** → message-rate ceiling = the **highest delivered `recv_per_s`**
     with `complete_pct ≥ 98` **and** `ws_errors = 0`. Judge throughput by
     **completeness, not latency** — the ~2 s outbox poll
     (`internal/outbox/relay.go`) is a fixed latency floor, so e2e p50/p95 sitting
     near 2 s is expected and is **not** a throughput cap.

3. **Diagnose *how* it broke** at the first non-green step. Distinguish:
   - **Thundering-herd handshakes** — `handshake_pct` drops while sockets open.
     Usually the ramp was too steep (sockets/`RAMP` too high), not a server wall;
     opening N sockets on one tick swamps the accept backlog. Recommend a longer
     `RAMP` before calling it a ceiling (cf. [[loadtest-ramp-config]]).
   - **k6 shutdown artifact** — a *small* `ws_errors` count and/or a handshake
     dip that appears only at the **largest** VU counts, with `complete_pct` still
     healthy, is k6 tearing down sockets at end-of-test, **not** a server limit.
     Do not fail a step on a handful of teardown `ws_errors` alone — corroborate
     with `handshake_pct` and `complete_pct` (cf. [[loadtest-handshake-failures-are-artifact]]).
   - **600 MB OOM restart** — the real server ceiling. Symptom: a step that was
     trending fine suddenly collapses (handshakes/completeness fall off a cliff).
     Confirm from the container, not just the JSON:
     ```powershell
     docker inspect chat-server --format '{{.RestartCount}} {{.State.OOMKilled}}'
     docker compose logs --tail 80 server   # look for restart / OOM near the step time
     ```
     (Only possible if the stack is still up, e.g. the sweep ran with `-KeepUp`.)
   - **Completeness drop (tput)** — `recv_per_s` stops tracking `sent_per_s` and
     `complete_pct` falls below 98 %: the relay/broadcast can't keep up with the
     offered rate. That step is past the message-rate ceiling.

4. **Go deeper into a single step only when needed.** Each JSON has
   `setup_data.rooms`/`.users` (sockets = rooms × users) and a `metrics` object
   in **milliseconds**: `ws_connecting`, `msg_e2e_latency` (`med`, `p(95)`, `max`),
   `msgs_sent`/`msgs_received` (`count`, `rate`), `ws_errors.count`, `vus_max`.
   The files are large (~250 KB+) — read the `metrics` block by offset (grep for
   `"metrics"`), don't load the whole UUID list. **Gotcha:** a metric's
   `thresholds` boolean in this export is `true` when the threshold was *breached*;
   trust the raw values and summarize.ps1's read-off, not that flag.

## Output
1. **Verdict** — one line: the ceiling. e.g. "Connection ceiling ≥ 10 000
   sockets (largest all-green step)." or "Message-rate ceiling ≈ N delivered
   msg/s at M sockets."
2. **Evidence** — the summarize.ps1 table (or the relevant rows).
3. **How it broke** — the diagnosis from step 3, naming the mechanism.
4. **Caveats** — generator shares the host CPU (matters most for tput); ~2 s
   latency floor; absolute numbers are this-machine-only, so 1-vs-N is a *relative*
   diff (see `BASELINE.md` caveats).

If asked to record it, fill the matching section of `loadtest/BASELINE.md` (paste
`summary.md`, set the `_____` ceiling and "where it broke" notes). Editing
BASELINE.md is fine; **never** edit the per-run result JSONs.

## Scope
Reads `loadtest/results/**` for the **v1** server only. Running sweeps is
`run-limits.ps1`; running the matrix is `run-matrix.ps1`; this skill consumes
their output.
