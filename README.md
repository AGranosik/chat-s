# chat-s — chat scalability

A series of experiments in **how far a real-time chat backend can be pushed**, one
architecture at a time. Each approach is built on honest, production-shaped
infrastructure (a load balancer, a real database, a k6 load-test matrix) and then
measured to find *where it breaks and why* — so that the next approach can be
compared against it as a drop-in diff.

The question is always the same:

> **How many concurrent WebSocket connections and how much message throughput can
> this design sustain before latency degrades, handshakes reset, or the process
> falls over — and what is the actual bottleneck?**

## The approaches

| # | Approach | Status | Directory |
|---|----------|--------|-----------|
| 1 | **Single instance** — one Go process, in-memory hub, no message queue | ✅ built & measured | [`single-instance/`](single-instance/) |
| 2 | **Multi-instance / cross-node fan-out** — same core, Redis/Kafka `Broadcaster` behind the same seam | 🔜 planned | _tbd_ |

More approaches will be added as the series grows. Each keeps the same load-test
harness and baseline template so results stay directly comparable.

### 1. Single instance — the current experiment

The first approach asks: **how far does one process go?** One Go server behind one
nginx load balancer, holding every live WebSocket in an in-memory hub, with Postgres
as the system of record and a transactional outbox making "persist" and "broadcast"
atomic. It is deliberately single-instance with **no message queue** — that
constraint *is* the experiment: a clean design whose ceiling we can characterize
(file descriptors? accept backlog? memory? the outbox poll?), with a documented seam
to scale out later.

See [`single-instance/README.md`](single-instance/README.md) for how to run it and
[`single-instance/loadtest/`](single-instance/loadtest/) for the load-test matrix and
[`BASELINE.md`](single-instance/loadtest/BASELINE.md) — the fixed reference a future
multi-instance run will diff against.

> **Test-machine limit:** these numbers are bounded by my PC, not the design. WebSocket
> connections top out around **~40k** — beyond that the host machine (not the server) runs
> out of resources, so the true single-instance ceiling may be higher on bigger hardware.

### 2. Multi-instance and beyond — checked later

The single-instance design keeps a clean **scaling seam**: `chat.Service` depends on
a `Broadcaster` interface, not the concrete in-memory hub. Swapping in a Redis- or
Kafka-backed `Broadcaster` (of the *same* interface) turns one node into N without
rewriting the core. That — and any other approaches worth measuring — will be built
and run through the identical sweeps, then compared 1-vs-N with the recorded baseline.

## Repository layout

```
single-instance/   approach #1 — the single-instance chat service + load tests
log-prop/          reference project: Kafka publisher/consumer (the multi-node fan-out reference)
```

## License

See [`LICENSE`](LICENSE).
