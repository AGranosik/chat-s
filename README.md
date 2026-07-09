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
| 2 | **Naive scale** — three copies of the same process behind nginx, Redis pub/sub fan-out | ✅ built & measured | [`naive-scale/`](naive-scale/) |

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

### 2. Naive scale — just add boxes

The second approach is the "just add more boxes" reflex, done as naively as possible
on purpose: **three copies** of the same Go process behind the same nginx, each pinned
to **200 MB** (so the fleet consumes the same memory the single instance had). The
single-instance design kept a clean **scaling seam** — `chat.Service` depends on a
`Broadcaster` interface, not the concrete in-memory hub — so the only real change is
a Redis pub/sub `Broadcaster` behind that same interface, closing the gap where a
message persisted on one instance would only reach that instance's clients.

**Almost no optimization went into it.** The one exception is correctness, not
speed: all three relays poll the same outbox table, so each batch is claimed with
`FOR UPDATE SKIP LOCKED` — without it every message was dispatched 2–3×. Everything
else is the single-instance code, copied.

See [`naive-scale/README.md`](naive-scale/README.md) and
[`naive-scale/loadtest/`](naive-scale/loadtest/) for the identical sweep matrix.

## Conclusions so far: single-instance toughness vs naive scale

This is a controlled experiment: **same machine, same k6 harness, same total memory
for the Go service** (1×600 MB vs 3×200 MB), same Postgres and nginx, same test —
10 000 WebSockets in rooms of 10, sweeping the send interval down until delivery
breaks. The only variable is one process vs three processes + Redis pub/sub.

**Result: the naive scale-out delivers ~54 % more messages per second before breaking.**

| stack | delivered ceiling | completeness at ceiling |
|---|---|---|
| single-instance (1×600 MB) | ≈ 5 542 msg/s | 98.7 % |
| naive-scale (3×200 MB + Redis) | ≈ 8 537 msg/s | 99.1 % |

Head-to-head at the same offered rates (delivered msg/s @ completeness):

| offered rate (send interval) | single-instance | naive-scale |
|---|---|---|
| ~560 sent/s (9 s) | 5 542 @ 98.7 % | 5 573 @ 99.6 % |
| ~630 sent/s (8 s) | 6 161 @ 97.9 % | 6 346 @ 99.8 % |
| ~730 sent/s (7 s) | 6 791 @ 92.5 % | 7 280 @ 99.6 % |
| ~860 sent/s (6 s) | 5 634 @ 66.1 % | 8 537 @ 99.1 % |
| ~1 300 sent/s (4 s) | 0 @ 0 % | 9 100 @ 70 % |

Both stacks fail the same way — handshakes stay at 100 % throughout, but past the
ceiling delivery stops tracking the offered rate and completeness collapses. The
difference is *where* and *how gracefully*: single-instance starts slipping around
~630 sent/s and is delivering nothing by ~1 300 sent/s; naive-scale stays ≥ 99 %
complete two sweep steps deeper and, even one step past its own ceiling, still
delivers 9 100 msg/s (at degraded completeness) instead of falling over.

**Why naive-scale wins:** the expensive part of chat throughput is not persisting a
message — it's the fan-out, writing each message to every socket in the room. In
single-instance all 10 000 sockets hang off one hub in one process, so one process
does all ~8 500 socket writes/s. Splitting the sockets across three processes splits
that write work (and the CPU it burns) three ways, while Redis pub/sub routes each
message only to the instances that hold members of its room. The shared Postgres
outbox was not the bottleneck at these rates.

**The caveat that keeps this honest:** naive-scale's advantage is not free and does
not extend indefinitely. Every published message is delivered by Redis to *each*
instance holding a member of that room — with round-robin balancing, a 10-member
room's sockets land on all three instances, so every message crosses the network
roughly once per instance. That per-message fan-out cost grows with the instance
count, and single-threaded Redis is a serialization point. Somewhere past this
scale there is a point where adding instances makes the pub/sub fan-out itself the
bottleneck and naive scale starts performing **worse** — finding that crossover is
the next experiment.

> Numbers are this-machine-only (the k6 generator shares the host CPU with the
> stack), so treat the absolute msg/s as relative — the shape of the comparison is
> the takeaway, not the constants.

## Repository layout

```
single-instance/   approach #1 — the single-instance chat service + load tests
naive-scale/       approach #2 — 3 replicas + Redis pub/sub fan-out + load tests
log-prop/          reference project: Kafka publisher/consumer (the multi-node fan-out reference)
```

## License

See [`LICENSE`](LICENSE).
