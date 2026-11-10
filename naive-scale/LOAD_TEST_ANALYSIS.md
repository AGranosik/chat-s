# Load-test analysis — naive scale

**Architecture.** Identical Go WebSocket services sit behind nginx, each with a local in-memory hub (three at 200 MB for the first catalogs below; five and ten at 100 MB for the 7,500-socket sweep, where nginx also routes `/ws` by room). PostgreSQL remains the message store and shared transactional outbox. A relay claims outbox rows and publishes room events through Redis Pub/Sub so each service can fan out to its own sockets.

Each k6 virtual user holds one socket and sends at the configured interval during a 30-second send window. `Delivered/s` is received frames divided by that window; completeness is received frames ÷ (sent messages × users per room). These values are from `loadtest/results/*/tput/`, on one machine.

## 7,500 sockets, 5 vs 10 instances (500 rooms × 15 users, 100 MB each, room-affinity routing)

One run per point; same ramp (180 s), send window (30 s) and drain (30 s) for both. Instance count of `5x100m` is taken from the result directory tag.

| Interval | Offered/s (5 / 10) | Delivered/s (5 / 10) | Complete (5 / 10) | WS errors (5 / 10) | E2E p50 (5 / 10) | E2E p95 (5 / 10) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 s | 750 / 750 | 11,251 / 11,257 | 100% / 100% | 1 / 0 | 1,190 / 1,183 ms | 1,793 / 1,743 ms |
| 8 s | 932 / 938 | 13,976 / 14,070 | 100% / 100% | 0 / 3 | 1,157 / 1,225 ms | 1,770 / 1,781 ms |
| 6 s | 1,249 / 1,250 | 18,729 / 18,755 | 100% / 100% | 2 / 2 | 1,189 / 1,296 ms | 1,832 / 1,884 ms |
| 4 s | 1,879 / 1,871 | 28,183 / 28,060 | 100% / 100% | 1 / 0 | 13,155 / 3,159 ms | 18,356 / 10,126 ms |
| 3 s | 2,500 / 2,498 | 32,881 / 29,276 | 87.7% / 78.1% | 0 / 540 | 28,145 / 27,432 ms | 33,608 / 36,873 ms |
| 2 s | 3,733 / 3,709 | 10,425 / 8,682 | 18.6% / 15.6% | 1,734 / 3,144 | 46,917 / 31,270 ms | 52,745 / 53,246 ms |
| 1 s | 7,214 / 7,262 | 3,190 / 5,395 | 2.9% / 5.0% | 4,636 / 3,297 | 29,502 / 31,820 ms | 55,494 / 55,335 ms |

Handshakes were 100% with a 2 ms connect p95 at every step.

## Conclusions

- At 7,500 sockets both the 5- and 10-instance stacks meet all three criteria (completeness, error budget, latency) up to about 1,250 offered msg/s (about 18,750 delivered/s). The four-second step is lossless but queues, with p95 of 10–18 s, so it is not an operating point. From the three-second step completeness falls and socket errors begin.
- Doubling the instances did not move that ceiling; it only softened the four-second step (p50 13.2 s with 5 instances, 3.2 s with 10). The limit is therefore something shared by all instances, but no CPU, memory, Postgres or Redis metrics were recorded, so it is not attributed.
- Differences of a few percent between the two sweeps (for example the one-second step, 2.9% vs 5.0%) are within single-run noise.
- At the matched 500- and 1,000-socket scenarios, delivery is complete at every stored interval. Tail latency is lower than single-instance in the recorded one-second cases.
- The 4,000-socket / 10-member-room three-second result achieves 13,337 delivered frames/s at 100% completeness, but p95 latency is 21.6 seconds; it does not meet the harness latency target.
- The two-second and recorded four-second 4,000-socket runs have many socket errors and incomplete delivery. Treat the three-second result as a throughput observation, not a proven low-latency operating point.
- Full write-up, earlier 3 x 200 MB baseline and caveats: [`docs/LOAD_TESTS.md`](docs/LOAD_TESTS.md).
