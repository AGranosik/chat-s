# Load-test analysis — naive scale

**Architecture.** Three identical Go WebSocket services sit behind nginx, each with a local in-memory hub. PostgreSQL remains the message store and shared transactional outbox. A relay claims outbox rows and publishes room events through Redis Pub/Sub so each service can fan out to its own sockets.

Each k6 virtual user holds one socket and sends at the configured interval during a 30-second send window. `Delivered/s` is received frames divided by that window; completeness is received frames ÷ (sent messages × users per room). These values are from `loadtest/results/*/tput/`, on one machine.

| Scenario | Rooms × users | Sockets | Interval | Offered/s | Delivered/s | Complete | WS errors | E2E p50 / p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100r5u | 100 × 5 | 500 | 10 s | 50.0 | 250.0 | 100.0% | 0 | 1,005 / 1,809 ms |
| 100r5u | 100 × 5 | 500 | 4 s | 125.3 | 626.7 | 100.0% | 0 | 718 / 1,667 ms |
| 100r5u | 100 × 5 | 500 | 1 s | 500.0 | 2,500.2 | 100.0% | 0 | 807 / 1,519 ms |
| 500r2u | 500 × 2 | 1,000 | 10 s | 100.0 | 200.1 | 100.0% | 0 | 937 / 1,775 ms |
| 500r2u | 500 × 2 | 1,000 | 4 s | 249.5 | 498.9 | 100.0% | 0 | 899 / 1,715 ms |
| 500r2u | 500 × 2 | 1,000 | 1 s | 1,000.3 | 2,000.7 | 100.0% | 0 | 634 / 1,499 ms |
| 400r10u | 400 × 10 | 4,000 | 4 s | 841.3 | 7,242.2 | 86.1% | 1,483 | 8,334 / 9,556 ms |
| 400r10u | 400 × 10 | 4,000 | 3 s | 1,333.7 | 13,337.0 | 100.0% | 0 | 20,672 / 21,564 ms |
| 400r10u | 400 × 10 | 4,000 | 2 s | 1,578.6 | 8,163.7 | 51.7% | 3,201 | 18,294 / 22,544 ms |

## Conclusions

- At the matched 500- and 1,000-socket scenarios, delivery is complete at every stored interval. Tail latency is lower than single-instance in the recorded one-second cases.
- The 4,000-socket / 10-member-room three-second result achieves 13,337 delivered frames/s at 100% completeness, but p95 latency is 21.6 seconds; it does not meet the harness latency target.
- The two-second and recorded four-second 4,000-socket runs have many socket errors and incomplete delivery. Treat the three-second result as a throughput observation, not a proven low-latency operating point.
