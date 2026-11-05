# Load-test summary — three chat architectures

This comparison uses committed k6 `tput` result JSONs. A k6 VU is one connected user; every sent room message should be received by every user in that room. `Delivered/s` is all received frames over the 30-second send window. Completeness is `received / (sent × users per room)`. End-to-end latency includes the transactional-outbox polling delay where applicable.

The 100r5u catalog is the only fully matched three-way interval sweep. The other two catalogs have different stored interval steps and several runs contain substantial WebSocket failures. In each table, slash-separated values follow the order in **Test intervals**; a dash means there is no committed result for that step.

## 100r5u — 100 rooms × 5 users (500 sockets)

| Architecture approach | Test intervals | Offered msg/s | Delivered msg/s | Completeness | WS errors | E2E p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Single instance — one in-memory hub; PostgreSQL outbox; local relay | 10 / 4 / 1 s | 50.0 / 124.9 / 500.1 | 250.2 / 624.3 / 2,500.7 | 100.0% / 100.0% / 100.0% | 0 / 0 / 0 | 1.86 / 1.81 / 1.89 s |
| Naive scale — three local hubs; PostgreSQL outbox; Redis Pub/Sub | 10 / 4 / 1 s | 50.0 / 125.3 / 500.0 | 250.0 / 626.7 / 2,500.2 | 100.0% / 100.0% / 100.0% | 0 / 0 / 0 | 1.81 / 1.67 / 1.52 s |
| Proper scale — three message services; presence; outbox → Kafka → delivery → Redis | 10 / 4 / 1 s | 50.0 / 125.2 / 500.2 | 250.2 / 626.2 / 316.5 | 100.0% / 100.0% / 12.7% | 0 / 0 / 0 | 2.41 / 25.60 / 55.90 s |

## 500r2u — 500 rooms × 2 users (1,000 sockets)

| Architecture approach | Test intervals | Offered msg/s | Delivered msg/s | Completeness | WS errors | E2E p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Single instance — one in-memory hub; PostgreSQL outbox; local relay | 10 / 4 / 1 s | 100.1 / 248.9 / 1,000.3 | 200.2 / 497.9 / 2,000.5 | 100.0% / 100.0% / 100.0% | 0 / 0 / 0 | 1.90 / 1.78 / 20.77 s |
| Naive scale — three local hubs; PostgreSQL outbox; Redis Pub/Sub | 10 / 4 / 1 s | 100.0 / 249.5 / 1,000.3 | 200.1 / 498.9 / 2,000.7 | 100.0% / 100.0% / 100.0% | 0 / 0 / 0 | 1.78 / 1.72 / 1.50 s |
| Proper scale — three message services; presence; outbox → Kafka → delivery → Redis | 20 / 4 / 3 / 2 s | 50.2 / 249.4 / 333.4 / 500.1 | 502.3 / 306.4 / 120.0 / 131.2 | 100.0% / 61.4% / 18.0% / 13.1% | 0 / 0 / 0 / 0 | 2.92 / 39.29 / 53.72 / 54.98 s |

The comparison at 10 s and 4 s shows complete delivery for single-instance and naive-scale. Proper-scale is healthy at its lower 20-second offered rate, but is already incomplete at the four-second step. The single-instance one-second result is complete but has a 20.77-second p95, so it is not a low-latency operating point.

## 400r10u — 400 rooms × 10 users (4,000 sockets)

| Architecture approach | Test intervals | Offered msg/s | Delivered msg/s | Completeness | WS errors | E2E p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Single instance — one in-memory hub; PostgreSQL outbox; local relay | 4 / 3 / 2 s | 999.9 / 1,319.3 / 1,939.1 | 4,724.8 / 2,070.5 / 245.7 | 47.3% / 15.7% / 1.3% | 0 / 1,313 / 1,305 | 45.52 / 53.11 / 56.14 s |
| Naive scale — three local hubs; PostgreSQL outbox; Redis Pub/Sub | 4 / 3 / 2 s | 841.3 / 1,333.7 / 1,578.6 | 7,242.2 / 13,337.0 / 8,163.7 | 86.1% / 100.0% / 51.7% | 1,483 / 0 / 3,201 | 9.56 / 21.56 / 22.54 s |
| Proper scale — three message services; presence; outbox → Kafka → delivery → Redis | 4 / 3 / 2 s | 19.3 / 78.4 / 79.0 | 139.1 / 280.9 / 260.7 | 72.1% / 35.8% / 33.0% | 3,924 / 3,797 / 3,842 | 35.06 / 34.92 / 42.40 s |

All three 4,000-socket catalogs are stress/failure evidence rather than clean capacity points. Naive-scale has one complete-delivery run at the three-second interval, but its 21.56-second p95 is far above the configured latency target. For proper-scale, the actual accepted send rate in this catalog was much lower than the nominal rate because most sockets had already failed; its delivered/s figures are therefore not directly comparable to the healthy runs.

## Conclusions

- The single-instance and naive-scale stacks are effectively tied on delivered rate at 500 clients: both deliver the expected five frames per sent message, even at 500 sent messages/s. Naive scale has modestly lower tail latency in the one-second run.
- At 1,000 clients / two-user rooms, both of those architectures again complete delivery at their stored one-second run (about 2,000 delivered frames/s). Single-instance p95 latency rises to 20.8 seconds, while naive-scale remains 1.5 seconds in the recorded artifact. That supports splitting socket fan-out across replicas, but remains one machine and one run per point.
- The current proper-scale artifacts show that added components have not yet translated into load capacity. Its 500-client four-second run keeps completeness but breaches the latency target by an order of magnitude; its one-second run collapses to 12.7% completeness. This is an observation of the integrated system, not proof that Kafka, Redis, or a particular service is the bottleneck.
- Do not use delivered frames/s alone to declare a ceiling. Several runs retain high or full delivery while latency is tens of seconds; others have low actual send rates because sockets already failed. A usable operating point must meet **all three**: completeness, WebSocket-error budget, and latency target.

## Next measurement to make

Re-run a single, versioned matrix across all three stacks: identical rooms, users per room, ramp, 30-second send/drain windows, intervals, generator placement, and explicit resource limits. Record service CPU, memory, PostgreSQL, Redis, Kafka, outbox backlog, accepted sends, disconnect reasons, and delivery completeness. That will turn the current directional evidence into an attributable architecture comparison.

## Detailed result tables

- [Single-instance analysis](single-instance/LOAD_TEST_ANALYSIS.md)
- [Naive-scale analysis](naive-scale/LOAD_TEST_ANALYSIS.md)
- [Proper-scale analysis](proper-scale/LOAD_TEST_ANALYSIS.md)
