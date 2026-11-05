# Load-test analysis — proper scale

**Architecture.** Three message-service replicas handle WebSockets behind nginx. Presence is a separate service; PostgreSQL outbox rows are relayed to Kafka; a delivery service consumes Kafka and uses Redis-backed delivery. This adds explicit service boundaries and asynchronous stages to the message path.

Each k6 virtual user holds one socket and sends at the configured interval during a 30-second send window. `Delivered/s` is received frames divided by that window; completeness is received frames ÷ (sent messages × users per room). Values are from `loadtest/results/*/tput/`, measured on one machine.

| Scenario | Rooms × users | Sockets | Interval | Offered/s | Delivered/s | Complete | WS errors | E2E p50 / p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100r5u | 100 × 5 | 500 | 10 s | 50.0 | 250.2 | 100.0% | 0 | 1,017 / 2,414 ms |
| 100r5u | 100 × 5 | 500 | 4 s | 125.2 | 626.2 | 100.0% | 0 | 13,676 / 25,604 ms |
| 100r5u | 100 × 5 | 500 | 1 s | 500.2 | 316.5 | 12.7% | 0 | 46,351 / 55,899 ms |
| 500r2u | 500 × 2 | 1,000 | 20 s | 50.2 | 502.3 | 100.0% | 0 | 1,058 / 2,918 ms |
| 500r2u | 500 × 2 | 1,000 | 4 s | 249.4 | 306.4 | 61.4% | 0 | 21,708 / 39,287 ms |
| 500r2u | 500 × 2 | 1,000 | 3 s | 333.4 | 120.0 | 18.0% | 0 | 38,467 / 53,720 ms |
| 500r2u | 500 × 2 | 1,000 | 2 s | 500.1 | 131.2 | 13.1% | 0 | 45,451 / 54,977 ms |
| 400ru10 | 400 × 10 | 4,000 | 4 s | 19.3 | 139.1 | 72.1% | 3,924 | 17,762 / 35,058 ms |
| 400ru10 | 400 × 10 | 4,000 | 3 s | 78.4 | 280.9 | 35.8% | 3,797 | 11,259 / 34,919 ms |
| 400ru10 | 400 × 10 | 4,000 | 2 s | 79.0 | 260.7 | 33.0% | 3,842 | 21,525 / 42,400 ms |

## Conclusions

- At 500 sockets, only the 10-second run is both complete and close to the latency target. At four seconds, delivery remains complete but p95 latency grows to 25.6 seconds; at one second, only 12.7% of expected deliveries arrive.
- At 1,000 sockets, the low offered-rate 20-second run is complete. The four-second and faster runs are incomplete with tens-of-seconds tail latency.
- The 4,000-socket runs are not usable capacity measurements: 95–98% of sockets reported errors, and the actual accepted send rate is far below the nominal interval-derived rate. The artifacts establish failure under this load, but cannot isolate its cause without service metrics and a repeatable sweep.
