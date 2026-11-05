# Load-test analysis — single instance

**Architecture.** One Go WebSocket process keeps the live-room hub in memory. PostgreSQL stores messages and the transactional outbox; the in-process relay polls that outbox and broadcasts to local sockets. nginx is the only test entry point.

Each k6 virtual user holds one socket and sends at the configured interval during a 30-second send window. `Delivered/s` is received frames divided by that window; completeness is received frames ÷ (sent messages × users per room). Latency is send-to-receive and includes the outbox relay poll. Results come from `loadtest/results/*/tput/`; they are measurements from one machine, not capacity guarantees.

| Scenario | Rooms × users | Sockets | Interval | Offered/s | Delivered/s | Complete | WS errors | E2E p50 / p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100r5u | 100 × 5 | 500 | 10 s | 50.0 | 250.2 | 100.0% | 0 | 979 / 1,862 ms |
| 100r5u | 100 × 5 | 500 | 4 s | 124.9 | 624.3 | 100.0% | 0 | 940 / 1,806 ms |
| 100r5u | 100 × 5 | 500 | 1 s | 500.1 | 2,500.7 | 100.0% | 0 | 1,042 / 1,887 ms |
| 500r2u | 500 × 2 | 1,000 | 10 s | 100.1 | 200.2 | 100.0% | 0 | 1,019 / 1,901 ms |
| 500r2u | 500 × 2 | 1,000 | 4 s | 248.9 | 497.9 | 100.0% | 0 | 887 / 1,775 ms |
| 500r2u | 500 × 2 | 1,000 | 1 s | 1,000.3 | 2,000.5 | 100.0% | 0 | 14,634 / 20,765 ms |
| 400r10u | 400 × 10 | 4,000 | 4 s | 999.9 | 4,724.8 | 47.3% | 0 | 41,982 / 45,522 ms |
| 400r10u | 400 × 10 | 4,000 | 3 s | 1,319.3 | 2,070.5 | 15.7% | 1,313 | 51,266 / 53,109 ms |
| 400r10u | 400 × 10 | 4,000 | 2 s | 1,939.1 | 245.7 | 1.3% | 1,305 | 30,489 / 56,140 ms |

## Conclusions

- At 500 and 1,000 sockets, every recorded run delivers all expected frames. At 1,000 sockets and a one-second interval, p95 latency rises to 20.8 seconds: queueing, not loss.
- The 4,000-socket / 10-member-room sweep is the stored stress boundary. Delivery falls below half at four seconds and p95 latency is about 45 seconds; faster intervals also produce thousands of WebSocket errors.
- Delivered frame rate alone is not a safe capacity criterion: 2,000 delivered frames/s at 1,000 sockets was complete but already had unacceptable tail latency.
