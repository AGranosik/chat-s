# Load-test analysis — single instance

**Architecture.** One Go WebSocket process keeps the live-room hub in memory. PostgreSQL stores messages and the transactional outbox; the in-process relay drains that outbox in 500-row batches and broadcasts to local sockets, then waits 1 s before the next poll (`internal/outbox/relay.go`). nginx is the only test entry point.

**How to read the numbers.** Each k6 virtual user holds one socket and sends at the configured interval during a 30-second send window; sockets stay open for a further 30-second drain. `Offered/s` is sent messages ÷ 30 s. `Delivered/s` is received frames ÷ 30 s. `Complete` is received ÷ (sent × users per room), so it includes the sender's own echo. Latency is send-to-receive and includes the outbox poll. Each VU connects once and never reconnects, so a socket that drops stops both sending and receiving for the rest of the step. These are measurements from one machine with the load generator on the same host, not capacity guarantees.

## 300 MB server, 1 s poll / 500-row relay

Source: `loadtest/results/300m/tput/` (`summary.md` regenerated from the step JSONs).

**Configuration under test** (from `docker-compose.yml`)

| Service | Memory | CPU |
| --- | ---: | ---: |
| Go server (the system under test) | 300 MB | 8 |
| PostgreSQL 16 | 600 MB | 2 |
| nginx | 512 MB | 1 |

**Workload.** 500 rooms × 15 users = 7,500 held sockets, 180 s ramp, 5 s settle, 30 s send window, 30 s drain. Only the send interval changes between steps. The server is restarted before every step so each starts from a fresh heap; each step creates its own rooms and users. Steps ran in the order shown.

| Send interval | Offered/s | Delivered/s | Complete | WS errors | E2E p50 / p95 / max | State |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 10 s | 750.2 | 11,253 | 100.0% | 1 | 707 / 1,061 / 1,472 ms | healthy, at the poll floor |
| 8 s | 939.8 | 14,097 | 100.0% | 0 | 750 / 1,090 / 1,549 ms | healthy, at the poll floor |
| 6 s | 1,250.3 | 18,755 | 100.0% | 0 | 945 / 1,496 / 2,413 ms | healthy, light queueing |
| 4 s | 1,876.1 | 28,141 | 100.0% | 4 | 1,216 / 3,097 / 3,820 ms | complete, queueing |
| 3 s | 2,501.1 | 37,517 | 100.0% | 2 | 22,926 / 26,979 / 27,832 ms | complete only thanks to the drain |
| 2 s | 3,714.3 | 21,093 | 37.9% | 1,930 | 44,634 / 45,336 / 45,623 ms | collapsed |
| 1 s | 7,321.7 | 3,837 | 3.5% | 2,979 | 26,441 / 54,744 / 57,151 ms | collapsed |

Handshake success was 100% and connect p95 was 2 ms in every step. The WS-error budget for teardown noise is 0.5% of sockets (37); every step down to 3 s stays inside it.

### Verdict

- **Completeness ceiling (`summarize.ps1` read-off): about 37,500 delivered frames/s** (2,501 sends/s) at the 3 s step: 100% complete, errors within the teardown budget.
- **That figure is not a sustainable rate.** At the 3 s step p50 is 22.9 s and the slowest message took 27.8 s, just inside the 30 s drain. Nothing was lost only because the senders stopped after 30 s and the backlog had time to clear. A longer send window or a shorter drain would turn this step incomplete.
- **Sustainable ceiling: about 28,100 delivered frames/s** (1,876 sends/s) at the 4 s step. Complete, with the slowest message at 3.8 s, so the backlog stays bounded.
- **Latency-clean operating point: about 18,800 delivered frames/s** (1,250 sends/s) at the 6 s step, p95 1.5 s. The poll floor is ~1.06 s p95 (10 s and 8 s steps). With a p95 target under 2 s this is the usable limit.
- **The knee is between 1,876 and 2,501 sends/s.** Past it the server degrades instead of plateauing.

### How it broke

1. **Not a connection limit.** Handshakes stayed at 100% with 2 ms connect p95 through the collapse, and the ramp is slow (about 42 sockets/s), so this is not a thundering herd. All 7,500 sockets were admitted at every step.
2. **Queueing starts (6 s → 4 s).** Completeness stays at 100%, but p95 moves off the ~1.06 s floor to 1.5 s at 6 s and 3.1 s at 4 s. The relay no longer clears each batch of new events within one poll, so some events wait for a later one. Delivery is late, not lost.
3. **Backlog the server cannot clear while load continues (3 s).** All 1,125,495 frames arrived, but they were spread over up to 57.8 s (30 s of sending + 27.8 s max latency), an average of about 19,500 frames/s. The 4 s step averaged about 25,000 frames/s over its 33.8 s. Offering more load therefore delivered *less* per second. The latency distribution is also bunched near the maximum (p50 is 82% of max); a steady queue at a constant lower rate would put p50 near half of max. Delivery slowed or stalled during the window rather than running at a constant lower speed.
4. **Collapse (2 s and 1 s).** Delivery falls to 37.9% and 3.5%, and 1,930 / 2,979 sockets (26% / 40%) error out, far above the 37-error teardown budget, so these are real disconnects, not the k6 shutdown artifact. At 2 s, everything that did arrive was 44.6–45.6 s late: delivery ran a near-constant ~45 s behind, so only messages sent early in the window could arrive before sockets closed at 60 s. Clients still sent 99.0% and 97.6% of their target messages, so the ingest side kept accepting while the delivery side fell behind. The 1 s step's fastest delivery took 3.0 s.
5. **The 2 s collapse is not a process restart.** The median socket session there (154.0 s) is only about 1 s shorter than in the healthy steps (~155.1 s). A restart drops every socket and VUs never reconnect, so a restart before the last second of the drain would have shortened every session. At the 1 s step the median session is 16 s shorter (139.4 s), so a restart there cannot be excluded from the k6 data alone.

Reproduce:

```powershell
./loadtest/run-limits.ps1 -Scenario tput -Rooms 500 -Users 15 -Steps 10,8,6,4,3,2,1
./loadtest/summarize.ps1 -Dir loadtest/results/300m/tput
```

## Conclusions

- Latency sits at the poll floor (~0.7 s p50 / ~1.06 s p95) up to 940 sends/s, rises gently to 1.5 s p95 at 1,250 sends/s, and then jumps: 3.1 s at 1,876 sends/s and 27 s at 2,501 sends/s.
- **Delivered frame rate and completeness alone are not a safe capacity criterion.** The 3 s step is 100% complete at 37,500 delivered/s, yet its messages were up to 28 s late and the server averaged only ~19,500 frames/s while clearing them. Completeness depends on the drain window; bounded maximum latency is the better test.
- **Past the knee the server degrades instead of plateauing.** Offering more load reduces what is delivered, and at about 2× the sustainable ceiling a quarter or more of the sockets drop. Any real deployment needs admission control or back-pressure in front of this design.
- Fan-out dominates the cost: each accepted message becomes 15 socket writes, so the sustainable *send* rate (~1,900/s) is far lower than the delivered frame rate suggests.
- These are single-machine, single-run-per-point numbers with the generator sharing the host. They show where this design bends, not an absolute capacity.
