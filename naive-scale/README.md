# naive-scale — approach #2 (scaffold)

The "just add more boxes" reflex: run **three** copies of the same Go process
behind one nginx load balancer, each on a **200m** memory budget (the
single-instance approach ran one process on 600m). Postgres and nginx keep the
single-instance configuration.

Right now this is an **empty service** — an HTTP server with a `/healthz`
endpoint and graceful shutdown. There is deliberately no shared broadcaster yet:
nginx round-robins connections across instances, so clients on different
instances can't see each other's messages. Closing that gap (a Redis/Kafka
`Broadcaster`) is what this approach will measure against the single-instance
baseline.

```
cmd/server/main.go     HTTP server + graceful shutdown
internal/config/       GetEnv(key, fallback)
nginx/nginx.conf       load-balances across server1..3
docker-compose.yml     postgres + 3x server (200m each) + nginx
```

## Run

```bash
go run ./cmd/server     # run one instance locally
docker compose up       # full stack: postgres + 3 servers + nginx on :80
go build ./...          # sanity build
```
