---
name: run-tests
description: Run the full v1 Go test suite for chat-s — a build + vet sanity pass, the fast unit tests (go test ./...), and the Docker-backed integration tests (go test -tags=integration ./internal/integration/...). Use before committing, or whenever asked to run all tests for v1. Invoked by the code-review gate.
---

# run-tests — full v1 test suite

Runs everything that gates a v1 commit: a compile + vet sanity pass, the fast
unit tests, and the Docker-backed integration tests. The two test tiers are
split by a build tag on purpose (see `CLAUDE.md`), so the default `go test ./...`
stays fast and DB-free — this skill runs **both** tiers.

All commands run from the **`v1/` directory** (Go module `chat-s`).

## Steps

1. **Compile + static check (fail fast).** If either errors, stop and report —
   don't run tests against code that doesn't build.
   ```bash
   go build ./...
   go vet ./...
   ```

2. **Unit tests** — fast, no Docker:
   ```bash
   go test ./...
   ```

3. **Integration tests** — need Docker Desktop running; they spin up a
   `postgres:16` container via testcontainers (one per package) and exercise the
   real SQL, the transactional outbox, the polling relay, and a full ws
   round-trip:
   ```bash
   go test -tags=integration ./internal/integration/...
   ```
   Per `CLAUDE.md`, a container that won't start is a **real failure, not a
   skip**. If Docker is genuinely unavailable, report it clearly and treat this
   tier as **NOT PASSED** so the commit gate stays red.

4. **Report** pass/fail per tier. On failure, include the failing test output.

## Scope
Covers the **`v1`** module only. `log-prop/consumer` and `log-prop/publisher` are
separate Go modules (own `go.mod`); if you touched them, run their tests from
their own directories.
