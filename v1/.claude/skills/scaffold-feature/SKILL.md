---
name: scaffold-feature
description: Scaffold a new chat feature (message type, REST endpoint, or ws action) across all layers of v1 — model, storage, service, transport, migration — following the project's layering and conventions. Use when adding any feature that touches more than one layer of the chat server.
---

# scaffold-feature

Adding a feature to this chat server almost always crosses the same layers in the
same order. This skill encodes that path so new code matches `docs/ARCHITECTURE.md`
and `CLAUDE.md` instead of drifting.

## When to use
A request like "add reactions", "add a typing event", "store edited_at on
messages", "add a `GET /api/rooms/{id}/members` endpoint" — anything spanning
model + storage + transport.

## Steps

1. **Read context first.** Open `docs/ARCHITECTURE.md` (data model + layout) and
   `docs/PLAN.md` (is this part of an existing phase or net-new?). Confirm the
   feature is in scope — multi-node fan-out is **not**.

2. **Classify the change** and touch only the layers it needs:
   - **Data shape changes** (new column/table) → new `migrations/NNNN_*.sql`
     (goose, next sequential number) + update the relevant `internal/models/`
     struct.
   - **New persisted query** → method on the matching `internal/storage/*.go`
     file. Use `pgxpool`; parameterize; for lists use keyset pagination.
   - **Business logic** → method on `internal/chat/service.go`. The canonical
     order is **validate → persist → broadcast**. Broadcast only via the
     `Broadcaster` interface, never the concrete hub.
   - **Real-time action** (new ws message kind) → add a case in the read-pump
     router (`internal/transport/ws.go` → `chat.Service`); define the envelope
     `type` tag in `internal/models/`.
   - **REST endpoint** → handler in `internal/transport/http.go`, registered on
     the router. JSON in/out.

3. **Honor the conventions** (from CLAUDE.md):
   - env via `config.GetEnv(key, fallback)`; lowercase `log` messages.
   - hub sends **block**, never drop.
   - keep the `Broadcaster` seam intact — no direct hub calls from `service`.

4. **Wire it** in `cmd/server/main.go` if a new dependency needs constructing.

5. **Verify.** Provide/run a check: a `curl` for REST, or two `websocat`
   clients for a ws action; confirm Postgres persisted the row. Suggest `/verify`
   for a live run and `/code-review` before committing.

6. **Update the docs.** If the data model or flow changed, edit
   `docs/ARCHITECTURE.md`; if it completes or adds a plan item, tick/append it in
   `docs/PLAN.md`. Same commit as the code.

## Output
State which layers you touched and why, list new/modified files grouped by layer,
and end with the exact verify command to run.
