-- +goose Up
create table outbox (
    id            bigserial primary key,
    room_id       uuid not null,
    -- Opaque to the relay: it never reads inside a payload, it forwards the
    -- bytes to Redis. bytea keeps them byte-identical end to end (jsonb would
    -- reorder keys and strip whitespace) and leaves the encoding free to become
    -- protobuf/msgpack later. Structure worth querying lives in `messages`.
    payload       bytea not null,
    created_at    timestamptz not null default now(),
    dispatched_at timestamptz
);
create index outbox_undispatched_idx on outbox (id) where dispatched_at is null;

-- +goose Down
drop table outbox;
