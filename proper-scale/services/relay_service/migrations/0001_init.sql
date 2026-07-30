-- +goose Up
create table outbox (
    id            bigserial primary key,
    room_id       uuid not null,
    payload       bytea not null,
    created_at    timestamptz not null default now(),
    dispatched_at timestamptz
);
create index outbox_undispatched_idx on outbox (id) where dispatched_at is null;

-- +goose Down
drop table outbox;
