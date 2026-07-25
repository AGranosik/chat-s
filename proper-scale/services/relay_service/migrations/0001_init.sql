-- +goose Up
create table outbox (
    id            bigserial primary key,
    room_id       uuid not null,
    payload       jsonb not null,
    created_at    timestamptz not null default now(),
    dispatched_at timestamptz
);
create index outbox_undispatched_idx on outbox (dispatched_at, id);

-- +goose Down
drop table outbox;
