-- +goose Up
create table users (
    id         uuid primary key default gen_random_uuid(),
    username   text not null unique,
    created_at timestamptz not null default now()
);

create table rooms (
    id         uuid primary key default gen_random_uuid(),
    name       text not null unique,
    created_at timestamptz not null default now()
);

create table messages (
    id         bigserial primary key,
    room_id    uuid not null references rooms (id),
    user_id    uuid not null references users (id),
    body       text not null,
    created_at timestamptz not null default now()
);
create index messages_room_created_idx on messages (room_id, created_at desc, id desc);

-- Transactional outbox: a message insert and its outbox event commit together,
-- and the relay drains undispatched rows in id order. See docs/ARCHITECTURE.md.
create table outbox (
    id            bigserial primary key,
    room_id       uuid not null,
    payload       jsonb not null,
    created_at    timestamptz not null default now(),
    dispatched_at timestamptz
);
create index outbox_undispatched_idx on outbox (dispatched_at, id);

-- Seed a room and a user so the websocket round-trip is testable out of the box.
insert into rooms (id, name) values ('00000000-0000-0000-0000-000000000001', 'general');
insert into users (id, username) values ('00000000-0000-0000-0000-000000000001', 'demo');

-- +goose Down
drop table outbox;
drop table messages;
drop table rooms;
drop table users;
