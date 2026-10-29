CREATE TABLE room_counters (
    room_id TEXT PRIMARY KEY CHECK (length(btrim(room_id)) > 0),
    last_seq BIGINT NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE messages (
    message_id UUID PRIMARY KEY,
    client_id TEXT NOT NULL CHECK (length(btrim(client_id)) > 0),
    client_message_id TEXT NOT NULL CHECK (length(btrim(client_message_id)) > 0),
    room_id TEXT NOT NULL REFERENCES room_counters(room_id),
    room_seq BIGINT NOT NULL CHECK (room_seq > 0),
    created_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    CONSTRAINT messages_room_sequence_unique UNIQUE (room_id, room_seq),
    CONSTRAINT messages_client_idempotency_unique UNIQUE (client_id, client_message_id)
);

CREATE TABLE message_outbox (
    message_id UUID PRIMARY KEY REFERENCES messages(message_id),
    room_id TEXT NOT NULL,
    room_seq BIGINT NOT NULL,
    event JSONB NOT NULL CHECK (
        jsonb_typeof(event) = 'object' AND COALESCE(event->>'version', '') = '1'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    CONSTRAINT message_outbox_room_sequence_unique UNIQUE (room_id, room_seq),
    CONSTRAINT message_outbox_message_sequence_fk
        FOREIGN KEY (room_id, room_seq)
        REFERENCES messages(room_id, room_seq)
);

CREATE INDEX message_outbox_pending_room_sequence_idx
    ON message_outbox (room_id, room_seq)
    WHERE published_at IS NULL;
