DROP INDEX message_outbox_pending_shard_idx;

CREATE INDEX message_outbox_pending_idx
    ON message_outbox (outbox_id)
    WHERE published_at IS NULL;

ALTER TABLE message_outbox DROP CONSTRAINT message_outbox_shard_range;

ALTER TABLE message_outbox DROP COLUMN shard;
