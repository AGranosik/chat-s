ALTER TABLE message_outbox ADD COLUMN shard SMALLINT;

ALTER TABLE message_outbox ALTER COLUMN shard SET NOT NULL;

ALTER TABLE message_outbox ADD CONSTRAINT message_outbox_shard_range CHECK (shard BETWEEN 0 AND 255);

DROP INDEX message_outbox_pending_idx;

CREATE INDEX message_outbox_pending_shard_idx
    ON message_outbox (shard, outbox_id)
    WHERE published_at IS NULL;
