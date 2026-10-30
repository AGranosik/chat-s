# Message contract v1

The Go module at `github.com/AGranosik/chat/contracts/messages` is the shared contract for `message_service`, `outbox_relay`, and `delivery_service`. `Event` is the Kafka value and the live message envelope forwarded over WebSocket. The room ID is the Kafka key.

The event shape is defined by the Go `Event` type and its validation function. The payload is a JSON value nested directly under `payload`, not a string containing encoded JSON. The server assigns `message_id`, `room_seq`, and `created_at`; the sender supplies `client_message_id`.

The WebSocket send request uses this shape:

```json
{
  "type": "send",
  "version": 1,
  "client_message_id": "0192f4d8-2c38-7a31-b5e4-40e8ef1d3ea7",
  "room_id": "room-123",
  "payload": {"text": "hello"}
}
```

The accepted acknowledgement uses this shape:

```json
{
  "type": "accepted",
  "version": 1,
  "message_id": "0192f4d8-2c38-7a31-b5e4-40e8ef1d3ea6",
  "client_message_id": "0192f4d8-2c38-7a31-b5e4-40e8ef1d3ea7",
  "room_id": "room-123",
  "room_seq": 42,
  "created_at": "2026-10-28T12:00:00Z"
}
```

If the request is malformed or the service cannot accept it, the server sends a rejected acknowledgement over the same WebSocket connection. The client message identifier is included when the server could decode it, so the client can match the result to its send attempt.

```json
{
  "type": "rejected",
  "version": 1,
  "client_message_id": "0192f4d8-2c38-7a31-b5e4-40e8ef1d3ea7",
  "error_code": "not_accepted"
}
```

`invalid_request` means the request could not be decoded or failed validation. `not_accepted` means the request passed validation but the service did not return a persisted message. The client can treat `accepted` as success and `rejected` as failure.

The server scopes `client_message_id` uniqueness to the `client_id` supplied by the WebSocket connection. A client should reuse the same key only when retrying the same send.

## Fresh v1 bootstrap

The initial v1 deployment starts with an empty PostgreSQL database schema, an empty Kafka topic named `messages` with six partitions, and a new delivery consumer group named `delivery-v1` with no committed offsets. Local single-broker Kafka uses replication factor 1. Configure a new consumer group with `auto.offset.reset=earliest`; the group is created when the delivery consumer first joins. Apply the `message_service` migration before the message service or relay becomes ready. Do not reuse legacy message topic records or consumer-group offsets; this plan does not include a message backfill.

The database migration creates `room_counters`, `messages`, and `message_outbox`. The message and outbox rows are owned by `message_service` and are written in one transaction in T2.
