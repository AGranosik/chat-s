package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type rowQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Store struct {
	pool  *pgxpool.Pool
	now   func() time.Time
	newID func() (string, error)
}

func NewPool(ctx context.Context, connectionString string) (*pgxpool.Pool, error) {
	if strings.TrimSpace(connectionString) == "" {
		return nil, errors.New("database connection string is required")
	}
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errors.New("database pool is required")
	}
	return &Store{pool: pool, now: time.Now, newID: newMessageID}, nil
}

func (s *Store) Accept(ctx context.Context, clientID string, request messagev1.SendRequest) (messagev1.Event, error) {
	if strings.TrimSpace(clientID) == "" {
		return messagev1.Event{}, errors.New("client_id is required")
	}
	if err := request.Validate(); err != nil {
		return messagev1.Event{}, err
	}

	event, err := findEvent(ctx, s.pool, clientID, request.ClientMessageID)
	if err == nil {
		return event, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return messagev1.Event{}, fmt.Errorf("find accepted message: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return messagev1.Event{}, fmt.Errorf("begin message transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	event, err = findEvent(ctx, tx, clientID, request.ClientMessageID)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return messagev1.Event{}, fmt.Errorf("commit idempotent read: %w", err)
		}
		return event, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return messagev1.Event{}, fmt.Errorf("find accepted message in transaction: %w", err)
	}

	var roomSeq int64
	err = tx.QueryRow(ctx, `
		INSERT INTO room_counters (room_id, last_seq)
		VALUES ($1, 1)
		ON CONFLICT (room_id) DO UPDATE
		SET last_seq = room_counters.last_seq + 1, updated_at = NOW()
		RETURNING last_seq`, request.RoomID).Scan(&roomSeq)
	if err != nil {
		return messagev1.Event{}, fmt.Errorf("allocate room sequence: %w", err)
	}

	messageID, err := s.newID()
	if err != nil {
		return messagev1.Event{}, fmt.Errorf("generate message id: %w", err)
	}
	databaseMessageID, err := parseMessageID(messageID)
	if err != nil {
		return messagev1.Event{}, fmt.Errorf("parse message id: %w", err)
	}
	createdAt := s.now().UTC()
	event = messagev1.Event{
		Version:         messagev1.Version,
		MessageID:       messageID,
		ClientMessageID: request.ClientMessageID,
		RoomID:          request.RoomID,
		RoomSeq:         roomSeq,
		CreatedAt:       createdAt,
		Payload:         append(json.RawMessage(nil), request.Payload...),
	}
	if err := event.Validate(); err != nil {
		return messagev1.Event{}, fmt.Errorf("validate accepted event: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO messages (message_id, client_id, client_message_id, room_id, room_seq, created_at, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)`,
		databaseMessageID, clientID, event.ClientMessageID, event.RoomID, event.RoomSeq, event.CreatedAt, string(event.Payload))
	if err != nil {
		if isClientIdempotencyConflict(err) {
			_ = tx.Rollback(ctx)
			accepted, findErr := findEvent(ctx, s.pool, clientID, request.ClientMessageID)
			if findErr == nil {
				return accepted, nil
			}
		}
		return messagev1.Event{}, fmt.Errorf("insert message history: %w", err)
	}

	serialized, err := json.Marshal(event)
	if err != nil {
		return messagev1.Event{}, fmt.Errorf("marshal message event: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO message_outbox (message_id, room_id, room_seq, event, created_at)
		VALUES ($1, $2, $3, $4::jsonb, $5)`,
		databaseMessageID, event.RoomID, event.RoomSeq, string(serialized), event.CreatedAt)
	if err != nil {
		return messagev1.Event{}, fmt.Errorf("insert message outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return messagev1.Event{}, fmt.Errorf("commit message transaction: %w", err)
	}
	return event, nil
}

func findEvent(ctx context.Context, queryer rowQueryer, clientID, clientMessageID string) (messagev1.Event, error) {
	var event messagev1.Event
	var payload []byte
	err := queryer.QueryRow(ctx, `
		SELECT message_id::text, client_message_id, room_id, room_seq, created_at, payload
		FROM messages
		WHERE client_id = $1 AND client_message_id = $2`, clientID, clientMessageID).
		Scan(&event.MessageID, &event.ClientMessageID, &event.RoomID, &event.RoomSeq, &event.CreatedAt, &payload)
	if err != nil {
		return messagev1.Event{}, err
	}
	event.Version = messagev1.Version
	event.Payload = append(json.RawMessage(nil), payload...)
	return event, nil
}

func isClientIdempotencyConflict(err error) bool {
	var postgresErr *pgconn.PgError
	return errors.As(err, &postgresErr) && postgresErr.Code == "23505" && postgresErr.ConstraintName == "messages_client_idempotency_unique"
}

func newMessageID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(value[0:4]), hex.EncodeToString(value[4:6]), hex.EncodeToString(value[6:8]), hex.EncodeToString(value[8:10]), hex.EncodeToString(value[10:16])), nil
}

func parseMessageID(value string) (pgtype.UUID, error) {
	raw, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	if err != nil {
		return pgtype.UUID{}, err
	}
	if len(raw) != 16 {
		return pgtype.UUID{}, fmt.Errorf("message id must contain 16 bytes")
	}
	var id pgtype.UUID
	copy(id.Bytes[:], raw)
	id.Valid = true
	return id, nil
}
