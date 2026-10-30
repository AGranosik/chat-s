package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"main/app"

	"github.com/IBM/sarama"
)

func TestConsumeClaimPreservesKafkaEnvelopeAndMarksOnSuccess(t *testing.T) {
	event := []byte(`{"version":1,"message_id":"id-1","room_id":"room-1","room_seq":3}`)
	message := &sarama.ConsumerMessage{
		Topic:     "messages",
		Partition: 2,
		Offset:    8,
		Key:       []byte("room-1"),
		Value:     event,
		Headers: []*sarama.RecordHeader{
			{Key: []byte("trace"), Value: []byte("trace-1")},
		},
		Timestamp: time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC),
	}
	handler := &testMessageHandler{}
	session := &testSession{ctx: context.Background()}
	claim := newTestClaim(message)

	if err := NewHandler(handler).ConsumeClaim(session, claim); err != nil {
		t.Fatal(err)
	}
	if len(session.marked) != 1 || session.marked[0] != message {
		t.Fatalf("marked messages = %v, want current message", session.marked)
	}
	if string(handler.message.Key) != "room-1" || string(handler.message.Value) != string(event) {
		t.Fatalf("forwarded envelope key/value = %q/%s", handler.message.Key, handler.message.Value)
	}
	if handler.message.Headers["trace"] != "trace-1" {
		t.Fatalf("forwarded headers = %v, want trace header", handler.message.Headers)
	}
	if !handler.message.Timestamp.Equal(message.Timestamp) {
		t.Fatalf("forwarded timestamp = %s, want %s", handler.message.Timestamp, message.Timestamp)
	}
}

func TestConsumeClaimReturnsHandlerErrorWithoutMarkingMessage(t *testing.T) {
	message := &sarama.ConsumerMessage{Topic: "messages", Partition: 1, Offset: 9}
	wantErr := errors.New("Redis unavailable")
	handler := &testMessageHandler{err: wantErr}
	session := &testSession{ctx: context.Background()}

	err := NewHandler(handler).ConsumeClaim(session, newTestClaim(message))
	if !errors.Is(err, wantErr) {
		t.Fatalf("ConsumeClaim() error = %v, want wrapped handler error", err)
	}
	if len(session.marked) != 0 {
		t.Fatalf("marked messages = %v, want none", session.marked)
	}
}

type testMessageHandler struct {
	message app.Message
	err     error
}

func (h *testMessageHandler) Handle(_ context.Context, message app.Message) error {
	h.message = message
	return h.err
}

type testSession struct {
	ctx    context.Context
	marked []*sarama.ConsumerMessage
}

func (s *testSession) Claims() map[string][]int32               { return nil }
func (s *testSession) MemberID() string                         { return "test-member" }
func (s *testSession) GenerationID() int32                      { return 1 }
func (s *testSession) MarkOffset(string, int32, int64, string)  {}
func (s *testSession) Commit()                                  {}
func (s *testSession) ResetOffset(string, int32, int64, string) {}
func (s *testSession) Context() context.Context                 { return s.ctx }
func (s *testSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	s.marked = append(s.marked, message)
}

type testClaim struct {
	messages chan *sarama.ConsumerMessage
}

func newTestClaim(message *sarama.ConsumerMessage) *testClaim {
	messages := make(chan *sarama.ConsumerMessage, 1)
	messages <- message
	close(messages)
	return &testClaim{messages: messages}
}

func (c *testClaim) Topic() string                            { return "messages" }
func (c *testClaim) Partition() int32                         { return 0 }
func (c *testClaim) InitialOffset() int64                     { return 0 }
func (c *testClaim) HighWaterMarkOffset() int64               { return 1 }
func (c *testClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }
