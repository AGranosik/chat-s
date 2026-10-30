package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
	contractsv1 "github.com/AGranosik/chat/contracts/protos"
	"google.golang.org/grpc"
)

func TestHandleForwardsVersionedEventUnchanged(t *testing.T) {
	event := testEvent()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	presence := &testPresenceClient{response: &contractsv1.UserConnectionResponse{InstanceNames: []string{"instance-1"}}}
	publisher := &testMessagePublisher{}
	service, err := NewMessageHandler(publisher, presence)
	if err != nil {
		t.Fatal(err)
	}
	message := Message{Key: []byte(event.RoomID), Value: data}

	if err := service.Handle(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if presence.roomID != event.RoomID {
		t.Fatalf("presence room = %q, want %q", presence.roomID, event.RoomID)
	}
	if publisher.calls != 1 {
		t.Fatalf("publisher calls = %d, want 1", publisher.calls)
	}
	if !bytes.Equal(publisher.message.Value, data) {
		t.Fatalf("published event = %s, want original %s", publisher.message.Value, data)
	}
	if publisher.message.Key == nil || string(publisher.message.Key) != event.RoomID {
		t.Fatalf("published Kafka key = %q, want %q", publisher.message.Key, event.RoomID)
	}
	if len(publisher.instances) != 1 || publisher.instances[0] != "instance-1" {
		t.Fatalf("target instances = %v, want [instance-1]", publisher.instances)
	}
}

func TestHandleTreatsNoConnectedInstancesAsLiveMiss(t *testing.T) {
	event := testEvent()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	presence := &testPresenceClient{response: &contractsv1.UserConnectionResponse{}}
	publisher := &testMessagePublisher{}
	service, err := NewMessageHandler(publisher, presence)
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Handle(context.Background(), Message{Key: []byte(event.RoomID), Value: data}); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 0 {
		t.Fatalf("publisher calls = %d, want 0", publisher.calls)
	}
}

func TestHandleReturnsPresenceError(t *testing.T) {
	event := testEvent()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("presence unavailable")
	presence := &testPresenceClient{err: wantErr}
	service, err := NewMessageHandler(&testMessagePublisher{}, presence)
	if err != nil {
		t.Fatal(err)
	}

	err = service.Handle(context.Background(), Message{Key: []byte(event.RoomID), Value: data})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Handle() error = %v, want wrapped presence error", err)
	}
}

func TestHandleReturnsPublisherError(t *testing.T) {
	event := testEvent()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("Redis unavailable")
	presence := &testPresenceClient{response: &contractsv1.UserConnectionResponse{InstanceNames: []string{"instance-1"}}}
	service, err := NewMessageHandler(&testMessagePublisher{err: wantErr}, presence)
	if err != nil {
		t.Fatal(err)
	}

	err = service.Handle(context.Background(), Message{Key: []byte(event.RoomID), Value: data})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Handle() error = %v, want wrapped publisher error", err)
	}
}

func TestHandleRejectsInvalidEventBeforePresenceLookup(t *testing.T) {
	event := testEvent()
	event.Version++
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	presence := &testPresenceClient{response: &contractsv1.UserConnectionResponse{}}
	service, err := NewMessageHandler(&testMessagePublisher{}, presence)
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Handle(context.Background(), Message{Key: []byte(event.RoomID), Value: data}); err == nil {
		t.Fatal("Handle() error = nil, want invalid event error")
	}
	if presence.calls != 0 {
		t.Fatalf("presence calls = %d, want 0", presence.calls)
	}
}

type testPresenceClient struct {
	contractsv1.UserServiceClient
	response *contractsv1.UserConnectionResponse
	err      error
	calls    int
	roomID   string
}

func (c *testPresenceClient) IsConnected(_ context.Context, request *contractsv1.ClientsConnectionDestRequest, _ ...grpc.CallOption) (*contractsv1.UserConnectionResponse, error) {
	c.calls++
	c.roomID = request.GetRoomId()
	return c.response, c.err
}

type testMessagePublisher struct {
	message   Message
	instances []string
	calls     int
	err       error
}

func (p *testMessagePublisher) PublishMessage(_ context.Context, message Message, instances []string) error {
	p.calls++
	p.message = message
	p.instances = append([]string(nil), instances...)
	return p.err
}

func testEvent() messagev1.Event {
	return messagev1.Event{
		Version:         messagev1.Version,
		MessageID:       "09c2df7e-c465-45e5-8b7f-dae2f7eb64ce",
		ClientMessageID: "client-message-1",
		RoomID:          "room-1",
		RoomSeq:         7,
		CreatedAt:       time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC),
		Payload:         json.RawMessage(`{"text":"hello"}`),
	}
}
