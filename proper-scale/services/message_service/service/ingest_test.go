package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
)

type fakeMessageStore struct {
	clientID  string
	request   messagev1.SendRequest
	accepted  messagev1.Event
	acceptErr error
}

func (f *fakeMessageStore) Accept(_ context.Context, clientID string, request messagev1.SendRequest) (messagev1.Event, error) {
	f.clientID = clientID
	f.request = request
	return f.accepted, f.acceptErr
}

func TestHandleIncomingValidatesAndPersists(t *testing.T) {
	store := &fakeMessageStore{
		accepted: messagev1.Event{
			Version:         messagev1.Version,
			MessageID:       "message-1",
			ClientMessageID: "client-key-1",
			RoomID:          "room-1",
			RoomSeq:         1,
			CreatedAt:       time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
			Payload:         json.RawMessage(`{"text":"hello"}`),
		},
	}
	handler, err := NewIngestHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	request := messagev1.SendRequest{
		Type:            messagev1.SendFrameType,
		Version:         messagev1.Version,
		ClientMessageID: "client-key-1",
		RoomID:          "room-1",
		Payload:         json.RawMessage(`{"text":"hello"}`),
	}

	got, err := handler.HandleIncoming(context.Background(), "client-1", request)
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageID != store.accepted.MessageID || store.clientID != "client-1" || store.request.ClientMessageID != request.ClientMessageID {
		t.Fatalf("accepted = %+v, stored client/request = %q/%+v", got, store.clientID, store.request)
	}
}

func TestHandleIncomingRejectsInvalidRequestBeforeStore(t *testing.T) {
	store := &fakeMessageStore{}
	handler, err := NewIngestHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler.HandleIncoming(context.Background(), "client-1", messagev1.SendRequest{}); err == nil {
		t.Fatal("HandleIncoming() error = nil, want validation error")
	}
	if store.clientID != "" {
		t.Fatalf("store was called with client id %q", store.clientID)
	}
}
