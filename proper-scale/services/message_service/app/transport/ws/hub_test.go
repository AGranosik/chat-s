package ws

import (
	"context"
	"messages/chat"
	"testing"
)

type fakeMessageHandler struct {
	calls []chat.Message
	err   error
}

func (f *fakeMessageHandler) HandleIncoming(ctx context.Context, m chat.Message) error {
	f.calls = append(f.calls, m)
	return f.err
}

func TestCreation_Service_CannotBeNull(t *testing.T) {
	_, err := NewHub(nil)
	if err == nil {
		t.Errorf("Creation should accept nil.")
	}
}

func TestCreation_Success(t *testing.T) {
	_, err := setupHub(t)
	if err != nil {
		t.Error("Creation failure")
	}
}

func TestRegister_NewClient(t *testing.T) {
	hub, _ := setupHub(t)
	clientId := "test"

	err := hub.Register(clientId)
	if err != nil {
		t.Errorf("registration failure")
	}
}

func TestRegister_ClientALreadyRegistered_NoError(t *testing.T) {
	hub, _ := setupHub(t)
	clientId := "test"

	err := hub.Register(clientId)
	if err != nil {
		t.Errorf("registration failure")
	}

	err = hub.Register(clientId)
	if err != nil {
		t.Errorf("registration failure")
	}
}

func setupHub(t *testing.T) (*Hub, *fakeMessageHandler) {
	t.Helper() // marks this as a helper so failures report the caller's line, not this one
	handler := &fakeMessageHandler{}
	hub, err := NewHub(handler)
	if err != nil {
		t.Fatalf("unexpected error creating hub: %v", err)
	}
	return hub, handler
}
