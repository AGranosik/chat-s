package ws

import (
	"context"
	"fmt"
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
	_, err := setupHub(nil, t)
	if err != nil {
		t.Error("Creation failure")
	}
}

func TestRegister_NewClient(t *testing.T) {
	hub, _ := setupHub(nil, t)
	clientId := "test"

	err := hub.Register(clientId)
	if err != nil {
		t.Errorf("registration failure")
	}
}

func TestRegister_ClientAlreadyRegistered_NoError(t *testing.T) {
	hub, _ := setupHub(nil, t)
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

func TestUnregister_ClientNotRegisteredBefore_NoError(t *testing.T) {
	hub, _ := setupHub(nil, t)
	clientId := "test"
	notRegisteredClient := "test2"

	hub.Register(clientId)

	err := hub.Unregister(notRegisteredClient)
	if err != nil {
		t.Errorf("Unregistered should retun error")
	}
}

func TestUnregister_ClientUnregistered_Success(t *testing.T) {
	hub, _ := setupHub(nil, t)
	clientId := "test"

	hub.Register(clientId)

	err := hub.Unregister(clientId)
	if err != nil {
		t.Errorf("Unregistered should retun error")
	}
}

func TestHandleIncoming_NoError(t *testing.T) {
	hub, _ := setupHub(nil, t)
	ctx := context.Background()

	err := hub.HandleIncoming(chat.Message{}, ctx)
	if err != nil {
		t.Fatalf("Shouldn't return error if handler does not.")
	}
}

func TestHandleIncoming_ReturnErrorOnHandlerError(t *testing.T) {
	ctx := context.Background()
	hub, _ := setupHub(&fakeMessageHandler{
		err: fmt.Errorf("fake error"),
	}, t)

	err := hub.HandleIncoming(chat.Message{}, ctx)
	if err == nil {
		t.Errorf("Error should be returned from hub.")
	}
}

func TestHandleIncoming_MessagePassed(t *testing.T) {
	ctx := context.Background()
	msgHandler := &fakeMessageHandler{
		err: fmt.Errorf("fake error"),
	}
	hub, _ := setupHub(msgHandler, t)
	msg := chat.Message{
		RoomID: "some-room",
	}
	hub.HandleIncoming(msg, ctx)
	calls := msgHandler.calls
	if len(calls) == 0 {
		t.Errorf("No messages were passed.")
	}
	if calls[0].RoomID != msg.RoomID {
		t.Error("Message isn't passed")
	}

}

func setupHub(handler *fakeMessageHandler, t *testing.T) (*Hub, *fakeMessageHandler) {
	t.Helper()
	if handler == nil {
		handler = &fakeMessageHandler{}
	}
	hub, err := NewHub(handler)
	if err != nil {
		t.Fatalf("unexpected error creating hub: %v", err)
	}
	return hub, handler
}
