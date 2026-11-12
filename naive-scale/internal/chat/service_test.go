package chat

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestHandleIncoming_ValidationRejects(t *testing.T) {
	svc := NewService(nil)

	cases := []struct {
		name string
		in   Incoming
	}{
		{"empty body", Incoming{UserID: "u1", Body: ""}},
		{"whitespace-only body", Incoming{UserID: "u1", Body: "   \t\n"}},
		{"body too long", Incoming{UserID: "u1", Body: strings.Repeat("a", maxBodyLen+1)}},
		{"missing user_id", Incoming{UserID: "", Body: "hello"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := svc.HandleIncoming(context.Background(), "room1", tc.in)
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("HandleIncoming err = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestHandleIncoming_BodyAtMaxLenPassesValidation(t *testing.T) {
	svc := NewService(nil)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected nil-store panic after validation passed, got none")
		}
	}()

	in := Incoming{UserID: "u1", Body: strings.Repeat("a", maxBodyLen)}
	_ = svc.HandleIncoming(context.Background(), "room1", in)
}
