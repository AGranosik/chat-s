package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"chat-s/internal/models"
	"chat-s/internal/storage"
)

// fakeStore implements outboxStore in memory. FetchUndispatched returns a
// pre-seeded batch per call (one slice per drain iteration) so we can simulate
// multi-batch drains; MarkDispatched records the ids it was asked to stamp.
type fakeStore struct {
	batches     [][]storage.OutboxEvent
	fetchCalls  int
	dispatched  []int64
	fetchErr    error
	dispatchErr error
}

func (f *fakeStore) Pool() *pgxpool.Pool { return nil } // never called by drain

func (f *fakeStore) FetchUndispatched(_ context.Context, _ int) ([]storage.OutboxEvent, error) {
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	if f.fetchCalls >= len(f.batches) {
		f.fetchCalls++
		return nil, nil
	}
	b := f.batches[f.fetchCalls]
	f.fetchCalls++
	return b, nil
}

func (f *fakeStore) MarkDispatched(_ context.Context, ids []int64) error {
	if f.dispatchErr != nil {
		return f.dispatchErr
	}
	f.dispatched = append(f.dispatched, ids...)
	return nil
}

// recordingBroadcaster captures the messages handed to it, in order.
type recordingBroadcaster struct {
	got []models.Message
}

func (r *recordingBroadcaster) Broadcast(_ string, msg models.Message) {
	r.got = append(r.got, msg)
}

func event(id int64, body string) storage.OutboxEvent {
	return storage.OutboxEvent{
		ID:      id,
		RoomID:  "room1",
		Message: models.Message{ID: id, RoomID: "room1", Body: body},
	}
}

func TestDrain_BroadcastsThenMarksDispatched(t *testing.T) {
	store := &fakeStore{batches: [][]storage.OutboxEvent{
		{event(1, "a"), event(2, "b")},
	}}
	bc := &recordingBroadcaster{}
	r := &Relay{store: store, broadcaster: bc}

	if err := r.drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}

	if len(bc.got) != 2 || bc.got[0].Body != "a" || bc.got[1].Body != "b" {
		t.Errorf("broadcast = %+v, want messages a,b in order", bc.got)
	}
	if want := []int64{1, 2}; !equalIDs(store.dispatched, want) {
		t.Errorf("dispatched = %v, want %v", store.dispatched, want)
	}
}

func TestDrain_EmptyOutboxIsNoOp(t *testing.T) {
	store := &fakeStore{} // no batches → FetchUndispatched returns nil
	bc := &recordingBroadcaster{}
	r := &Relay{store: store, broadcaster: bc}

	if err := r.drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(bc.got) != 0 {
		t.Errorf("broadcast = %+v, want none", bc.got)
	}
	if len(store.dispatched) != 0 {
		t.Errorf("dispatched = %v, want none", store.dispatched)
	}
}

// A full batch must trigger another fetch; a short batch ends the drain.
func TestDrain_LoopsUntilBatchUnderLimit(t *testing.T) {
	full := make([]storage.OutboxEvent, batchSize)
	for i := range full {
		full[i] = event(int64(i+1), "x")
	}
	short := []storage.OutboxEvent{event(int64(batchSize+1), "last")}

	store := &fakeStore{batches: [][]storage.OutboxEvent{full, short}}
	bc := &recordingBroadcaster{}
	r := &Relay{store: store, broadcaster: bc}

	if err := r.drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}

	if got, want := len(bc.got), batchSize+1; got != want {
		t.Errorf("broadcast count = %d, want %d", got, want)
	}
	// Two non-empty batches fetched, then the loop stops (short batch < limit).
	if store.fetchCalls != 2 {
		t.Errorf("fetchCalls = %d, want 2", store.fetchCalls)
	}
}

func TestDrain_FetchErrorPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	store := &fakeStore{fetchErr: sentinel}
	r := &Relay{store: store, broadcaster: &recordingBroadcaster{}}

	if err := r.drain(context.Background()); !errors.Is(err, sentinel) {
		t.Errorf("drain err = %v, want %v", err, sentinel)
	}
}

// MarkDispatched failing must surface — losing the stamp means re-broadcast.
func TestDrain_MarkDispatchedErrorPropagates(t *testing.T) {
	sentinel := errors.New("update failed")
	store := &fakeStore{
		batches:     [][]storage.OutboxEvent{{event(1, "a")}},
		dispatchErr: sentinel,
	}
	r := &Relay{store: store, broadcaster: &recordingBroadcaster{}}

	if err := r.drain(context.Background()); !errors.Is(err, sentinel) {
		t.Errorf("drain err = %v, want %v", err, sentinel)
	}
}

func equalIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
