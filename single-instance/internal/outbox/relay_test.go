package outbox

import (
	"context"
	"errors"
	"testing"

	"chat-s/internal/models"
	"chat-s/internal/storage"
)

// fakeStore implements outboxStore in memory. Each DispatchBatch call serves the
// next pre-seeded batch (one slice per drain iteration) so we can simulate
// multi-batch drains, invoking the relay's dispatch callback per event and
// recording the ids it stamped. The real store does the claim + broadcast + mark
// in one transaction; the fake collapses that into a single call.
type fakeStore struct {
	batches    [][]storage.OutboxEvent
	calls      int
	dispatched []int64
	err        error // if set, DispatchBatch fails immediately (claim/mark error)
}

func (f *fakeStore) DispatchBatch(_ context.Context, _ int, dispatch func(storage.OutboxEvent) error) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	if f.calls >= len(f.batches) {
		f.calls++
		return 0, nil
	}
	b := f.batches[f.calls]
	f.calls++
	for _, e := range b {
		if err := dispatch(e); err != nil {
			return 0, err
		}
		f.dispatched = append(f.dispatched, e.ID)
	}
	return len(b), nil
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

func TestDrain_BroadcastsAndMarksDispatched(t *testing.T) {
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
	store := &fakeStore{} // no batches → DispatchBatch returns 0
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

// A full batch must trigger another dispatch; a short batch ends the drain.
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
	// Two non-empty batches processed, then the loop stops (short batch < limit).
	if store.calls != 2 {
		t.Errorf("DispatchBatch calls = %d, want 2", store.calls)
	}
}

// A claim/mark failure inside DispatchBatch must surface so the tick retries the
// (rolled-back, still-undispatched) rows rather than silently losing them.
func TestDrain_DispatchBatchErrorPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	store := &fakeStore{err: sentinel}
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
