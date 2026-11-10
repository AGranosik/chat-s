package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"chat-s/internal/models"
	"chat-s/internal/storage"
)

type fakeStore struct {
	batches    [][]storage.OutboxEvent
	calls      int
	dispatched []int64
	err        error
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
	store := &fakeStore{}
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
	if store.calls != 2 {
		t.Errorf("DispatchBatch calls = %d, want 2", store.calls)
	}
}

func TestDrain_DispatchBatchErrorPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	store := &fakeStore{err: sentinel}
	r := &Relay{store: store, broadcaster: &recordingBroadcaster{}}

	if err := r.drain(context.Background()); !errors.Is(err, sentinel) {
		t.Errorf("drain err = %v, want %v", err, sentinel)
	}
}

func TestRun_FullBatchPollsAgainWithoutWaiting(t *testing.T) {
	gapAfterFirstPoll(t, batchSize, nil, time.Hour)
}

func TestRun_ShortBatchWaitsPollInterval(t *testing.T) {
	const interval = 50 * time.Millisecond

	gap := gapAfterFirstPoll(t, batchSize-1, nil, interval)

	if gap < interval {
		t.Errorf("gap between polls = %v, want at least %v", gap, interval)
	}
}

func TestRun_DispatchErrorWaitsPollInterval(t *testing.T) {
	const interval = 50 * time.Millisecond

	gap := gapAfterFirstPoll(t, 0, errors.New("boom"), interval)

	if gap < interval {
		t.Errorf("gap between polls = %v, want at least %v", gap, interval)
	}
}

type scriptedStore struct {
	next  func(call int) (int, error)
	calls int
}

func (s *scriptedStore) DispatchBatch(_ context.Context, _ int, _ func(storage.OutboxEvent) error) (int, error) {
	s.calls++
	return s.next(s.calls)
}

func gapAfterFirstPoll(t *testing.T, firstBatch int, firstErr error, interval time.Duration) time.Duration {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var firstPolled time.Time
	var gap time.Duration
	store := &scriptedStore{next: func(call int) (int, error) {
		if call == 1 {
			firstPolled = time.Now()
			return firstBatch, firstErr
		}
		gap = time.Since(firstPolled)
		cancel()
		return 0, nil
	}}
	r := &Relay{store: store, broadcaster: &recordingBroadcaster{}, pollInterval: interval}

	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not poll a second time within 5s")
	}
	return gap
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
