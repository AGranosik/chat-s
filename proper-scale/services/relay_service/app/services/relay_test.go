package services

import (
	"context"
	"errors"
	"relayservice/app/storage"
	"testing"
	"time"
)

type fakeOutboxStore struct {
	NumberOfDispatchCalled int
	Err                    error
}

func createFakeStore() *fakeOutboxStore {
	return &fakeOutboxStore{
		NumberOfDispatchCalled: 0,
	}
}

func (f *fakeOutboxStore) DispatchBatch(ctx context.Context, batchSize int, dispatch func(context.Context, []storage.OutboxEvent) error) (int, error) {
	f.NumberOfDispatchCalled = f.NumberOfDispatchCalled + 1
	if f.Err != nil {
		return 0, f.Err
	}
	return 0, nil
}

func TestCreationWorks(t *testing.T) {
	relay := NewRelay(createFakeStore())

	if relay == nil {
		t.Fatal()
	}
}

func TestDispatchCalledAtLeastOnce(t *testing.T) {
	fake := createFakeStore()
	relay := NewRelay(fake)
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()
	go relay.Run(ctx)
	<-ctx.Done()

	if fake.NumberOfDispatchCalled == 0 {
		t.Fatalf("Dispatch never called.")
	}

}

func TestRunPanicsOnDrainError(t *testing.T) {
	fake := createFakeStore()
	fake.Err = errors.New("boom")
	relay := NewRelay(fake)

	defer func() {
		if recover() == nil {
			t.Fatal("expected Run to panic on drain error")
		}
	}()

	relay.Run(t.Context())
	t.Fatal("Run should have panicked before returning")
}
