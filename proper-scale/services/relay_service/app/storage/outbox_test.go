package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakePool struct {
	tx         *fakeTx
	beginErr   error
	beginCalls int
}

func (p *fakePool) Begin(ctx context.Context) (pgx.Tx, error) {
	p.beginCalls++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	return p.tx, nil
}

type execCall struct {
	sql  string
	args []any
}

type fakeTx struct {
	pgx.Tx

	rows      *fakeRows
	queryErr  error
	execErr   error
	commitErr error

	querySQL  string
	queryArgs []any
	execs     []execCall
	commits   int
	rollbacks int

	// Captured at call time: the deferred cancel has already fired by the time
	// a test inspects the tx, so the context itself tells us nothing later.
	rollbackCtxErr      error
	rollbackDeadline    time.Time
	rollbackHasDeadline bool
}

func (tx *fakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.querySQL = sql
	tx.queryArgs = args
	if tx.queryErr != nil {
		return nil, tx.queryErr
	}
	return tx.rows, nil
}

func (tx *fakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, execCall{sql: sql, args: args})
	if tx.execErr != nil {
		return pgconn.CommandTag{}, tx.execErr
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (tx *fakeTx) Commit(ctx context.Context) error {
	tx.commits++
	return tx.commitErr
}

func (tx *fakeTx) Rollback(ctx context.Context) error {
	tx.rollbacks++
	tx.rollbackCtxErr = ctx.Err()
	tx.rollbackDeadline, tx.rollbackHasDeadline = ctx.Deadline()
	if tx.commits > 0 {
		// Mirror pgx: rolling back a committed tx is a no-op error.
		return pgx.ErrTxClosed
	}
	return nil
}

type fakeRows struct {
	pgx.Rows

	events []OutboxEvent
	err    error // surfaced by Err(), like a mid-stream read failure

	pos    int
	closed bool
}

func (r *fakeRows) Next() bool {
	if r.err != nil || r.pos >= len(r.events) {
		return false
	}
	r.pos++
	return true
}

func (r *fakeRows) Err() error { return r.err }

func (r *fakeRows) Close() { r.closed = true }

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return []pgconn.FieldDescription{{Name: "id"}, {Name: "room_id"}, {Name: "payload"}}
}

// Scan mirrors what pgx hands a row scanner: one target per field description,
// in field-description order.
func (r *fakeRows) Scan(dest ...any) error {
	if len(dest) != 3 {
		return fmt.Errorf("fakeRows.Scan: got %d targets, want 3", len(dest))
	}

	event := r.events[r.pos-1]

	id, ok := dest[0].(*int64)
	if !ok {
		return fmt.Errorf("fakeRows.Scan: id target is %T, want *int64", dest[0])
	}
	roomID, ok := dest[1].(*string)
	if !ok {
		return fmt.Errorf("fakeRows.Scan: room_id target is %T, want *string", dest[1])
	}
	payload, ok := dest[2].(*[]byte)
	if !ok {
		return fmt.Errorf("fakeRows.Scan: payload target is %T, want *[]byte", dest[2])
	}

	*id, *roomID, *payload = event.ID, event.RoomID, event.Payload
	return nil
}

type recordingDispatcher struct {
	err    error
	calls  int
	events []OutboxEvent
}

func (d *recordingDispatcher) dispatch(ctx context.Context, events []OutboxEvent) error {
	d.calls++
	d.events = slices.Clone(events)
	return d.err
}

func squashSQL(sql string) string { return strings.Join(strings.Fields(sql), " ") }

func eventIDs(events []OutboxEvent) []int64 {
	ids := make([]int64, 0, len(events))
	for _, e := range events {
		ids = append(ids, e.ID)
	}
	return ids
}

func twoEvents() []OutboxEvent {
	return []OutboxEvent{
		{ID: 7, RoomID: "11111111-1111-1111-1111-111111111111", Payload: []byte(`{"seq":1}`)},
		{ID: 9, RoomID: "22222222-2222-2222-2222-222222222222", Payload: []byte(`{"seq":2}`)},
	}
}

func newFakeStore(events []OutboxEvent) (*Store, *fakePool, *fakeTx) {
	tx := &fakeTx{rows: &fakeRows{events: events}}
	pool := &fakePool{tx: tx}
	return &Store{pool: pool}, pool, tx
}

func TestDispatchBatchRejectsNonPositiveBatchSize(t *testing.T) {
	for _, batchSize := range []int{0, -1} {
		t.Run(fmt.Sprintf("batchSize=%d", batchSize), func(t *testing.T) {
			store, pool, _ := newFakeStore(twoEvents())
			var dispatcher recordingDispatcher

			n, err := store.DispatchBatch(context.Background(), batchSize, dispatcher.dispatch)

			if err == nil {
				t.Fatal("DispatchBatch error = nil, want an error")
			}
			if n != 0 {
				t.Errorf("DispatchBatch n = %d, want 0", n)
			}
			if pool.beginCalls != 0 {
				t.Errorf("began %d transactions, want 0 - the batch size is rejected before any DB work", pool.beginCalls)
			}
			if dispatcher.calls != 0 {
				t.Errorf("dispatch called %d times, want 0", dispatcher.calls)
			}
		})
	}
}

func TestDispatchBatchReturnsBeginError(t *testing.T) {
	wantErr := errors.New("pool exhausted")
	store, pool, _ := newFakeStore(twoEvents())
	pool.beginErr = wantErr
	var dispatcher recordingDispatcher

	n, err := store.DispatchBatch(context.Background(), 10, dispatcher.dispatch)

	if !errors.Is(err, wantErr) {
		t.Fatalf("DispatchBatch error = %v, want it to wrap %v", err, wantErr)
	}
	if n != 0 {
		t.Errorf("DispatchBatch n = %d, want 0", n)
	}
	if dispatcher.calls != 0 {
		t.Errorf("dispatch called %d times, want 0", dispatcher.calls)
	}
}

func TestDispatchBatchRollsBackWhenClaimQueryFails(t *testing.T) {
	wantErr := errors.New("connection reset")
	store, _, tx := newFakeStore(twoEvents())
	tx.queryErr = wantErr
	var dispatcher recordingDispatcher

	n, err := store.DispatchBatch(context.Background(), 10, dispatcher.dispatch)

	if !errors.Is(err, wantErr) {
		t.Fatalf("DispatchBatch error = %v, want it to wrap %v", err, wantErr)
	}
	if n != 0 {
		t.Errorf("DispatchBatch n = %d, want 0", n)
	}
	if dispatcher.calls != 0 {
		t.Errorf("dispatch called %d times, want 0", dispatcher.calls)
	}
	if tx.commits != 0 {
		t.Errorf("committed %d times, want 0", tx.commits)
	}
	if tx.rollbacks != 1 {
		t.Errorf("rolled back %d times, want 1", tx.rollbacks)
	}
}

func TestDispatchBatchRollsBackWhenRowsFail(t *testing.T) {
	wantErr := errors.New("bad row")
	store, _, tx := newFakeStore(nil)
	tx.rows.err = wantErr
	var dispatcher recordingDispatcher

	n, err := store.DispatchBatch(context.Background(), 10, dispatcher.dispatch)

	if !errors.Is(err, wantErr) {
		t.Fatalf("DispatchBatch error = %v, want it to wrap %v", err, wantErr)
	}
	if n != 0 {
		t.Errorf("DispatchBatch n = %d, want 0", n)
	}
	if dispatcher.calls != 0 {
		t.Errorf("dispatch called %d times, want 0", dispatcher.calls)
	}
	if tx.commits != 0 {
		t.Errorf("committed %d times, want 0", tx.commits)
	}
	if tx.rollbacks != 1 {
		t.Errorf("rolled back %d times, want 1", tx.rollbacks)
	}
}

func TestDispatchBatchWithNothingPending(t *testing.T) {
	store, _, tx := newFakeStore(nil)
	var dispatcher recordingDispatcher

	n, err := store.DispatchBatch(context.Background(), 10, dispatcher.dispatch)

	if err != nil {
		t.Fatalf("DispatchBatch error = %v, want nil", err)
	}
	if n != 0 {
		t.Errorf("DispatchBatch n = %d, want 0", n)
	}
	if dispatcher.calls != 0 {
		t.Errorf("dispatch called %d times, want 0 - there is nothing to send", dispatcher.calls)
	}
	if len(tx.execs) != 0 {
		t.Errorf("ran %d exec(s), want 0 - nothing to mark dispatched", len(tx.execs))
	}
	if tx.commits != 0 {
		t.Errorf("committed %d times, want 0 - an empty claim has nothing to commit", tx.commits)
	}
	if tx.rollbacks != 1 {
		t.Errorf("rolled back %d times, want 1", tx.rollbacks)
	}
	if !tx.rows.closed {
		t.Error("rows were not closed")
	}
}

func TestDispatchBatchDispatchesThenMarksAndCommits(t *testing.T) {
	events := twoEvents()
	store, _, tx := newFakeStore(events)
	var dispatcher recordingDispatcher

	n, err := store.DispatchBatch(context.Background(), 3, dispatcher.dispatch)

	if err != nil {
		t.Fatalf("DispatchBatch error = %v, want nil", err)
	}
	if n != len(events) {
		t.Errorf("DispatchBatch n = %d, want %d", n, len(events))
	}

	if dispatcher.calls != 1 {
		t.Fatalf("dispatch called %d times, want 1", dispatcher.calls)
	}
	if got, want := dispatcher.events, events; !slices.EqualFunc(got, want, sameEvent) {
		t.Errorf("dispatch got events %+v, want %+v", got, want)
	}

	if want := []any{3}; !slices.Equal(tx.queryArgs, want) {
		t.Errorf("claim query args = %v, want %v (the batch size)", tx.queryArgs, want)
	}
	if claim := squashSQL(tx.querySQL); !strings.Contains(claim, "for update skip locked") {
		t.Errorf("claim query lacks `for update skip locked`, so two relays would fan out the same rows:\n%s", claim)
	}

	if len(tx.execs) != 1 {
		t.Fatalf("ran %d exec(s), want 1", len(tx.execs))
	}
	markSQL := squashSQL(tx.execs[0].sql)
	if !strings.Contains(markSQL, "update outbox") || !strings.Contains(markSQL, "dispatched_at") {
		t.Errorf("exec is not the mark-dispatched update:\n%s", markSQL)
	}
	if len(tx.execs[0].args) != 1 {
		t.Fatalf("mark-dispatched args = %v, want a single id list", tx.execs[0].args)
	}
	gotIDs, ok := tx.execs[0].args[0].([]int64)
	if !ok {
		t.Fatalf("mark-dispatched arg is %T, want []int64", tx.execs[0].args[0])
	}
	if want := eventIDs(events); !slices.Equal(gotIDs, want) {
		t.Errorf("marked ids %v dispatched, want %v", gotIDs, want)
	}

	if tx.commits != 1 {
		t.Errorf("committed %d times, want 1", tx.commits)
	}
	if !tx.rows.closed {
		t.Error("rows were not closed")
	}
}

func TestDispatchBatchDoesNotMarkWhenDispatchFails(t *testing.T) {
	wantErr := errors.New("fanout unavailable")
	store, _, tx := newFakeStore(twoEvents())
	dispatcher := recordingDispatcher{err: wantErr}

	n, err := store.DispatchBatch(context.Background(), 10, dispatcher.dispatch)

	if !errors.Is(err, wantErr) {
		t.Fatalf("DispatchBatch error = %v, want it to wrap %v", err, wantErr)
	}
	if n != 0 {
		t.Errorf("DispatchBatch n = %d, want 0", n)
	}
	if len(tx.execs) != 0 {
		t.Errorf("ran %d exec(s), want 0 - a failed dispatch must not mark anything dispatched", len(tx.execs))
	}
	if tx.commits != 0 {
		t.Errorf("committed %d times, want 0", tx.commits)
	}
	if tx.rollbacks != 1 {
		t.Errorf("rolled back %d times, want 1 - the events stay claimable", tx.rollbacks)
	}
}

func TestDispatchBatchDoesNotCommitWhenMarkFails(t *testing.T) {
	wantErr := errors.New("deadlock detected")
	store, _, tx := newFakeStore(twoEvents())
	tx.execErr = wantErr
	var dispatcher recordingDispatcher

	n, err := store.DispatchBatch(context.Background(), 10, dispatcher.dispatch)

	if !errors.Is(err, wantErr) {
		t.Fatalf("DispatchBatch error = %v, want it to wrap %v", err, wantErr)
	}
	if n != 0 {
		t.Errorf("DispatchBatch n = %d, want 0", n)
	}
	if tx.commits != 0 {
		t.Errorf("committed %d times, want 0", tx.commits)
	}
	if tx.rollbacks != 1 {
		t.Errorf("rolled back %d times, want 1", tx.rollbacks)
	}
}

func TestDispatchBatchReturnsCommitError(t *testing.T) {
	wantErr := errors.New("server closed the connection")
	store, _, tx := newFakeStore(twoEvents())
	tx.commitErr = wantErr
	var dispatcher recordingDispatcher

	n, err := store.DispatchBatch(context.Background(), 10, dispatcher.dispatch)

	if !errors.Is(err, wantErr) {
		t.Fatalf("DispatchBatch error = %v, want it to wrap %v", err, wantErr)
	}
	if n != 0 {
		t.Errorf("DispatchBatch n = %d, want 0", n)
	}
	if dispatcher.calls != 1 {
		t.Errorf("dispatch called %d times, want 1 - the events went out even though the commit failed", dispatcher.calls)
	}
}

// The rollback runs on a context detached from the caller's, so a shutdown
// that cancels ctx mid-dispatch still releases the row locks instead of
// leaving them held until the connection dies.
func TestDispatchBatchRollsBackAfterCallerContextIsCanceled(t *testing.T) {
	store, _, tx := newFakeStore(twoEvents())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dispatchErr := errors.New("shutting down")
	_, err := store.DispatchBatch(ctx, 10, func(context.Context, []OutboxEvent) error {
		cancel()
		return dispatchErr
	})

	if !errors.Is(err, dispatchErr) {
		t.Fatalf("DispatchBatch error = %v, want it to wrap %v", err, dispatchErr)
	}
	if tx.rollbacks != 1 {
		t.Fatalf("rolled back %d times, want 1", tx.rollbacks)
	}
	if tx.rollbackCtxErr != nil {
		t.Errorf("rollback context error = %v, want nil - the rollback must outlive the canceled caller context", tx.rollbackCtxErr)
	}
	if !tx.rollbackHasDeadline {
		t.Fatal("rollback context has no deadline, so a wedged rollback would hang the worker")
	}
	if remaining := time.Until(tx.rollbackDeadline); remaining <= 0 || remaining > rollbackTimeout {
		t.Errorf("rollback deadline is %v away, want (0, %v]", remaining, rollbackTimeout)
	}
}

func sameEvent(a, b OutboxEvent) bool {
	return a.ID == b.ID && a.RoomID == b.RoomID && string(a.Payload) == string(b.Payload)
}
