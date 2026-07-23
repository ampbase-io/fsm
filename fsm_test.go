package fsm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"

	"go.etcd.io/bbolt"
)

type orderReq struct{ Name string }

type orderResp struct{ Status string }

type gadgetReq struct{ Name string }

type gadgetResp struct{ Status string }

// newTestManager creates a BoltDB-backed Manager for tests that are specific to that backend;
// backend-agnostic behavior tests use runBackends instead.
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m, _ := newBoltFactory(t).newManager(nil)
	return m
}

func TestEndToEnd(t *testing.T) { runBackends(t, testEndToEnd) }

func testEndToEnd(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	var transitions atomic.Int32

	start, _, err := m.Register[orderReq, orderResp]("create").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			transitions.Add(1)
			return NewResponse(&orderResp{Status: "created:" + req.Msg.Name}), nil
		}).
		To("verified", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			transitions.Add(1)
			if req.W.Msg == nil || req.W.Msg.Status != "created:widget" {
				return nil, errors.New("previous response did not propagate")
			}
			return req.W.Msg.toResponse("verified"), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "order-1", NewRequest(&orderReq{Name: "widget"}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}

	if err := m.Wait(ctx, version); err != nil {
		t.Fatalf("FSM completed with error: %v", err)
	}

	if got := transitions.Load(); got != 2 {
		t.Fatalf("expected 2 transitions, got %d", got)
	}
}

func (r *orderResp) toResponse(status string) *Response[orderResp] {
	return NewResponse(&orderResp{Status: status})
}

// TestHistoryAfterArchive verifies a completed run can still be looked up via History after the
// archive loop has moved it from the archive bucket into the history DB.
func TestHistoryAfterArchive(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	start, _, err := m.Register[orderReq, orderResp]("archive").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return NewResponse(&orderResp{Status: "ok"}), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "order-2", NewRequest(&orderReq{Name: "widget"}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	if err := m.Wait(ctx, version); err != nil {
		t.Fatalf("FSM completed with error: %v", err)
	}

	if _, err := m.store.History(ctx, version); err != nil {
		t.Fatalf("History before archive: %v", err)
	}

	bs, ok := m.store.(*boltStore)
	if !ok {
		t.Fatalf("expected boltStore, got %T", m.store)
	}
	bs.archiveCh <- struct{}{}

	// Wait for the archive loop to drain the archive bucket.
	deadline := time.After(10 * time.Second)
	for {
		var archived int
		bs.db.View(func(tx *bbolt.Tx) error {
			archived = tx.Bucket(archiveBucket).Stats().KeyN
			return nil
		})
		if archived == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for archive loop")
		case <-time.After(10 * time.Millisecond):
		}
	}

	he, err := m.store.History(ctx, version)
	if err != nil {
		t.Fatalf("History after archive: %v", err)
	}
	if he.GetLastEvent() == nil {
		t.Fatal("expected last event in archived history")
	}
}

// TestHistory verifies a completed run is immediately retrievable via History on both backends
// (BoltDB serves it from the archive bucket, object storage from the FINISH-time history object).
func TestHistory(t *testing.T) { runBackends(t, testHistory) }

func testHistory(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	start, _, err := m.Register[orderReq, orderResp]("history").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return NewResponse(&orderResp{Status: "ok"}), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "order-3", NewRequest(&orderReq{Name: "widget"}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	if err := m.Wait(ctx, version); err != nil {
		t.Fatalf("FSM completed with error: %v", err)
	}

	he, err := m.store.History(ctx, version)
	if err != nil {
		t.Fatalf("History failed: %v", err)
	}
	if he.GetLastEvent().GetType() != fsmv1.EventType_EVENT_TYPE_FINISH {
		t.Fatalf("expected FINISH last event, got %v", he.GetLastEvent().GetType())
	}
}

// TestRuns verifies past runs for a resource stay enumerable after completion, across actions,
// scoped to the requested id.
func TestRuns(t *testing.T) { runBackends(t, testRuns) }

func testRuns(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	registerAction := func(action string) Start[orderReq, orderResp] {
		start, _, err := m.Register[orderReq, orderResp](action).
			Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				return nil, nil
			}).
			End("done").
			Build(ctx)
		if err != nil {
			t.Fatalf("failed to build %s FSM: %v", action, err)
		}
		return start
	}
	createStart := registerAction("create")
	deployStart := registerAction("deploy")

	runAndWait := func(start Start[orderReq, orderResp], id string) ulid.ULID {
		t.Helper()
		v, err := start(ctx, id, NewRequest(&orderReq{}, &orderResp{}))
		if err != nil {
			t.Fatalf("failed to start %s: %v", id, err)
		}
		if err := m.Wait(ctx, v); err != nil {
			t.Fatalf("%s completed with error: %v", id, err)
		}
		// ULIDs within the same millisecond are not ordered; space runs out so the
		// chronological ordering assertion below is deterministic.
		time.Sleep(2 * time.Millisecond)
		return v
	}

	// A second resource TYPE sharing the same id: exercises the cross-type dedup in Manager.Runs
	// (the Bolt backend keys events by id alone, so without dedup its runs repeat per type).
	gadgetStart, _, err := m.Register[gadgetReq, gadgetResp]("assemble").
		Start("created", func(ctx context.Context, req *Request[gadgetReq, gadgetResp]) (*Response[gadgetResp], error) {
			return nil, nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build gadget FSM: %v", err)
	}

	v1 := runAndWait(createStart, "res-1")
	v2 := runAndWait(deployStart, "res-1")
	v3 := runAndWait(createStart, "res-1")
	runAndWait(createStart, "res-2")

	v4, err := gadgetStart(ctx, "res-1", NewRequest(&gadgetReq{}, &gadgetResp{}))
	if err != nil {
		t.Fatalf("failed to start gadget: %v", err)
	}
	if err := m.Wait(ctx, v4); err != nil {
		t.Fatalf("gadget completed with error: %v", err)
	}

	runs, err := m.Runs(ctx, "res-1")
	if err != nil {
		t.Fatalf("Runs failed: %v", err)
	}
	want := []ulid.ULID{v1, v2, v3, v4}
	if len(runs) != len(want) {
		t.Fatalf("expected %d completed runs for res-1, got %d: %v", len(want), len(runs), runs)
	}
	for i, v := range want {
		if runs[i] != v {
			t.Fatalf("expected chronological runs %v, got %v", want, runs)
		}
	}

	// Every returned version resolves to details via History.
	for _, v := range runs {
		if _, err := m.store.History(ctx, v); err != nil {
			t.Fatalf("History for run %s failed: %v", v, err)
		}
	}

	if runs, err := m.Runs(ctx, "res-unknown"); err != nil || len(runs) != 0 {
		t.Fatalf("expected no runs for unknown resource, got %v (err %v)", runs, err)
	}
}

// TestActiveExactID verifies Active only returns runs for the exact id, not ids sharing a prefix.
func TestActiveExactID(t *testing.T) { runBackends(t, testActiveExactID) }

func testActiveExactID(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	var (
		block   = make(chan struct{})
		started = make(chan struct{}, 2)
	)

	start, _, err := m.Register[orderReq, orderResp]("provision").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			started <- struct{}{}
			select {
			case <-block:
			case <-ctx.Done():
			}
			return nil, nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	if _, err := start(ctx, "machine-1", NewRequest(&orderReq{}, &orderResp{})); err != nil {
		t.Fatalf("failed to start machine-1: %v", err)
	}
	v10, err := start(ctx, "machine-10", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start machine-10: %v", err)
	}

	<-started
	<-started

	active, err := m.Active(ctx, "machine-1")
	if err != nil {
		t.Fatalf("Active failed: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected exactly 1 active run for machine-1, got %d: %v", len(active), active)
	}

	// Wait for one run by id and the other by version to cover both wait paths; WaitByID is
	// called while the run may still be executing, exercising the watch loop.
	waitErr := make(chan error, 1)
	go func() { waitErr <- m.WaitByID(ctx, "machine-1") }()

	close(block)
	if err := <-waitErr; err != nil {
		t.Fatalf("machine-1 completed with error: %v", err)
	}
	if err := m.Wait(ctx, v10); err != nil {
		t.Fatalf("machine-10 completed with error: %v", err)
	}
}

// TestResumeAfterShutdown verifies an interrupted run is picked back up by Resume after the
// manager restarts on the same storage.
func TestResumeAfterShutdown(t *testing.T) { runBackends(t, testResumeAfterShutdown) }

func testResumeAfterShutdown(t *testing.T, f *managerFactory) {
	ctx := context.Background()

	var allowComplete, sawRestart atomic.Bool
	completed := make(chan struct{}, 1)
	entered := make(chan struct{}, 2)

	register := func(m *Manager) (Start[orderReq, orderResp], Resume, error) {
		return m.Register[orderReq, orderResp]("deploy").
			Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				sawRestart.Store(IsRestartFromContext(ctx))
				entered <- struct{}{}
				if !allowComplete.Load() {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return NewResponse(&orderResp{Status: "deployed"}), nil
			}).
			End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], runErr RunErr) {
				if runErr.Err == nil {
					completed <- struct{}{}
				}
			})).
			Build(ctx)
	}

	m1, stop1 := f.newManager(nil)

	start, _, err := register(m1)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	if _, err := start(ctx, "app-1", NewRequest(&orderReq{Name: "app"}, &orderResp{})); err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}

	// Shut down while the first transition is blocked so no FINISH event is recorded.
	<-entered
	if sawRestart.Load() {
		t.Fatal("fresh run should not be marked as a restart")
	}
	stop1()

	allowComplete.Store(true)

	m2, _ := f.newManager(nil)

	_, resume, err := register(m2)
	if err != nil {
		t.Fatalf("failed to rebuild FSM: %v", err)
	}

	if err := resume(ctx); err != nil {
		t.Fatalf("failed to resume: %v", err)
	}

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("resumed FSM never re-entered the first transition")
	}
	if !sawRestart.Load() {
		t.Fatal("resumed run should be marked as a restart")
	}

	select {
	case <-completed:
	case <-time.After(10 * time.Second):
		t.Fatal("resumed FSM never completed")
	}
}

// TestAbortSkipsRemainingTransitions verifies an aborting transition halts the run: later
// transitions are skipped, the finalizer observes the failure, and Wait surfaces the error.
func TestAbortSkipsRemainingTransitions(t *testing.T) {
	runBackends(t, testAbortSkipsRemainingTransitions)
}

func testAbortSkipsRemainingTransitions(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	var thirdRan atomic.Bool
	finalErr := make(chan RunErr, 1)

	start, _, err := m.Register[orderReq, orderResp]("abort").
		Start("one", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return NewResponse(&orderResp{Status: "one"}), nil
		}).
		To("two", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return nil, Abort(errors.New("boom"))
		}).
		To("three", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			thirdRan.Store(true)
			return nil, nil
		}).
		End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], runErr RunErr) {
			finalErr <- runErr
		})).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "abort-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}

	waitErr := m.Wait(ctx, version)
	ae, ok := errors.AsType[*AbortError](waitErr)
	if !ok {
		t.Fatalf("expected Wait to return an AbortError, got %v", waitErr)
	}
	if ae.Error() != "boom" {
		t.Fatalf("unexpected abort error: %v", ae)
	}

	if thirdRan.Load() {
		t.Fatal("transition after the abort should have been skipped")
	}

	select {
	case re := <-finalErr:
		if re.State != "two" {
			t.Fatalf("expected failure recorded in state two, got %q", re.State)
		}
		if _, ok := errors.AsType[*AbortError](re.Err); !ok {
			t.Fatalf("expected finalizer to observe the AbortError, got %v", re.Err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("finalizer never ran")
	}
}

// TestRetryUntilSuccess verifies a failing transition is retried with the attempt count exposed
// via RetryFromContext, and the run completes once the transition succeeds.
func TestRetryUntilSuccess(t *testing.T) { runBackends(t, testRetryUntilSuccess) }

func testRetryUntilSuccess(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	var (
		attempts  atomic.Int32
		lastRetry atomic.Uint64
	)

	start, _, err := m.Register[orderReq, orderResp]("retry").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			lastRetry.Store(RetryFromContext(ctx))
			if attempts.Add(1) < 3 {
				return nil, errors.New("flaky")
			}
			return NewResponse(&orderResp{Status: "ok"}), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "retry-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	if err := m.Wait(ctx, version); err != nil {
		t.Fatalf("FSM completed with error: %v", err)
	}

	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
	if got := lastRetry.Load(); got != 2 {
		t.Fatalf("expected retry count 2 on final attempt, got %d", got)
	}
}

// TestInitializersInterceptorsFinalizers verifies the option hooks: initializers seed the
// transition context, interceptors wrap execution, and finalizers run on success with no error.
func TestInitializersInterceptorsFinalizers(t *testing.T) {
	runBackends(t, testInitializersInterceptorsFinalizers)
}

func testInitializersInterceptorsFinalizers(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	type ctxKey struct{}

	var (
		sawValue     atomic.Bool
		intercepted  atomic.Int32
		finalizerErr = make(chan RunErr, 1)
	)

	start, _, err := m.Register[orderReq, orderResp]("hooks").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			sawValue.Store(ctx.Value(ctxKey{}) == "seeded")
			return NewResponse(&orderResp{Status: "ok"}), nil
		},
			WithInitializers(func(ctx context.Context, req *Request[orderReq, orderResp]) context.Context {
				return context.WithValue(ctx, ctxKey{}, "seeded")
			}),
			WithInterceptors[orderReq, orderResp](func(next TransitionFunc) TransitionFunc {
				return func(ctx context.Context, req AnyRequest) (AnyResponse, error) {
					intercepted.Add(1)
					return next(ctx, req)
				}
			}),
		).
		End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], runErr RunErr) {
			finalizerErr <- runErr
		})).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "hooks-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	if err := m.Wait(ctx, version); err != nil {
		t.Fatalf("FSM completed with error: %v", err)
	}

	if !sawValue.Load() {
		t.Fatal("transition did not observe the initializer-seeded context value")
	}
	if got := intercepted.Load(); got == 0 {
		t.Fatal("interceptor never ran")
	}

	select {
	case re := <-finalizerErr:
		if re.Err != nil {
			t.Fatalf("expected clean finalizer error, got %+v", re)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("finalizer never ran")
	}
}
