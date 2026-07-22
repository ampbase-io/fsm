package fsm

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

type orderReq struct{ Name string }

type orderResp struct{ Status string }

// newTestManager creates a Manager backed by a short-lived temp dir. The dir is created directly
// under /tmp to keep the admin unix socket path under the sun_path length limit.
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	return newTestManagerWithQueues(t, nil)
}

func newTestManagerWithQueues(t *testing.T, queues map[string]int) *Manager {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "fsm-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	m, err := New(Config{DBPath: dir, Queues: queues})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	t.Cleanup(func() { m.Shutdown(5 * time.Second) })

	return m
}

func TestEndToEnd(t *testing.T) {
	m := newTestManager(t)
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

// TestActiveExactID verifies Active only returns runs for the exact id, not ids sharing a prefix.
func TestActiveExactID(t *testing.T) {
	m := newTestManager(t)
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
// manager restarts on the same data directory.
func TestResumeAfterShutdown(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "fsm-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

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

	m1, err := New(Config{DBPath: dir})
	if err != nil {
		t.Fatalf("failed to create first manager: %v", err)
	}

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
	m1.Shutdown(5 * time.Second)

	allowComplete.Store(true)

	m2, err := New(Config{DBPath: dir})
	if err != nil {
		t.Fatalf("failed to create second manager: %v", err)
	}
	t.Cleanup(func() { m2.Shutdown(5 * time.Second) })

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
	m := newTestManager(t)
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
	var ae *AbortError
	if !errors.As(waitErr, &ae) {
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
		if !errors.As(re.Err, &ae) {
			t.Fatalf("expected finalizer to observe the AbortError, got %v", re.Err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("finalizer never ran")
	}
}

// TestRetryUntilSuccess verifies a failing transition is retried with the attempt count exposed
// via RetryFromContext, and the run completes once the transition succeeds.
func TestRetryUntilSuccess(t *testing.T) {
	m := newTestManager(t)
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
	m := newTestManager(t)
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
