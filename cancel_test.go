package fsm

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ampbase-io/fsm/fsmtest/fake"
	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"connectrpc.com/connect"
)

// within receives from ch, failing the test if nothing arrives in d.
func within[T any](t *testing.T, ch <-chan T, d time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

// TestCancelCause verifies a handler can name why its context ended: an operator's cancel arrives
// as a *CancelError carrying the reason, a shutdown as ErrShutdown.
func TestCancelCause(t *testing.T) { runBackends(t, testCancelCause) }

func testCancelCause(t *testing.T, b *backend) {
	ctx := context.Background()
	m, stop := b.newManager(nil)

	entered := make(chan struct{}, 1)
	causes := make(chan error, 1)
	start, _, err := m.Register[orderReq, orderResp]("cause").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			<-ctx.Done()
			causes <- context.Cause(ctx)
			return nil, ctx.Err()
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "cause-canceled", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	within(t, entered, 10*time.Second, "the canceled run to start")
	if err := m.Cancel(ctx, version, "operator says stop"); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	cause := within(t, causes, 10*time.Second, "the cancel cause")
	if ce, ok := errors.AsType[*CancelError](cause); !ok || ce.Reason != "operator says stop" {
		t.Fatalf("expected a *CancelError carrying the reason, got %T %v", cause, cause)
	}

	if _, err := start(ctx, "cause-shutdown", NewRequest(&orderReq{}, &orderResp{})); err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	within(t, entered, 10*time.Second, "the shut-down run to start")
	stop()
	if cause := within(t, causes, 10*time.Second, "the shutdown cause"); !errors.Is(cause, ErrShutdown) {
		t.Fatalf("expected ErrShutdown, got %T %v", cause, cause)
	}
}

// TestCancelRecordsStoppingState verifies a canceled run reports the state it stopped in, not a
// later skipped one — to its finalizer, in its durable record, and as its Wait outcome.
func TestCancelRecordsStoppingState(t *testing.T) { runBackends(t, testCancelRecordsStoppingState) }

func testCancelRecordsStoppingState(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	entered := make(chan struct{}, 1)
	finalized := make(chan RunErr, 1)
	pass := func(context.Context, *Request[orderReq, orderResp]) (*Response[orderResp], error) {
		return nil, nil
	}
	start, _, err := m.Register[orderReq, orderResp]("stopstate").
		Start("a", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}).
		To("b", pass).
		To("c", pass).
		End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], runErr RunErr) {
			finalized <- runErr
		})).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "stopstate-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	within(t, entered, 10*time.Second, "the run to start")
	if err := m.Cancel(ctx, version, "operator says stop"); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	runErr := within(t, finalized, 10*time.Second, "the finalizer")
	if runErr.State != "a" {
		t.Fatalf("expected the run to report stopping in state a, got %q", runErr.State)
	}
	if _, ok := errors.AsType[*CancelError](runErr.Err); !ok {
		t.Fatalf("expected the finalizer to observe a *CancelError, got %T %v", runErr.Err, runErr.Err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	admin := &adminServer{m: m}
	waitResp, err := admin.Wait(waitCtx, connect.NewRequest(&fsmv1.WaitRequest{Version: version.String()}))
	if err != nil {
		t.Fatalf("expected Wait to report the cancel as the run's outcome, got %v", err)
	}
	if waitResp.Msg.GetError() != "operator says stop" {
		t.Fatalf("expected the cancel reason from Wait, got %q", waitResp.Msg.GetError())
	}

	store, ok := m.store.(*objectStore)
	if !ok {
		return
	}
	if state := mustManifest(t, store, version).GetErrorState(); state != "a" {
		t.Fatalf("expected the manifest to record error state a, got %q", state)
	}
	if !objectGone(t, store, store.cancelKey(version)) {
		t.Fatal("expected the cancel sentinel deleted at finish")
	}
}

// TestFinalizerOutlivesCancel verifies an operator's cancel halts a run's transitions but leaves
// its finalizers a live context: a finalizer that waits on a child run holds the parent — and
// its resource lock — until the child finishes.
func TestFinalizerOutlivesCancel(t *testing.T) { runBackends(t, testFinalizerOutlivesCancel) }

func testFinalizerOutlivesCancel(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	release := make(chan struct{})
	startChild, _, err := m.Register[orderReq, orderResp]("finalizer-child").
		Start("soak", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build child FSM: %v", err)
	}

	type seededKey struct{}

	entered := make(chan struct{}, 1)
	finalizing := make(chan error, 1)
	deadlined := make(chan bool, 1)
	seeded := make(chan any, 1)
	childDone := make(chan error, 1)
	startParent, _, err := m.Register[orderReq, orderResp]("finalizer-parent").
		Start("hold", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		},
			// An initializer's values must reach the finalizer; its cancellation must not.
			WithInitializers(func(ctx context.Context, req *Request[orderReq, orderResp]) context.Context {
				ctx, cancel := context.WithTimeout(context.WithValue(ctx, seededKey{}, "seeded"), time.Hour)
				t.Cleanup(cancel)
				return ctx
			}),
		).
		End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], _ RunErr) {
			_, hasDeadline := ctx.Deadline()
			deadlined <- hasDeadline
			seeded <- ctx.Value(seededKey{})
			finalizing <- ctx.Err()
			child, err := startChild(ctx, "finalizer-1/child", NewRequest(&orderReq{}, &orderResp{}), WithParent(req.Run().StartVersion))
			if err != nil {
				childDone <- err
				return
			}
			childDone <- m.Wait(ctx, child)
		})).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build parent FSM: %v", err)
	}

	parent, err := startParent(ctx, "finalizer-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start parent: %v", err)
	}
	within(t, entered, 10*time.Second, "the parent to start")
	if err := m.Cancel(ctx, parent, "operator says stop"); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	if within(t, deadlined, 10*time.Second, "the finalizer") {
		t.Fatal("expected the finalizer's context to carry no deadline")
	}
	if got := within(t, seeded, 10*time.Second, "the finalizer"); got != "seeded" {
		t.Fatalf("expected the finalizer's context to carry the initializer's value, got %v", got)
	}
	if err := within(t, finalizing, 10*time.Second, "the finalizer"); err != nil {
		t.Fatalf("expected the finalizer's context live after an operator cancel, got %v", err)
	}

	// The finalizer is blocked on the child, so the parent is still running and holds its lock.
	select {
	case err := <-childDone:
		t.Fatalf("the finalizer's wait on its child returned while the child was still running: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	_, err = startParent(ctx, "finalizer-1", NewRequest(&orderReq{}, &orderResp{}))
	if _, already := errors.AsType[*AlreadyRunningError](err); !already {
		t.Fatalf("expected the finalizing parent to still hold its lock, got %v", err)
	}

	close(release)
	if err := within(t, childDone, 10*time.Second, "the finalizer's wait on its child"); err != nil {
		t.Fatalf("expected the child to finish cleanly, got %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m.Wait(waitCtx, parent); err == nil || err.Error() != "operator says stop" {
		t.Fatalf("expected the parent to finish with the cancel reason, got %v", err)
	}
}

// TestShutdownReleasesFinalizer verifies the other half of the finalizer's context contract: it
// survives an operator's cancel but not a shutdown, so a blocked finalizer never holds a
// redeploy to the shutdown timeout, and the unfinished run resumes elsewhere.
func TestShutdownReleasesFinalizer(t *testing.T) { runBackends(t, testShutdownReleasesFinalizer) }

func testShutdownReleasesFinalizer(t *testing.T, b *backend) {
	ctx := context.Background()

	var resumed atomic.Bool
	entered := make(chan struct{}, 1)
	finalizing := make(chan struct{}, 1)
	causes := make(chan error, 1)
	register := func(m *Manager) (Start[orderReq, orderResp], Resume, error) {
		return m.Register[orderReq, orderResp]("finalizer-shutdown").
			Start("hold", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				entered <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			}).
			End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], _ RunErr) {
				if resumed.Load() {
					return
				}
				finalizing <- struct{}{}
				<-ctx.Done()
				causes <- context.Cause(ctx)
			})).
			Build(ctx)
	}

	m1, stop1 := b.newManager(nil)
	start, _, err := register(m1)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	version, err := start(ctx, "finalizer-shutdown-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	within(t, entered, 10*time.Second, "the run to start")
	if err := m1.Cancel(ctx, version, "operator says stop"); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	within(t, finalizing, 10*time.Second, "the finalizer")

	began := time.Now()
	stop1()
	if took := time.Since(began); took > 2*time.Second {
		t.Fatalf("shutdown took %s: the blocked finalizer held it to the timeout", took)
	}
	if cause := within(t, causes, 10*time.Second, "the finalizer's cancel cause"); !errors.Is(cause, ErrShutdown) {
		t.Fatalf("expected the finalizer released by ErrShutdown, got %T %v", cause, cause)
	}

	resumed.Store(true)
	m2, _ := b.newManager(nil)
	_, resume, err := register(m2)
	if err != nil {
		t.Fatalf("failed to rebuild FSM: %v", err)
	}
	if err := resume(ctx); err != nil {
		t.Fatalf("failed to resume: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err == nil || !strings.Contains(err.Error(), "operator says stop") {
		t.Fatalf("expected the resumed run to finish with the cancel reason, got %v", err)
	}
}

// TestCancelLandingOnCompletedHandler verifies an accepted cancel is the run's outcome even when
// the handler it landed on never read its context and returned success: Cancel has already
// answered its caller, so the run must not finish as a success behind it.
func TestCancelLandingOnCompletedHandler(t *testing.T) {
	runBackends(t, testCancelLandingOnCompletedHandler)
}

func testCancelLandingOnCompletedHandler(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	running := make(chan context.Context, 1)
	release := make(chan struct{})
	finalized := make(chan RunErr, 1)
	start, _, err := m.Register[orderReq, orderResp]("cancel-ignored").
		Start("work", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			running <- ctx
			<-release
			return NewResponse(&orderResp{Status: "ok"}), nil
		}).
		End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], runErr RunErr) {
			finalized <- runErr
		})).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "cancel-ignored-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	handlerCtx := within(t, running, 10*time.Second, "the run to start")
	if err := m.Cancel(ctx, version, "operator says stop"); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	eventually(t, 10*time.Second, func() bool { return handlerCtx.Err() != nil }, "the cancel never reached the handler's context")
	close(release)

	runErr := within(t, finalized, 10*time.Second, "the finalizer")
	if _, ok := errors.AsType[*CancelError](runErr.Err); !ok || runErr.State != "work" {
		t.Fatalf("expected the finalizer to observe the cancel in state work, got %+v", runErr)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m.Wait(waitCtx, version); err == nil || err.Error() != "operator says stop" {
		t.Fatalf("expected the accepted cancel as the run's outcome, got %v", err)
	}
}

// TestStrayCanceledIsRetried verifies a handler's own context.Canceled — from a context it
// derived, while the run's is live — is an ordinary failure, not a completed transition.
func TestStrayCanceledIsRetried(t *testing.T) { runBackends(t, testStrayCanceledIsRetried) }

func testStrayCanceledIsRetried(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	var attempts atomic.Int32
	start, _, err := m.Register[orderReq, orderResp]("stray").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			if attempts.Add(1) == 1 {
				return nil, context.Canceled
			}
			return NewResponse(&orderResp{Status: "ok"}), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "stray-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m.Wait(waitCtx, version); err != nil {
		t.Fatalf("expected the run to complete after a retry, got %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected the stray context.Canceled to be retried once, got %d attempts", got)
	}
}

// TestCancelRunningAcrossNodesViaBus verifies subject-addressed cancel end to end: a node that
// does not own a run issues the cancel, the durable sentinel + broadcast reach the owning node,
// and its cancel sweep stops the executing run with the recorded cause.
func TestCancelRunningAcrossNodesViaBus(t *testing.T) {
	bus := fake.NewBus()
	b := newObjectBackendWithBus(t, bus, nil)
	ctx := context.Background()

	m1, _ := b.newManager(nil)
	entered := make(chan struct{}, 1)
	canceled := make(chan error, 1)
	start, _, err := m1.Register[orderReq, orderResp]("xcancel").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			<-ctx.Done()
			canceled <- context.Cause(ctx)
			return nil, ctx.Err()
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "xcancel-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	// A second node — which owns nothing and never registered the FSM — issues the cancel. With
	// the periodic heartbeat sweep 10s out by default, only the broadcast can deliver in time.
	m2, _ := b.newManager(nil)
	if err := m2.Cancel(ctx, version, "stop from another node"); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	// The owner rebuilds the cause from the sentinel, so the type must survive the node boundary.
	cause := within(t, canceled, 5*time.Second, "the cancel broadcast to stop the run")
	if ce, ok := errors.AsType[*CancelError](cause); !ok || ce.Reason != "stop from another node" {
		t.Fatalf("expected a *CancelError carrying the reason, got %T %v", cause, cause)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	switch err := m1.Wait(waitCtx, version); {
	case err == nil:
		t.Fatal("expected Wait to surface the cancellation")
	case !strings.Contains(err.Error(), "stop from another node"):
		t.Fatalf("expected the cancel cause from Wait, got %v", err)
	}
}

// TestCancelViaSweepFloorNoBus verifies the correctness floor: with no bus injected the cancel
// rides the durable sentinel alone, and the owning node's heartbeat sweep discovers it and stops
// the run — no broadcast involved.
func TestCancelViaSweepFloorNoBus(t *testing.T) {
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		cfg.HeartbeatPeriod = 200 * time.Millisecond
		cfg.LeaseTimeout = 2 * time.Second
	})
	ctx := context.Background()

	m1, _ := b.newManager(nil)
	entered := make(chan struct{}, 1)
	canceled := make(chan error, 1)
	start, _, err := m1.Register[orderReq, orderResp]("sweepcancel").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			<-ctx.Done()
			canceled <- context.Cause(ctx)
			return nil, ctx.Err()
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "sweepcancel-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	m2, _ := b.newManager(nil)
	if err := m2.Cancel(ctx, version, "stop via the sweep floor"); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	select {
	case cause := <-canceled:
		if !strings.Contains(cause.Error(), "stop via the sweep floor") {
			t.Fatalf("expected the cancel cause, got %v", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the run was not canceled by the heartbeat sweep")
	}
}

// TestCancelDelayedRunBeforeExecution verifies the canceled-before-execution path: a delayed run
// this node owns but has not begun executing is driven to a terminal canceled manifest, so its
// waiters resolve with the cause instead of blocking to the delay, and its transition never runs.
func TestCancelDelayedRunBeforeExecution(t *testing.T) {
	bus := fake.NewBus()
	b := newObjectBackendWithBus(t, bus, nil)
	ctx := context.Background()

	m, _ := b.newManager(nil)
	var ran atomic.Bool
	start, _, err := m.Register[orderReq, orderResp]("delaycancel").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			ran.Store(true)
			return NewResponse(&orderResp{Status: "ok"}), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	const delay = 1 * time.Second
	version, err := start(ctx, "delaycancel-1", NewRequest(&orderReq{}, &orderResp{}), WithDelayedStart(time.Now().Add(delay)))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}

	if err := m.Cancel(ctx, version, "cancel before it runs"); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	// The waiter must resolve with the cause well before the delay elapses, not poll to its
	// deadline — the owner drives the queued run to terminal itself.
	waitCtx, cancel := context.WithTimeout(ctx, delay/2)
	defer cancel()
	switch err := m.Wait(waitCtx, version); {
	case err == nil:
		t.Fatal("expected Wait to resolve with the cancellation, not success")
	case !strings.Contains(err.Error(), "cancel before it runs"):
		t.Fatalf("expected the cancel cause from Wait, got %v", err)
	}

	// Past the original delay, confirm the transition never executed: dropping the lease at the
	// terminal drive makes the delayed dispatch cancel itself at execute-start.
	time.Sleep(delay)
	if ran.Load() {
		t.Fatal("a run canceled before execution must never run its transition")
	}
}
