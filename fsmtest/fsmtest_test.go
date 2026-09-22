package fsmtest_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ampbase-io/fsm"
	"github.com/ampbase-io/fsm/fsmtest"
	"github.com/ampbase-io/fsm/fsmtest/fake"
	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"
)

type deployReq struct{ Name string }

type deployResp struct{ Status string }

// registerDeploy registers a one-transition deploy FSM whose handler blocks on ctx until
// allowComplete is set, reporting each entry and the cancellation cause it observed.
func registerDeploy(t *testing.T, m *fsm.Manager, allowComplete *atomic.Bool, entered chan<- struct{}, canceled chan<- error) (fsm.Start[deployReq, deployResp], fsm.Resume) {
	t.Helper()
	start, resume, err := m.Register[deployReq, deployResp]("deploy").
		Start("created", func(ctx context.Context, req *fsm.Request[deployReq, deployResp]) (*fsm.Response[deployResp], error) {
			entered <- struct{}{}
			if allowComplete.Load() {
				return fsm.NewResponse(&deployResp{Status: "deployed"}), nil
			}
			<-ctx.Done()
			canceled <- context.Cause(ctx)
			return nil, ctx.Err()
		}).
		End("done").
		Build(context.Background())
	if err != nil {
		t.Fatalf("failed to build deploy FSM: %v", err)
	}
	return start, resume
}

// TestResumeAcrossRestart is the consumer's restart scenario on both backends: a run interrupted
// by shutdown is picked back up by Resume on a fresh manager over the same storage.
func TestResumeAcrossRestart(t *testing.T) {
	fsmtest.RunBackends(t, func(t *testing.T, f *fsmtest.Factory) {
		ctx := context.Background()
		var allowComplete atomic.Bool
		entered := make(chan struct{}, 2)
		canceled := make(chan error, 1)

		m1, stop1 := f.NewManager(nil)
		start, _ := registerDeploy(t, m1, &allowComplete, entered, canceled)
		version, err := start(ctx, "app-1", fsm.NewRequest(&deployReq{Name: "app"}, &deployResp{}))
		if err != nil {
			t.Fatalf("failed to start run: %v", err)
		}
		<-entered
		stop1()
		if cause := <-canceled; !errors.Is(cause, fsm.ErrShutdown) {
			t.Fatalf("expected the interrupted run canceled with ErrShutdown, got %v", cause)
		}

		allowComplete.Store(true)
		m2, _ := f.NewManager(nil)
		_, resume := registerDeploy(t, m2, &allowComplete, entered, canceled)
		if err := resume(ctx); err != nil {
			t.Fatalf("failed to resume: %v", err)
		}
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := m2.Wait(waitCtx, version); err != nil {
			t.Fatalf("resumed run completed with error: %v", err)
		}
	})
}

// TestTakeoverOnLeaseLoss is the consumer's multi-node scenario: an owner that cannot defend its
// lease loses the run to a peer's claim loop, which finishes it; the old owner's handler is
// canceled with ErrLeaseLost, and the lifecycle shows up on the shared bus.
func TestTakeoverOnLeaseLoss(t *testing.T) {
	bus := fake.NewBus()
	f := fsmtest.NewObjectFactory(t,
		fsmtest.WithBus(bus),
		fsmtest.WithObjectConfig(fsmtest.AsymmetricTimings(400*time.Millisecond)),
	)
	ctx := context.Background()
	var allowComplete atomic.Bool
	entered := make(chan struct{}, 2)
	canceled := make(chan error, 1)

	m1, _ := f.NewManager(nil)
	start, _ := registerDeploy(t, m1, &allowComplete, entered, canceled)
	version, err := start(ctx, "app-1", fsm.NewRequest(&deployReq{Name: "app"}, &deployResp{}))
	if err != nil {
		t.Fatalf("failed to start run: %v", err)
	}
	<-entered

	// The peer's claim loop takes the run once the owner's lease lapses; no Resume call is made.
	allowComplete.Store(true)
	m2, _ := f.NewManager(nil)
	registerDeploy(t, m2, &allowComplete, entered, canceled)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("taken-over run completed with error: %v", err)
	}
	select {
	case cause := <-canceled:
		if !errors.Is(cause, fsm.ErrLeaseLost) {
			t.Fatalf("expected the old owner canceled with ErrLeaseLost, got %v", cause)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the old owner's run was never canceled after losing its lease")
	}

	fsmtest.Eventually(t, 2*time.Second, func() bool {
		for _, p := range bus.Published() {
			if p.Subject == "fsm.run.done."+version.String() && p.Event.GetKind() == fsmv1.RunEventKind_RUN_EVENT_KIND_DONE {
				return true
			}
		}
		return false
	}, "expected a done event on the bus for the finished run")
}
