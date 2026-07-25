package fsm

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestCancelRunningAcrossNodesViaBus verifies subject-addressed cancel end to end: a node that
// does not own a run issues the cancel, the durable sentinel + broadcast reach the owning node,
// and its cancel sweep stops the executing run with the recorded cause.
func TestCancelRunningAcrossNodesViaBus(t *testing.T) {
	bus := newTestBus()
	f := newObjectFactoryWithBus(t, bus, nil)
	ctx := context.Background()

	m1, _ := f.newManager(nil)
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
	m2, _ := f.newManager(nil)
	if err := m2.Cancel(ctx, version, "stop from another node"); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	select {
	case cause := <-canceled:
		if !strings.Contains(cause.Error(), "stop from another node") {
			t.Fatalf("expected the cancel cause, got %v", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the run was not canceled via the cancel broadcast")
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
	f := newObjectFactoryWith(t, func(cfg *ObjectStorageConfig) {
		cfg.HeartbeatPeriod = 200 * time.Millisecond
		cfg.LeaseTimeout = 2 * time.Second
	})
	ctx := context.Background()

	m1, _ := f.newManager(nil)
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

	m2, _ := f.newManager(nil)
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
	bus := newTestBus()
	f := newObjectFactoryWithBus(t, bus, nil)
	ctx := context.Background()

	m, _ := f.newManager(nil)
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
