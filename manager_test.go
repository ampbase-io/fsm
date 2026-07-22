package fsm

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

// blockingFSM registers a single-transition FSM whose transition signals entered and then blocks
// until block is closed (or the run is canceled).
func blockingFSM(t *testing.T, m *Manager, action string, entered chan<- struct{}, block <-chan struct{}) Start[orderReq, orderResp] {
	t.Helper()

	start, _, err := m.Register[orderReq, orderResp](action).
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			select {
			case <-block:
				return NewResponse(&orderResp{Status: "ok"}), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).
		End("done").
		Build(context.Background())
	if err != nil {
		t.Fatalf("failed to build %s FSM: %v", action, err)
	}
	return start
}

func TestAlreadyRunning(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	start := blockingFSM(t, m, "already-running", entered, block)

	version, err := start(ctx, "dup-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	_, err = start(ctx, "dup-1", NewRequest(&orderReq{}, &orderResp{}))
	var are *AlreadyRunningError
	if !errors.As(err, &are) {
		t.Fatalf("expected AlreadyRunningError, got %v", err)
	}
	if are.Version != version {
		t.Fatalf("expected conflicting version %s, got %s", version, are.Version)
	}

	close(block)
	if err := m.Wait(ctx, version); err != nil {
		t.Fatalf("FSM completed with error: %v", err)
	}
}

func TestCancel(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	start := blockingFSM(t, m, "cancel", entered, block)

	version, err := start(ctx, "cancel-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	if err := m.Cancel(ctx, version, "operator requested stop"); err != nil {
		t.Fatalf("failed to cancel FSM: %v", err)
	}

	waitErr := m.Wait(ctx, version)
	if waitErr == nil {
		t.Fatal("expected Wait to surface the cancellation cause")
	}
	if !strings.Contains(waitErr.Error(), "operator requested stop") {
		t.Fatalf("expected cancellation cause in error, got %v", waitErr)
	}

	// Canceling an unknown version reports ErrFsmNotFound.
	if err := m.Cancel(ctx, version, "again"); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected ErrFsmNotFound for finished FSM, got %v", err)
	}
}

func TestQueueLimitsConcurrency(t *testing.T) {
	m := newTestManagerWithQueues(t, map[string]int{"deploys": 1})
	ctx := context.Background()

	var (
		inflight   atomic.Int32
		overlapped atomic.Bool
	)

	start, _, err := m.Register[orderReq, orderResp]("queued").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			if inflight.Add(1) > 1 {
				overlapped.Store(true)
			}
			time.Sleep(50 * time.Millisecond)
			inflight.Add(-1)
			return nil, nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	versions := make([]ulid.ULID, 0, 3)
	for _, id := range []string{"q-1", "q-2", "q-3"} {
		v, err := start(ctx, id, NewRequest(&orderReq{}, &orderResp{}), WithQueue("deploys"))
		if err != nil {
			t.Fatalf("failed to start %s: %v", id, err)
		}
		versions = append(versions, v)
	}

	for _, v := range versions {
		if err := m.Wait(ctx, v); err != nil {
			t.Fatalf("queued FSM completed with error: %v", err)
		}
	}

	if overlapped.Load() {
		t.Fatal("queue with size 1 ran FSMs concurrently")
	}
}

func TestRunAfter(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
		bRan    atomic.Bool
	)
	startA := blockingFSM(t, m, "first", entered, block)

	startB, _, err := m.Register[orderReq, orderResp]("second").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			bRan.Store(true)
			return nil, nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build second FSM: %v", err)
	}

	vA, err := startA(ctx, "chain-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start first FSM: %v", err)
	}
	<-entered

	vB, err := startB(ctx, "chain-1", NewRequest(&orderReq{}, &orderResp{}), WithRunAfter(vA))
	if err != nil {
		t.Fatalf("failed to start second FSM: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if bRan.Load() {
		t.Fatal("second FSM ran before the first completed")
	}

	close(block)
	if err := m.Wait(ctx, vB); err != nil {
		t.Fatalf("second FSM completed with error: %v", err)
	}
	if !bRan.Load() {
		t.Fatal("second FSM never ran")
	}
}

func TestChildren(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	parentStart, _, err := m.Register[orderReq, orderResp]("parent").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return nil, nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build parent FSM: %v", err)
	}

	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	childStart := blockingFSM(t, m, "child", entered, block)

	vParent, err := parentStart(ctx, "parent-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start parent: %v", err)
	}
	if err := m.Wait(ctx, vParent); err != nil {
		t.Fatalf("parent completed with error: %v", err)
	}

	vChild, err := childStart(ctx, "child-1", NewRequest(&orderReq{}, &orderResp{}), WithParent(vParent))
	if err != nil {
		t.Fatalf("failed to start child: %v", err)
	}
	<-entered

	children, err := m.Children(ctx, vParent)
	if err != nil {
		t.Fatalf("Children failed: %v", err)
	}
	if len(children) != 1 || children[0] != vChild {
		t.Fatalf("expected children [%s], got %v", vChild, children)
	}

	active, err := m.ActiveChildren(ctx, vParent)
	if err != nil {
		t.Fatalf("ActiveChildren failed: %v", err)
	}
	if len(active) != 1 || active[0].ID != "child-1" {
		t.Fatalf("expected one active child child-1, got %+v", active)
	}

	close(block)
	if err := m.Wait(ctx, vChild); err != nil {
		t.Fatalf("child completed with error: %v", err)
	}

	// A completed child is no longer active but remains in Children.
	active, err = m.ActiveChildren(ctx, vParent)
	if err != nil {
		t.Fatalf("ActiveChildren failed: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("expected no active children after completion, got %+v", active)
	}

	children, err = m.Children(ctx, vParent)
	if err != nil {
		t.Fatalf("Children failed: %v", err)
	}
	if len(children) != 1 {
		t.Fatalf("expected completed child to remain in Children, got %v", children)
	}
}

func TestDelayedStart(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	ranAt := make(chan time.Time, 1)

	start, _, err := m.Register[orderReq, orderResp]("delayed").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			ranAt <- time.Now()
			return nil, nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	target := time.Now().Add(250 * time.Millisecond)
	version, err := start(ctx, "delay-1", NewRequest(&orderReq{}, &orderResp{}), WithDelayedStart(target))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	if err := m.Wait(ctx, version); err != nil {
		t.Fatalf("FSM completed with error: %v", err)
	}

	// Allow a small margin for timer skew.
	if at := <-ranAt; at.Before(target.Add(-20 * time.Millisecond)) {
		t.Fatalf("transition ran at %s, before the delayed start target %s", at, target)
	}
}
