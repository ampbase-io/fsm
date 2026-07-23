package fsm

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"connectrpc.com/connect"
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

func TestAlreadyRunning(t *testing.T) { runBackends(t, testAlreadyRunning) }

func testAlreadyRunning(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
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
	are, ok := errors.AsType[*AlreadyRunningError](err)
	if !ok {
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

func TestCancel(t *testing.T) { runBackends(t, testCancel) }

func testCancel(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
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

	// Canceling a finished FSM reports ErrFsmNotFound. The running-map entry is removed by the
	// run goroutine's deferred cleanup, which can lag Wait's return slightly, so poll briefly.
	deadline := time.After(5 * time.Second)
	for {
		err := m.Cancel(ctx, version, "again")
		if errors.Is(err, ErrFsmNotFound) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected ErrFsmNotFound for finished FSM, got %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestQueueLimitsConcurrency(t *testing.T) { runBackends(t, testQueueLimitsConcurrency) }

func testQueueLimitsConcurrency(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(map[string]int{"deploys": 1})
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

func TestRunAfter(t *testing.T) { runBackends(t, testRunAfter) }

func testRunAfter(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
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

func TestChildren(t *testing.T) { runBackends(t, testChildren) }

func testChildren(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
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

func TestDelayedStart(t *testing.T) { runBackends(t, testDelayedStart) }

func testDelayedStart(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
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

// TestActiveAcrossTypes verifies Active merges runs across every registered resource type for
// an id, deduplicated by run — the Manager queries the store once per distinct type.
func TestActiveAcrossTypes(t *testing.T) { runBackends(t, testActiveAcrossTypes) }

func testActiveAcrossTypes(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	var (
		orderEntered = make(chan struct{}, 1)
		orderBlock   = make(chan struct{})
	)
	startOrder := blockingFSM(t, m, "multi-order", orderEntered, orderBlock)

	var (
		gadgetEntered = make(chan struct{}, 1)
		gadgetBlock   = make(chan struct{})
	)
	startGadget, _, err := m.Register[gadgetReq, gadgetResp]("multi-gadget").
		Start("created", func(ctx context.Context, req *Request[gadgetReq, gadgetResp]) (*Response[gadgetResp], error) {
			gadgetEntered <- struct{}{}
			select {
			case <-gadgetBlock:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build gadget FSM: %v", err)
	}

	const id = "multi-1"
	orderVersion, err := startOrder(ctx, id, NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start order FSM: %v", err)
	}
	<-orderEntered
	gadgetVersion, err := startGadget(ctx, id, NewRequest(&gadgetReq{}, &gadgetResp{}))
	if err != nil {
		t.Fatalf("failed to start gadget FSM: %v", err)
	}
	<-gadgetEntered

	active, err := m.Active(ctx, id)
	if err != nil {
		t.Fatalf("failed to list active runs: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("expected both types' runs active, got %v", active)
	}
	if _, ok := active[ActiveKey{Action: "multi-order", Version: orderVersion}]; !ok {
		t.Fatalf("expected the order run in the active set, got %v", active)
	}
	if _, ok := active[ActiveKey{Action: "multi-gadget", Version: gadgetVersion}]; !ok {
		t.Fatalf("expected the gadget run in the active set, got %v", active)
	}

	close(orderBlock)
	close(gadgetBlock)
	for _, version := range []ulid.ULID{orderVersion, gadgetVersion} {
		if err := m.Wait(ctx, version); err != nil {
			t.Fatalf("FSM completed with error: %v", err)
		}
	}

	active, err = m.Active(ctx, id)
	if err != nil {
		t.Fatalf("failed to list active runs: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("expected no active runs after completion, got %v", active)
	}
}

// TestWaitUnknownRun locks in the not-found contract both Wait paths were rebuilt on: a run
// the backend never recorded reports "gone" (nil), promptly — never a hang or an error. The
// runAfter runner depends on this to start dependents whose predecessor is unknown.
func TestWaitUnknownRun(t *testing.T) { runBackends(t, testWaitUnknownRun) }

func testWaitUnknownRun(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	completingFSM(t, m, "unknown-wait")

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Wait(waitCtx, ulid.Make()); err != nil {
		t.Fatalf("expected nil for an unknown run version, got %v", err)
	}
	if err := m.WaitByID(waitCtx, "never-started"); err != nil {
		t.Fatalf("expected nil for an unknown run id, got %v", err)
	}
}

// TestAdminListActive covers the admin handler's assembly of ListActive responses from the
// store's run state.
func TestAdminListActive(t *testing.T) { runBackends(t, testAdminListActive) }

func testAdminListActive(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	start := blockingFSM(t, m, "admin-list", entered, block)

	version, err := start(ctx, "admin-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	admin := &adminServer{m: m}
	resp, err := admin.ListActive(ctx, connect.NewRequest(&fsmv1.ListActiveRequest{}))
	if err != nil {
		t.Fatalf("ListActive failed: %v", err)
	}
	found := false
	for _, af := range resp.Msg.GetActive() {
		if af.GetVersion() != version.String() {
			continue
		}
		found = true
		if af.GetId() != "admin-1" || af.GetAction() != "admin-list" {
			t.Fatalf("unexpected active FSM: %+v", af)
		}
		if af.GetRunState() == fsmv1.RunState_RUN_STATE_COMPLETE {
			t.Fatal("expected a non-terminal run state")
		}
	}
	if !found {
		t.Fatalf("expected the run in the admin listing, got %v", resp.Msg.GetActive())
	}

	close(block)
	if err := m.Wait(ctx, version); err != nil {
		t.Fatalf("FSM completed with error: %v", err)
	}

	resp, err = admin.ListActive(ctx, connect.NewRequest(&fsmv1.ListActiveRequest{}))
	if err != nil {
		t.Fatalf("ListActive failed: %v", err)
	}
	for _, af := range resp.Msg.GetActive() {
		if af.GetVersion() == version.String() {
			t.Fatalf("expected completed run absent from the admin listing, got %+v", af)
		}
	}
}
