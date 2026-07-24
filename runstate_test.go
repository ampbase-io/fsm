package fsm

import (
	"context"
	"testing"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// completingFSM registers the blockingFSM shape (created → done) with a transition that
// completes immediately, returning the resume function used to adopt runs started elsewhere.
func completingFSM(t *testing.T, m *Manager, action string) Resume {
	t.Helper()

	_, resume, err := m.Register[orderReq, orderResp](action).
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return NewResponse(&orderResp{Status: "resumed"}), nil
		}).
		End("done").
		Build(context.Background())
	if err != nil {
		t.Fatalf("failed to build %s FSM: %v", action, err)
	}
	return resume
}

// crossManagerTimings expires leases quickly while suppressing heartbeats and the periodic
// claim pass, so a second manager's explicit Resume is the deterministic takeover path.
func crossManagerTimings(cfg *ObjectStorageConfig) {
	cfg.LeaseTimeout = 250 * time.Millisecond
	cfg.HeartbeatPeriod = time.Hour
	cfg.ClaimInterval = time.Hour
}

// takeOver starts a blocking run on a first manager, lets its lease expire, and completes the
// run on a second manager via Resume. It returns the first manager, the run version, and the
// releaser for that manager's still-blocked transition.
func takeOver(t *testing.T, f *managerFactory, action, id string) (m1 *Manager, version ulid.ULID, release func()) {
	t.Helper()
	ctx := context.Background()

	m1, _ = f.newManager(nil)
	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	start := blockingFSM(t, m1, action, entered, block)

	version, err := start(ctx, id, NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	// Once the lease expires the run is claimable; the owner's heartbeat is suppressed.
	time.Sleep(2 * 250 * time.Millisecond)

	m2, _ := f.newManager(nil)
	resume := completingFSM(t, m2, action)
	if err := resume(ctx); err != nil {
		t.Fatalf("failed to resume on second manager: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("resumed run completed with error: %v", err)
	}

	return m1, version, func() { close(block) }
}

// TestObjectWaitCrossManager is the regression test for the stale-local-index hang: when a run
// started on one manager migrates to another manager sharing the bucket and finishes there,
// the first manager's Wait must observe the completion instead of parking on local state that
// will never change.
func TestObjectWaitCrossManager(t *testing.T) {
	f := newObjectFactoryWith(t, crossManagerTimings)

	m1, version, release := takeOver(t, f, "xwait", "cross-1")
	defer release()

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m1.Wait(waitCtx, version); err != nil {
		t.Fatalf("cross-manager Wait: %v", err)
	}
}

// TestObjectWaitByIDCrossManager covers id resolution once the resource lock is gone: the
// completed run must be found through the index/ fallback and report its outcome.
func TestObjectWaitByIDCrossManager(t *testing.T) {
	f := newObjectFactoryWith(t, crossManagerTimings)

	m1, _, release := takeOver(t, f, "xwaitid", "cross-2")
	defer release()

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m1.WaitByID(waitCtx, "cross-2"); err != nil {
		t.Fatalf("cross-manager WaitByID: %v", err)
	}
}

// TestObjectActiveCrossManager verifies a manager answers Active and ActiveChildren from
// object storage for runs it did not start.
func TestObjectActiveCrossManager(t *testing.T) {
	f := newObjectFactory(t)
	ctx := context.Background()

	m1, _ := f.newManager(nil)
	var (
		parentEntered = make(chan struct{}, 1)
		parentBlock   = make(chan struct{})
		childEntered  = make(chan struct{}, 1)
		childBlock    = make(chan struct{})
	)
	startParent := blockingFSM(t, m1, "xparent", parentEntered, parentBlock)
	startChild := blockingFSM(t, m1, "xchild", childEntered, childBlock)

	parentVersion, err := startParent(ctx, "cross-3", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start parent FSM: %v", err)
	}
	<-parentEntered

	childVersion, err := startChild(ctx, "cross-3-child", NewRequest(&orderReq{}, &orderResp{}), WithParent(parentVersion))
	if err != nil {
		t.Fatalf("failed to start child FSM: %v", err)
	}
	<-childEntered

	// A second manager with the same registrations but no local run state.
	m2, _ := f.newManager(nil)
	completingFSM(t, m2, "xparent")
	completingFSM(t, m2, "xchild")

	active, err := m2.Active(ctx, "cross-3")
	if err != nil {
		t.Fatalf("failed to list active runs: %v", err)
	}
	state, ok := active[ActiveKey{Action: "xparent", Version: parentVersion}]
	if !ok {
		t.Fatalf("expected active parent run on second manager, got %v", active)
	}
	if state == fsmv1.RunState_RUN_STATE_COMPLETE {
		t.Fatal("expected a non-terminal run state")
	}

	children, err := m2.ActiveChildren(ctx, parentVersion)
	if err != nil {
		t.Fatalf("failed to list active children: %v", err)
	}
	if len(children) != 1 {
		t.Fatalf("expected one active child, got %v", children)
	}
	child := children[0]
	if child.StartVersion != childVersion || child.ID != "cross-3-child" || child.Action != "xchild" {
		t.Fatalf("unexpected child run: %+v", child)
	}
	if child.Parent != parentVersion {
		t.Fatalf("expected child parent %s, got %s", parentVersion, child.Parent)
	}
	if child.ResourceName != "order_req" {
		t.Fatalf("expected the resource alias restored from the registered FSM, got %q", child.ResourceName)
	}

	close(parentBlock)
	close(childBlock)
	if err := m1.Wait(ctx, parentVersion); err != nil {
		t.Fatalf("parent completed with error: %v", err)
	}
	if err := m1.Wait(ctx, childVersion); err != nil {
		t.Fatalf("child completed with error: %v", err)
	}

	for _, m := range []*Manager{m1, m2} {
		active, err := m.Active(ctx, "cross-3")
		if err != nil {
			t.Fatalf("failed to list active runs: %v", err)
		}
		if len(active) != 0 {
			t.Fatalf("expected no active runs after completion, got %v", active)
		}
		children, err := m.ActiveChildren(ctx, parentVersion)
		if err != nil {
			t.Fatalf("failed to list active children: %v", err)
		}
		if len(children) != 0 {
			t.Fatalf("expected no active children after completion, got %v", children)
		}
	}
}

func TestListActive(t *testing.T) { runBackends(t, testListActive) }

func testListActive(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	start := blockingFSM(t, m, "list-active", entered, block)

	version, err := start(ctx, "list-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	states, err := m.store.ListActive(ctx)
	if err != nil {
		t.Fatalf("failed to list active runs: %v", err)
	}
	found := false
	for _, rs := range states {
		if rs.StartVersion != version {
			continue
		}
		found = true
		if rs.ID != "list-1" {
			t.Fatalf("unexpected run id %q", rs.ID)
		}
		if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
			t.Fatal("expected a non-terminal run state")
		}
	}
	if !found {
		t.Fatalf("expected the run in ListActive, got %v", states)
	}

	close(block)
	if err := m.Wait(ctx, version); err != nil {
		t.Fatalf("FSM completed with error: %v", err)
	}

	states, err = m.store.ListActive(ctx)
	if err != nil {
		t.Fatalf("failed to list active runs: %v", err)
	}
	for _, rs := range states {
		if rs.StartVersion == version {
			t.Fatalf("expected completed run absent from ListActive, got %+v", rs)
		}
	}
}
