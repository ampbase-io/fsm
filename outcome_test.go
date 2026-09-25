package fsm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

// haltingFSM registers an FSM whose first transition halts by the request's Name: "cancel"
// blocks until its context ends, "abort" aborts, "system" and "user" fail unrecoverably, and
// anything else passes. entered reports each entry into the blocking branch.
func haltingFSM(t *testing.T, m *Manager, action string, entered chan<- struct{}) Start[orderReq, orderResp] {
	t.Helper()
	start, _, err := m.Register[orderReq, orderResp](action).
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			switch req.Msg.Name {
			case "cancel":
				entered <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			case "abort":
				return nil, Abort(errors.New("boom"))
			case "system":
				return nil, NewUnrecoverableSystemError(errors.New("disk gone"))
			case "user":
				return nil, NewUnrecoverableUserError(errors.New("bad input"))
			default:
				return NewResponse(&orderResp{Status: "ok"}), nil
			}
		}).
		End("done").
		Build(context.Background())
	if err != nil {
		t.Fatalf("failed to build %s FSM: %v", action, err)
	}
	return start
}

func isAbort(err error) bool {
	_, ok := errors.AsType[*AbortError](err)
	return ok
}

// TestOutcomeError pins the rebuild of a recorded error from its kind: each kind yields its
// type inside a halt, and a kind this version does not know — or the empty kind of an older
// record — yields the message alone.
func TestOutcomeError(t *testing.T) {
	cases := []struct {
		kind string
		want func(error) bool
	}{
		{kindCanceled, func(err error) bool { c, ok := errors.AsType[*CancelError](err); return ok && c.Reason == "msg" }},
		{kindAbort, func(err error) bool { _, ok := errors.AsType[*AbortError](err); return ok }},
		{kindUnrecoverableSystem, func(err error) bool {
			u, ok := errors.AsType[*UnrecoverableError](err)
			return ok && u.Kind == ErrorKindSystem
		}},
		{kindUnrecoverableUser, func(err error) bool {
			u, ok := errors.AsType[*UnrecoverableError](err)
			return ok && u.Kind == ErrorKindUser
		}},
		{kindHandoff, func(err error) bool { _, ok := errors.AsType[*HandoffError](err); return !ok }},
		{kindError, func(err error) bool { return err.Error() == "msg" }},
		{"", func(err error) bool { return err.Error() == "msg" }},
		{"from-the-future", func(err error) bool { return err.Error() == "msg" }},
	}
	for _, tc := range cases {
		err := outcomeError(tc.kind, "msg")
		if !isHalt(err) {
			t.Errorf("%q: expected a halt, got %T", tc.kind, err)
		}
		if err.Error() != "msg" {
			t.Errorf("%q: expected the message preserved, got %q", tc.kind, err.Error())
		}
		if !tc.want(err) {
			t.Errorf("%q: unexpected rebuilt error %T %v", tc.kind, err, err)
		}
	}
	// The kinds that carry a type round-trip through a rebuild; handoff does not (its target is
	// not rebuilt), and an unknown or empty kind reads as a plain error.
	for _, kind := range []string{kindCanceled, kindAbort, kindUnrecoverableSystem, kindUnrecoverableUser} {
		if got := outcomeKind(outcomeError(kind, "msg")); got != kind {
			t.Errorf("%q: expected the rebuilt error to classify back to its kind, got %q", kind, got)
		}
	}
	if recordedOutcome("", "") != nil {
		t.Fatal("expected an empty record to read as success")
	}
	if c, ok := errors.AsType[*CancelError](recordedOutcome(kindCanceled, "")); !ok || c.Reason != "" {
		t.Fatal("expected a cancel with no reason to read as a cancel, not a success")
	}
}

// TestOutcomeTypedAcrossNodes verifies a run's outcome reads with its type from a node that did
// not run it: a cancel (with and without a reason), an abort and an unrecoverable halt on m1 are
// each rebuilt on m2 from the record alone.
func TestOutcomeTypedAcrossNodes(t *testing.T) {
	b := newObjectBackend(t)
	ctx := context.Background()
	m1, _ := b.newManager(nil)
	m2, _ := b.newManager(nil)
	entered := make(chan struct{}, 2)
	start := haltingFSM(t, m1, "outcome", entered)

	waitOn := func(id, name string) error {
		t.Helper()
		version, err := start(ctx, id, NewRequest(&orderReq{Name: name}, &orderResp{}))
		if err != nil {
			t.Fatalf("failed to start %s: %v", id, err)
		}
		if name == "cancel" {
			<-entered
			if err := m1.Cancel(ctx, version, id); err != nil {
				t.Fatalf("cancel failed: %v", err)
			}
		}
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return m2.Wait(waitCtx, version)
	}

	if c, ok := errors.AsType[*CancelError](waitOn("stop with reason", "cancel")); !ok || c.Reason != "stop with reason" {
		t.Fatal("expected a typed cancel with its reason from the other node")
	}
	if _, ok := errors.AsType[*CancelError](waitOn("", "cancel")); !ok {
		t.Fatal("expected a cancel with no reason to read as a cancel, not a success, from the other node")
	}
	if err := waitOn("outcome-abort", "abort"); !isAbort(err) {
		t.Fatalf("expected a typed abort from the other node, got %T %v", err, err)
	}
	if u, ok := errors.AsType[*UnrecoverableError](waitOn("outcome-user", "user")); !ok || u.Kind != ErrorKindUser || u.Error() != "bad input" {
		t.Fatal("expected a typed unrecoverable user error from the other node")
	}
	if err := waitOn("outcome-ok", "pass"); err != nil {
		t.Fatalf("expected success from the other node, got %v", err)
	}
}

// TestOutcomeTypedAfterRestart verifies the outcome survives the process that ran it: a manager
// on the same storage, with no in-memory record, rebuilds the typed halt from history.
func TestOutcomeTypedAfterRestart(t *testing.T) { runBackends(t, testOutcomeTypedAfterRestart) }

func testOutcomeTypedAfterRestart(t *testing.T, b *backend) {
	ctx := context.Background()
	m1, stop1 := b.newManager(nil)
	entered := make(chan struct{}, 1)
	start := haltingFSM(t, m1, "restart-outcome", entered)

	aborted, err := start(ctx, "restart-abort", NewRequest(&orderReq{Name: "abort"}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	canceled, err := start(ctx, "restart-cancel", NewRequest(&orderReq{Name: "cancel"}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	<-entered
	if err := m1.Cancel(ctx, canceled, ""); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	for _, v := range []ulid.ULID{aborted, canceled} {
		if err := m1.Wait(ctx, v); err == nil {
			t.Fatal("expected the halted run to report its error")
		}
	}
	stop1()

	m2, _ := b.newManager(nil)
	if !isAbort(m2.Wait(ctx, aborted)) {
		t.Fatal("expected a typed abort after restart")
	}
	if c, ok := errors.AsType[*CancelError](m2.Wait(ctx, canceled)); !ok || c.Reason != "" {
		t.Fatal("expected a typed cancel with an empty reason after restart")
	}
}

// TestResumedHaltKeepsItsType verifies the CANCEL event carries the kind: a run halted by an
// abort on a node that then loses its lease mid-finalizer is taken over with its typed halt —
// the taker's finalizer sees an abort rebuilt from the record, and the FINISH it writes reads
// as one. (A shutdown would not do: it ends the finalizer's context, the finalizer returns, and
// the finisher records FINISH; only lease loss leaves a halted, unfinished run for a peer.)
func TestResumedHaltKeepsItsType(t *testing.T) {
	b := newObjectBackendWith(t, asymmetricTimings(400*time.Millisecond))
	ctx := context.Background()
	var holdFinalizer atomic.Bool
	holdFinalizer.Store(true)
	finalizing := make(chan RunErr, 2)

	register := func(m *Manager) Start[orderReq, orderResp] {
		t.Helper()
		start, _, err := m.Register[orderReq, orderResp]("resumed-halt").
			Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				return nil, Abort(errors.New("boom"))
			}).
			End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], runErr RunErr) {
				finalizing <- runErr
				if holdFinalizer.Load() {
					<-ctx.Done() // ends with ErrLeaseLost once the peer has taken the run
				}
			})).
			Build(ctx)
		if err != nil {
			t.Fatalf("failed to build FSM: %v", err)
		}
		return start
	}

	m1, _ := b.newManager(nil)
	start := register(m1)
	version, err := start(ctx, "resumed-halt-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	if !isAbort((<-finalizing).Err) {
		t.Fatal("expected the first finalizer to see the typed abort")
	}

	// The peer's claim loop takes the run once m1's undefended lease lapses; its finalizer must
	// not block, or the run would never finish.
	holdFinalizer.Store(false)
	m2, _ := b.newManager(nil)
	register(m2)
	select {
	case runErr := <-finalizing:
		if !isAbort(runErr.Err) {
			t.Fatalf("expected the taker's finalizer to see the typed abort rebuilt from the record, got %T %v", runErr.Err, runErr.Err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the peer never took over the halted run")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if !isAbort(m2.Wait(waitCtx, version)) {
		t.Fatal("expected the taken-over run's outcome to read as an abort")
	}
}

// TestActiveCarriesTypedHalt verifies the resume path's source on both backends: a run halted by
// an abort and not yet finished (its finalizer is held) is listed by Active with its halt
// rebuilt from the CANCEL event — the typed error a resumer's finalizer and FINISH rely on.
func TestActiveCarriesTypedHalt(t *testing.T) { runBackends(t, testActiveCarriesTypedHalt) }

func testActiveCarriesTypedHalt(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)
	held, release := make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	start, _, err := m.Register[orderReq, orderResp]("active-halt").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return nil, Abort(errors.New("boom"))
		}).
		End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], runErr RunErr) {
			held <- struct{}{}
			<-release
		})).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	version, err := start(ctx, "active-halt-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	<-held

	active, err := m.store.Active(ctx, fsmKey{typeName: "orderReq", action: "active-halt"})
	if err != nil {
		t.Fatalf("Active failed: %v", err)
	}
	if len(active) != 1 || active[0].version != version {
		t.Fatalf("expected the halted run listed as active, got %v", active)
	}
	if !isAbort(active[0].fsmError.Err) || active[0].fsmError.State != "created" {
		t.Fatalf("expected the active run's halt rebuilt as an abort in created, got %T %v in %q", active[0].fsmError.Err, active[0].fsmError.Err, active[0].fsmError.State)
	}
}

// TestCancelDuringRetryIsTheOutcome verifies a cancel that lands while the retry interceptor is
// sleeping between attempts — where the bare context error escapes — is still recorded as the
// cancel, with its reason, not as "context canceled".
func TestCancelDuringRetryIsTheOutcome(t *testing.T) {
	runBackends(t, testCancelDuringRetryIsTheOutcome)
}

func testCancelDuringRetryIsTheOutcome(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)
	attempts := make(chan struct{}, 8)
	start, _, err := m.Register[orderReq, orderResp]("retry-cancel").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			attempts <- struct{}{}
			return nil, errors.New("flaky")
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	version, err := start(ctx, "retry-cancel-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	<-attempts // the first attempt has failed; the retry is sleeping
	if err := m.Cancel(ctx, version, "stop the retries"); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if c, ok := errors.AsType[*CancelError](m.Wait(waitCtx, version)); !ok || c.Reason != "stop the retries" {
		t.Fatal("expected the cancel, with its reason, as the run's outcome")
	}
}
