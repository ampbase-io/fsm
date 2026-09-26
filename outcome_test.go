package fsm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// haltByName is a transition that halts by the request's Name: "cancel" blocks until its
// context ends, "abort" aborts, "system" and "user" fail unrecoverably; anything else passes.
// entered reports each entry into the blocking branch.
func haltByName(entered chan<- struct{}) Transition[orderReq, orderResp] {
	return func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
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
	}
}

// haltingFSM registers a one-transition FSM on haltByName, with any end options.
func haltingFSM(t *testing.T, m *Manager, action string, entered chan<- struct{}, opts ...EndOption[orderReq, orderResp]) Start[orderReq, orderResp] {
	t.Helper()
	start, _, err := m.Register[orderReq, orderResp](action).
		Start("created", haltByName(entered)).
		End("done", opts...).
		Build(context.Background())
	if err != nil {
		t.Fatalf("failed to build %s FSM: %v", action, err)
	}
	return start
}

// runHalting starts a run of a halting FSM under name, cancels it with reason once it blocks,
// and returns its version and the outcome m reports.
func runHalting(t *testing.T, m *Manager, start Start[orderReq, orderResp], entered <-chan struct{}, id, name, reason string) (ulid.ULID, error) {
	t.Helper()
	ctx := context.Background()
	version, err := start(ctx, id, NewRequest(&orderReq{Name: name}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start %s: %v", id, err)
	}
	if name == "cancel" {
		<-entered
		if err := m.Cancel(ctx, version, reason); err != nil {
			t.Fatalf("cancel failed: %v", err)
		}
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return version, m.Wait(waitCtx, version)
}

func wantAbort(t *testing.T, where string, err error) {
	t.Helper()
	if !isA[*AbortError](err) {
		t.Fatalf("%s: expected a typed abort, got %T %v", where, err, err)
	}
}

// TestOutcomeError pins the rebuild of a recorded error from its kind: each kind that carries a
// type classifies back to itself through a halt with the message preserved; handoff, a kind
// this version does not know, and the unspecified kind of an older record rebuild as a plain
// error.
func TestOutcomeError(t *testing.T) {
	cases := []struct{ kind, back fsmv1.HaltKind }{
		{fsmv1.HaltKind_HALT_KIND_CANCELED, fsmv1.HaltKind_HALT_KIND_CANCELED},
		{fsmv1.HaltKind_HALT_KIND_ABORT, fsmv1.HaltKind_HALT_KIND_ABORT},
		{fsmv1.HaltKind_HALT_KIND_UNRECOVERABLE_SYSTEM, fsmv1.HaltKind_HALT_KIND_UNRECOVERABLE_SYSTEM},
		{fsmv1.HaltKind_HALT_KIND_UNRECOVERABLE_USER, fsmv1.HaltKind_HALT_KIND_UNRECOVERABLE_USER},
		{fsmv1.HaltKind_HALT_KIND_HANDOFF, fsmv1.HaltKind_HALT_KIND_ERROR},
		{fsmv1.HaltKind_HALT_KIND_ERROR, fsmv1.HaltKind_HALT_KIND_ERROR},
		{fsmv1.HaltKind_HALT_KIND_UNSPECIFIED, fsmv1.HaltKind_HALT_KIND_ERROR},
		{fsmv1.HaltKind(99), fsmv1.HaltKind_HALT_KIND_ERROR},
	}
	for _, tc := range cases {
		err := outcomeError(tc.kind, "msg")
		if !isHalt(err) || err.Error() != "msg" {
			t.Errorf("%q: expected a halt carrying the message, got %T %v", tc.kind, err, err)
		}
		if got := outcomeKind(err); got != tc.back {
			t.Errorf("%q: expected the rebuilt error to classify as %q, got %q", tc.kind, tc.back, got)
		}
	}

	if err := recordedRunErr(&fsmv1.StateEvent{}).Err; err != nil {
		t.Fatalf("expected an empty record to read as success, got %v", err)
	}
	got := recordedRunErr(&fsmv1.StateEvent{HaltKind: fsmv1.HaltKind_HALT_KIND_CANCELED, ErrorState: "a"})
	if !isA[*CancelError](got.Err) || got.State != "a" {
		t.Fatalf("expected a cancel with no reason to read as a cancel in its state, got %T %v in %q", got.Err, got.Err, got.State)
	}
}

// TestOutcomeTyped verifies a run's outcome reads with its type from a manager that holds no
// in-memory record of it — a cancel with and without a reason, an abort, an unrecoverable halt
// and a success are each rebuilt from storage alone after the process that ran them is gone.
func TestOutcomeTyped(t *testing.T) { runBackends(t, testOutcomeTyped) }

func testOutcomeTyped(t *testing.T, b *backend) {
	cases := []struct {
		id, name, reason string
		check            func(error) bool
	}{
		{"typed-cancel", "cancel", "stop with reason", func(err error) bool {
			c, ok := errors.AsType[*CancelError](err)
			return ok && c.Reason == "stop with reason"
		}},
		{"typed-cancel-silent", "cancel", "", func(err error) bool {
			c, ok := errors.AsType[*CancelError](err)
			return ok && c.Reason == ""
		}},
		{"typed-abort", "abort", "", isA[*AbortError]},
		{"typed-user", "user", "", func(err error) bool {
			u, ok := errors.AsType[*UnrecoverableError](err)
			return ok && u.Kind == ErrorKindUser && u.Error() == "bad input"
		}},
		{"typed-ok", "pass", "", func(err error) bool { return err == nil }},
	}

	m1, stop1 := b.newManager(nil)
	entered := make(chan struct{}, 1)
	start := haltingFSM(t, m1, "typed", entered)
	versions := make([]ulid.ULID, len(cases))
	for i, tc := range cases {
		version, err := runHalting(t, m1, start, entered, tc.id, tc.name, tc.reason)
		if !tc.check(err) {
			t.Fatalf("%s: unexpected outcome in the process that ran it: %T %v", tc.id, err, err)
		}
		versions[i] = version
	}
	stop1()

	m2, _ := b.newManager(nil)
	for i, tc := range cases {
		if err := m2.Wait(context.Background(), versions[i]); !tc.check(err) {
			t.Fatalf("%s: unexpected outcome rebuilt from storage: %T %v", tc.id, err, err)
		}
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
	finalizer := WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], runErr RunErr) {
		finalizing <- runErr
		if holdFinalizer.Load() {
			<-ctx.Done() // ends with ErrLeaseLost once the peer has taken the run
		}
	})

	m1, _ := b.newManager(nil)
	start := haltingFSM(t, m1, "resumed-halt", nil, finalizer)
	version, err := start(ctx, "resumed-halt-1", NewRequest(&orderReq{Name: "abort"}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start: %v", err)
	}
	wantAbort(t, "the first finalizer", within(t, finalizing, 10*time.Second, "the first finalizer").Err)

	// The peer's claim loop takes the run once m1's undefended lease lapses; its finalizer must
	// not block, or the run would never finish.
	holdFinalizer.Store(false)
	m2, _ := b.newManager(nil)
	haltingFSM(t, m2, "resumed-halt", nil, finalizer)
	wantAbort(t, "the taker's finalizer", within(t, finalizing, 15*time.Second, "the peer to take over the halted run").Err)
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	wantAbort(t, "the taken-over run's outcome", m2.Wait(waitCtx, version))
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
	start := haltingFSM(t, m, "active-halt", nil, WithFinalizers(func(context.Context, *Request[orderReq, orderResp], RunErr) {
		held <- struct{}{}
		<-release
	}))
	version, err := start(ctx, "active-halt-1", NewRequest(&orderReq{Name: "abort"}, &orderResp{}))
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
	wantAbort(t, "the active run's halt", active[0].fsmError.Err)
	if active[0].fsmError.State != "created" {
		t.Fatalf("expected the halt recorded in created, got %q", active[0].fsmError.State)
	}
}

// TestCancelDuringRetryIsTheOutcome verifies a cancel that lands while the retry interceptor is
// sleeping between attempts — where RetryNotify returns its own bare context error — is still
// the transition's and the run's outcome: recorded as canceled with its reason, not as
// "context canceled".
func TestCancelDuringRetryIsTheOutcome(t *testing.T) {
	runBackends(t, testCancelDuringRetryIsTheOutcome)
}

func testCancelDuringRetryIsTheOutcome(t *testing.T, b *backend) {
	ctx := context.Background()
	reader := metricsReader(b)
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
	if got := counterValue(t, collect(t, reader), "fsm.transition.completed", attrStatus, "canceled"); got != 1 {
		t.Fatalf("expected the transition recorded as canceled once, got %d", got)
	}
}
