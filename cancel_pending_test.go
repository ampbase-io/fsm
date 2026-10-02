package fsm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"connectrpc.com/connect"
	"github.com/oklog/ulid/v2"
)

// mustCancelError fails the test unless err is a *CancelError carrying reason.
func mustCancelError(t *testing.T, err error, reason, what string) {
	t.Helper()
	cerr, ok := errors.AsType[*CancelError](err)
	if !ok {
		t.Fatalf("%s: expected *CancelError, got %v", what, err)
	}
	if cerr.Reason != reason {
		t.Fatalf("%s: expected reason %q, got %q", what, reason, cerr.Reason)
	}
}

// TestCancelPendingFreesTheResource verifies the operator's escape hatch on a run stuck behind a
// queue with no capacity: cancelling it settles the run where it stands — waiters get the cancel,
// the body never runs — and its id is free for a fresh start at once.
func TestCancelPendingFreesTheResource(t *testing.T) {
	runBackends(t, testCancelPendingFreesTheResource)
}

func testCancelPendingFreesTheResource(t *testing.T, b *backend) {
	ctx := context.Background()
	// Capacity 0 admits nothing, so the run stays pending for as long as the test needs.
	m, _ := b.newManager(map[string]int{"stuck": 0})

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	start := blockingFSM(t, m, "stuck-run", entered, release)

	version, err := startExclusive(start, "stuck-1", "stuck")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if !noSignal(entered) {
		t.Fatal("expected the run to stay pending on a queue with no capacity")
	}

	if err := m.Cancel(ctx, version, "operator unstuck the org"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	mustCancelError(t, waitFor(m, version), "operator unstuck the org", "waiting on the canceled run")

	// The run held its id exclusively; settling it released the resource, so a fresh start wins it
	// rather than being refused by the pending run's lock.
	if _, err := startExclusive(start, "stuck-1", "stuck"); err != nil {
		t.Fatalf("a start after the pending run was canceled: %v", err)
	}
	if !noSignal(entered) {
		t.Fatal("neither run should have executed: the queue still has no capacity")
	}
}

// TestCancelPendingRunNeverExecutes verifies a settled run stays settled once its queue frees: the
// slot it was waiting for goes to somebody else, and the dispatch that reaches it is refused rather
// than running a canceled run's transitions.
func TestCancelPendingRunNeverExecutes(t *testing.T) {
	runBackends(t, testCancelPendingRunNeverExecutes)
}

func testCancelPendingRunNeverExecutes(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(map[string]int{"one": 1})

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	start := blockingFSM(t, m, "one-at-a-time", entered, release)

	holder, err := start(ctx, "holder", NewRequest(&orderReq{}, &orderResp{}), WithQueue("one"))
	if err != nil {
		t.Fatalf("start the holder: %v", err)
	}
	within(t, entered, 10*time.Second, "the holder to take the only slot")

	waiting, err := start(ctx, "waiting", NewRequest(&orderReq{}, &orderResp{}), WithQueue("one"))
	if err != nil {
		t.Fatalf("start the waiting run: %v", err)
	}
	if err := m.Cancel(ctx, waiting, "not needed after all"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	mustCancelError(t, waitFor(m, waiting), "not needed after all", "waiting on the canceled run")

	// Freeing the slot is what would dispatch the canceled run, on the runner it is still queued on
	// or on the next claim pass.
	close(release)
	waitRun(t, m, holder)
	if !noSignal(entered) {
		t.Fatal("the canceled run executed after its queue freed")
	}
}

// TestCancelPendingRecordsTheSameOutcome verifies a run canceled before it started is recorded
// like any other canceled run: a FINISH event carrying the cancel, so History and a Wait from
// another node report the outcome rather than a missing or successful run.
func TestCancelPendingRecordsTheSameOutcome(t *testing.T) {
	runBackends(t, testCancelPendingRecordsTheSameOutcome)
}

func testCancelPendingRecordsTheSameOutcome(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(map[string]int{"stuck": 0})

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	start := blockingFSM(t, m, "stuck-history", entered, release)

	version, err := start(ctx, "hist-1", NewRequest(&orderReq{}, &orderResp{}), WithQueue("stuck"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := m.Cancel(ctx, version, "never ran"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	mustCancelError(t, waitFor(m, version), "never ran", "waiting on the canceled run")

	he, err := m.store.History(ctx, version)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	last := he.GetLastEvent()
	if last.GetType() != fsmv1.EventType_EVENT_TYPE_FINISH {
		t.Fatalf("expected the history to end in FINISH, got %v", last.GetType())
	}
	if last.GetHaltKind() != fsmv1.HaltKind_HALT_KIND_CANCELED {
		t.Fatalf("expected a canceled halt on the FINISH event, got %v", last.GetHaltKind())
	}
	if last.GetErrorState() != cancelBeforeExecState {
		t.Fatalf("expected the FINISH to record %q as where the run stopped, got %q", cancelBeforeExecState, last.GetErrorState())
	}

	// The control API reports the same classification, so a consumer waiting over RPC need not
	// read the message text to tell a cancel from a failure.
	admin := &adminServer{m: m}
	resp, err := admin.Wait(ctx, connect.NewRequest(&fsmv1.WaitRequest{Version: version.String()}))
	if err != nil {
		t.Fatalf("admin wait: %v", err)
	}
	if resp.Msg.GetHaltKind() != fsmv1.HaltKind_HALT_KIND_CANCELED {
		t.Fatalf("expected the RPC wait to report a canceled halt, got %v", resp.Msg.GetHaltKind())
	}
	if resp.Msg.GetErrorState() != cancelBeforeExecState {
		t.Fatalf("expected the RPC wait to report %q, got %q", cancelBeforeExecState, resp.Msg.GetErrorState())
	}
}

// TestCancelPendingLeavesAdmittedRunToItsContext verifies the settle path does not reach past
// pending runs: a run already executing is canceled through its context, as before, so its
// finalizers run and its outcome is recorded by the run itself.
func TestCancelPendingLeavesAdmittedRunToItsContext(t *testing.T) {
	runBackends(t, testCancelPendingLeavesAdmittedRunToItsContext)
}

func testCancelPendingLeavesAdmittedRunToItsContext(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(map[string]int{"open": 1})

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)

	finalized := make(chan struct{}, 1)
	start := blockingFSM(t, m, "admitted", entered, release,
		WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], err RunErr) {
			finalized <- struct{}{}
		}))

	version, err := start(ctx, "admitted-1", NewRequest(&orderReq{}, &orderResp{}), WithQueue("open"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	within(t, entered, 10*time.Second, "the admitted run to start")

	if err := m.Cancel(ctx, version, "stop the running one"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	mustCancelError(t, waitFor(m, version), "stop the running one", "waiting on the canceled run")
	within(t, finalized, 10*time.Second, "the running run's finalizer")
}

// TestCancelPendingUnknownRun verifies an unknown or already-finished run is still reported as
// such: settling a pending run must not turn Cancel into a silent success.
func TestCancelPendingUnknownRun(t *testing.T) { runBackends(t, testCancelPendingUnknownRun) }

// startedFSM registers a run whose first transition completes and whose second waits for its
// context to end, reporting each transition's entry and the outcome its finalizer sees.
func startedFSM(t *testing.T, m *Manager, action string, first, second chan<- struct{}, finalized chan<- RunErr) (Start[orderReq, orderResp], Resume) {
	t.Helper()

	start, resume, err := m.Register[orderReq, orderResp](action).
		Start("first", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			first <- struct{}{}
			return nil, nil
		}).
		To("second", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			second <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}).
		End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], err RunErr) {
			finalized <- err
		})).
		Build(context.Background())
	if err != nil {
		t.Fatalf("failed to build %s FSM: %v", action, err)
	}
	return start, resume
}

// TestCancelLeavesExpiredStartedRunToTakeover verifies a cancel landing after a running run's
// owner lost its lease, but before a peer took it over, does not settle the run: the peer takes it
// over, the cancel reaches it through its context, and its finalizers run.
func TestCancelLeavesExpiredStartedRunToTakeover(t *testing.T) {
	ctx := context.Background()
	nodes := 0
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		nodes++
		cfg.LeaseTimeout = 150 * time.Millisecond
		if nodes == 3 {
			// The peer, which takes the run over.
			cfg.HeartbeatPeriod = 75 * time.Millisecond
			cfg.ClaimInterval = 75 * time.Millisecond
			return
		}
		// The owner, whose lease lapses, and the node the cancel lands on: neither extends nor claims.
		cfg.HeartbeatPeriod = time.Hour
		cfg.ClaimInterval = time.Hour
	})

	owner, _ := b.newManager(nil)
	ownerFirst := make(chan struct{}, 1)
	start, _ := startedFSM(t, owner, "taken-over", ownerFirst, make(chan struct{}, 1), make(chan RunErr, 1))
	version, err := start(ctx, "taken-over-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	within(t, ownerFirst, 10*time.Second, "the first transition on the owner")

	canceller, _ := b.newManager(nil)
	s, ok := canceller.store.(*objectStore)
	if !ok {
		t.Fatalf("expected objectStore, got %T", canceller.store)
	}
	eventually(t, 5*time.Second, func() bool {
		return s.claimable(mustManifest(t, s, version), time.Now())
	}, "the owner's lease to expire")

	if err := canceller.Cancel(ctx, version, "stop it"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if manifestTerminal(mustManifest(t, s, version)) {
		t.Fatal("the cancel settled a run that had started, skipping its finalizers")
	}

	peer, _ := b.newManager(nil)
	peerFirst := make(chan struct{}, 1)
	peerFinalized := make(chan RunErr, 1)
	startedFSM(t, peer, "taken-over", peerFirst, make(chan struct{}, 1), peerFinalized)

	outcome := within(t, peerFinalized, 10*time.Second, "the takeover's finalizers")
	mustCancelError(t, outcome.Err, "stop it", "the outcome the finalizer saw")
	mustCancelError(t, waitFor(canceller, version), "stop it", "waiting on the run")
	if !noSignal(peerFirst) {
		t.Fatal("the takeover re-ran a transition the owner had completed")
	}
}

// TestCancelLeavesResumedStartedRunToItsContext verifies BoltDB's counterpart: a run that had
// started before a restart, and is resumed behind a queue with no free slot, is not settled by a
// cancel. It is left to its own context, so once it runs again its finalizers run.
func TestCancelLeavesResumedStartedRunToItsContext(t *testing.T) {
	ctx := context.Background()
	b := newBoltBackend(t)

	before, stopBefore := b.newManager(map[string]int{"q": 1})
	reached := make(chan struct{}, 1)
	start, _ := startedFSM(t, before, "resumed", make(chan struct{}, 1), reached, make(chan RunErr, 1))
	version, err := start(ctx, "resumed-1", NewRequest(&orderReq{}, &orderResp{}), WithQueue("q"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// Reaching the second transition means the first's COMPLETE is recorded: the run has started.
	within(t, reached, 10*time.Second, "the second transition")
	stopBefore()

	// Restarted with no capacity, the run is resumed but waits for a slot.
	held, stopHeld := b.newManager(map[string]int{"q": 0})
	_, resumeHeld := startedFSM(t, held, "resumed", make(chan struct{}, 1), make(chan struct{}, 1), make(chan RunErr, 1))
	if err := resumeHeld(ctx); err != nil {
		t.Fatalf("resume: %v", err)
	}
	switch err := held.Cancel(ctx, version, "stop it"); {
	case err == nil, errors.Is(err, ErrFsmNotFound):
	default:
		t.Fatalf("cancel: %v", err)
	}
	pendingRun(t, held, version) // still active: not settled
	stopHeld()

	// With a slot it runs again, so a cancel reaches it through its context.
	after, _ := b.newManager(map[string]int{"q": 1})
	second := make(chan struct{}, 1)
	finalized := make(chan RunErr, 1)
	_, resumeAfter := startedFSM(t, after, "resumed", make(chan struct{}, 1), second, finalized)
	if err := resumeAfter(ctx); err != nil {
		t.Fatalf("resume: %v", err)
	}
	within(t, second, 10*time.Second, "the resumed run's second transition")
	if err := after.Cancel(ctx, version, "stop it"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	outcome := within(t, finalized, 10*time.Second, "the resumed run's finalizers")
	mustCancelError(t, outcome.Err, "stop it", "the outcome the finalizer saw")
}

func testCancelPendingUnknownRun(t *testing.T, b *backend) {
	m, _ := b.newManager(nil)
	if err := m.Cancel(context.Background(), ulid.Make(), "nobody home"); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected ErrFsmNotFound, got %v", err)
	}
}

// race runs a and b concurrently, released together, and returns their errors in that order.
func race(a, b func() error) (error, error) {
	var (
		wg       sync.WaitGroup
		aErr     = make(chan error, 1)
		bErr     = make(chan error, 1)
		announce = make(chan struct{})
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-announce
		aErr <- a()
	}()
	go func() {
		defer wg.Done()
		<-announce
		bErr <- b()
	}()
	close(announce)
	wg.Wait()
	return <-aErr, <-bErr
}

// TestCancelPendingRacesTheClaim verifies the object backend's arbiter: a settle on one node takes
// the run through the same lease claim a claim pass on another takes, so exactly one of them wins
// it. If both did, a canceled run would execute anyway, its FINISH already written.
func TestCancelPendingRacesTheClaim(t *testing.T) {
	ctx := context.Background()
	h := newLeaseHarness(t)
	canceller := h.store("node-a", 10*time.Second)
	claimer := h.store("node-b", 10*time.Second)

	for i := range 25 {
		run := pendingObjectRun(t, canceller, fmt.Sprintf("race-%d", i))

		settleErr, claimErr := race(
			func() error { return canceller.cancelPending(ctx, run.StartVersion, &CancelError{Reason: "racing"}) },
			func() error {
				_, err := claimer.claimManifest(ctx, run.StartVersion)
				return err
			})
		if settleErr != nil {
			t.Fatalf("run %d: settling a pending run failed: %v", i, settleErr)
		}

		// The claim reports its own loss; the settle's win is the terminal manifest it leaves.
		switch settled := manifestTerminal(mustManifest(t, canceller, run.StartVersion)); {
		case settled && claimErr == nil:
			t.Fatalf("run %d: the cancel and the claim both won", i)
		case !settled && claimErr != nil:
			t.Fatalf("run %d: neither the cancel nor the claim won (claim: %v)", i, claimErr)
		}
	}
}

// pendingObjectRun persists an unowned run, as the ingress does for a queued start: no node has
// claimed it, so a cancel and a claim pass are both free to take it.
func pendingObjectRun(t *testing.T, s *objectStore, id string) Run {
	t.Helper()

	run := Run{ID: id, StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq", Queue: "race"}
	_, err := s.Start(context.Background(), run, &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_START,
		Id:           run.ID,
		ResourceType: run.TypeName,
		Action:       run.Action,
		State:        "created",
	}, &startRecord{Resource: []byte("{}"), Transitions: []string{"created", "done"}, Unowned: true})
	if err != nil {
		t.Fatalf("failed to persist a pending run: %v", err)
	}
	return run
}

// TestCancelPendingRacesTheDispatch verifies the BoltDB arbiter: the settle takes the run out of
// PENDING, which is what SetRunning refuses to dispatch without, so exactly one of them wins it.
func TestCancelPendingRacesTheDispatch(t *testing.T) {
	ctx := context.Background()
	m, _ := newBoltBackend(t).newManager(map[string]int{"race": 0})

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	start := blockingFSM(t, m, "race-dispatch", entered, release)

	for i := range 25 {
		version, err := startExclusive(start, fmt.Sprintf("race-%d", i), "race")
		if err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
		run := pendingRun(t, m, version)

		settleErr, startErr := race(
			func() error { return m.store.cancelPending(ctx, version, &CancelError{Reason: "racing"}) },
			func() error { return m.store.SetRunning(ctx, run) })
		switch {
		case settleErr == nil && startErr == nil:
			t.Fatalf("run %d: the cancel and the dispatch both won", i)
		case settleErr != nil && startErr != nil:
			t.Fatalf("run %d: neither won: settle %v, dispatch %v", i, settleErr, startErr)
		}
	}
}

// TestSetRunningRefusesSettledRun verifies what the BoltDB arbiter leaves behind: the dispatch that
// eventually reaches a settled run is refused rather than executing a canceled run, and a second
// settle finds nothing left to take.
func TestSetRunningRefusesSettledRun(t *testing.T) {
	ctx := context.Background()
	m, _ := newBoltBackend(t).newManager(map[string]int{"stuck": 0})

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	start := blockingFSM(t, m, "settled", entered, release)

	version, err := startExclusive(start, "settled-1", "stuck")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	run := pendingRun(t, m, version)

	if err := m.store.cancelPending(ctx, version, &CancelError{Reason: "settled"}); err != nil {
		t.Fatalf("cancelPending: %v", err)
	}
	if err := m.store.SetRunning(ctx, run); !errors.Is(err, errRunSettled) {
		t.Fatalf("expected errRunSettled dispatching a settled run, got %v", err)
	}
	if err := m.store.cancelPending(ctx, version, &CancelError{Reason: "again"}); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected ErrFsmNotFound settling an already-settled run, got %v", err)
	}
}

// pendingRun returns the Run the backend recorded for a version still waiting to start, as the run
// loop holds it when it reaches SetRunning.
func pendingRun(t *testing.T, m *Manager, version ulid.ULID) Run {
	t.Helper()
	active, err := m.store.ListActive(context.Background())
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	for _, rs := range active {
		if rs.StartVersion == version {
			return rs.Run
		}
	}
	t.Fatalf("no active run recorded for %s", version)
	return Run{}
}

// TestResumeScanLeavesSettledRunSettled verifies the one window between a settle's two writes: a
// resume scan that read the run's still-live record must not seed it pending again, which would
// undo the settle and let the resume run a canceled run.
func TestResumeScanLeavesSettledRunSettled(t *testing.T) {
	ctx := context.Background()
	m, _ := newBoltBackend(t).newManager(map[string]int{"stuck": 0})
	s, ok := m.store.(*boltStore)
	if !ok {
		t.Fatalf("expected boltStore, got %T", m.store)
	}

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	start := blockingFSM(t, m, "scanned", entered, release)

	version, err := startExclusive(start, "scanned-1", "stuck")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	run := pendingRun(t, m, version)

	// Take the run but stop short of the FINISH that deletes its record, which is what a scan would
	// otherwise still find live.
	outcome := RunErr{Err: halt(&CancelError{Reason: "settled"}), State: cancelBeforeExecState}
	if _, err := s.takePending(version, outcome); err != nil {
		t.Fatalf("takePending: %v", err)
	}
	if _, err := s.Active(ctx, fsmKey{typeName: run.TypeName, action: run.Action}); err != nil {
		t.Fatalf("active: %v", err)
	}
	if err := s.SetRunning(ctx, run); !errors.Is(err, errRunSettled) {
		t.Fatalf("expected the resume scan to leave the run settled, got %v", err)
	}
}

// TestCancelReachesExecutingRunWhateverItsRow verifies a cancel of a run this process is executing
// goes through its context, even when BoltDB's row says it never started — as it does after a
// SetRunning failure the run continues past. The settle only ever sees a run that is not executing,
// so an executing run's finalizers run.
func TestCancelReachesExecutingRunWhateverItsRow(t *testing.T) {
	ctx := context.Background()
	m, _ := newBoltBackend(t).newManager(nil)
	s, ok := m.store.(*boltStore)
	if !ok {
		t.Fatalf("expected boltStore, got %T", m.store)
	}

	executing := make(chan struct{}, 1)
	finalized := make(chan RunErr, 1)
	start, _ := startedFSM(t, m, "unrecorded-row", make(chan struct{}, 1), executing, finalized)
	version, err := start(ctx, "unrecorded-row-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	within(t, executing, 10*time.Second, "the run's second transition")

	txn := s.memDB.Txn(true)
	rs, err := runRow(txn, version)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	rs.State = fsmv1.RunState_RUN_STATE_PENDING
	if err := txn.Insert(fsmTable, rs); err != nil {
		t.Fatalf("rewrite row: %v", err)
	}
	txn.Commit()

	if err := m.Cancel(ctx, version, "stop it"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	outcome := within(t, finalized, 10*time.Second, "the executing run's finalizers")
	mustCancelError(t, outcome.Err, "stop it", "the outcome the finalizer saw")
	mustCancelError(t, waitFor(m, version), "stop it", "waiting on the run")
}
