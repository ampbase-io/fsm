package fsm

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/oklog/ulid/v2"
)

// leaseCoordinator is the coordinate loop's view of a lease-owning backend: everything one
// heartbeat-and-claim pass drives, composed of the narrow roles its helpers each use plus the two
// heartbeat methods. The object backend implements it; BoltDB does not, so the Manager runs no
// coordination loop for BoltDB. It is asserted once in New, and only the loop consumes it — every
// per-run call site and every loop helper depends on the narrow view it actually uses, not this
// union.
type leaseCoordinator interface {
	runClaimer    // claimPass
	fencer        // cancelUnleased
	cancelSweeper // sweepCancellations
	// extendLeases performs one heartbeat pass over every held lease, dropping any found
	// lost; the caller sweeps for executing runs left without a lease.
	extendLeases(ctx context.Context)
	// coordinationIntervals returns the heartbeat and claim cadence for the coordinate loop.
	coordinationIntervals() (heartbeatEvery, claimEvery time.Duration)
}

// The coordinate loop's own capability views follow; the ones consumed elsewhere are declared with
// their consumers (cancelRecorder in manager.go, nodeIdentified in fsm.go). Each consumer depends
// on just the view it uses, so BoltDB, which implements none of them, is excluded structurally,
// without a nil field or a fictional no-op implementation.

// runClaimer is a backend that distributes runs through the claim loop. Asserting it both drives
// resumption (claimRuns) and witnesses "this backend executes via cluster claiming" at the
// execution-placement sites (an unowned Start the loop picks up, rather than local execution).
type runClaimer interface {
	// claimRuns claims every eligible run of the given registered FSMs, each paired with the
	// key of the FSM that will resume it. Keys rather than FSMs: a backend selects runs by
	// resource type and action and never executes one, so it has no use for the fsm itself.
	claimRuns(ctx context.Context, keys []fsmKey) ([]claimedRun, error)
}

// fencer is a backend that fences runs by lease epoch.
type fencer interface {
	// ownedEpoch reports the epoch this node holds the run's lease at, and whether it holds it —
	// the fencing token surfaced to handlers.
	ownedEpoch(version ulid.ULID) (int64, bool)
}

// cancelSweeper is the coordinate loop's cancel-sweep view: find the cancels covering runs this
// node owns and drive each owned-but-idle run to a terminal canceled state.
type cancelSweeper interface {
	// pendingCancellations returns the cancel sentinels covering runs this node owns — keyed by
	// run version, valued by cause — from one keys-only listing of the cancel prefix
	// intersected with the owned set.
	pendingCancellations(ctx context.Context) (map[ulid.ULID]error, error)
	// cancelOwnedRun drives an owned-but-not-executing run to a terminal canceled state.
	cancelOwnedRun(ctx context.Context, version ulid.ULID, cause error) error
}

// claimWakeDelay is the small, jittered pause before an idle worker scans on a pending-event
// wakeup. It spreads the woken herd across a short window; it is deliberately absolute rather
// than a fraction of the claim interval, since it exists to stagger a single broadcast, not to
// track the periodic cadence — and the claim CAS makes any residual overlap safe.
const claimWakeDelay = 50 * time.Millisecond

// claimedRun pairs a claimed resource with the key of the FSM that will resume it. The Manager
// resolves the key through its own registry, so the claim never carries an fsm through the store.
type claimedRun struct {
	key fsmKey

	resource *activeResource
}

// activeScanner reads every incomplete run of one FSM. It is the resume strategy of a backend
// without a claim loop, where all active runs are local by definition; a lease-coordinated
// backend implements runClaimer instead, and never this.
type activeScanner interface {
	Active(ctx context.Context, key fsmKey) ([]*activeResource, error)
}

// resumable returns the runs of f this node should resume. A backend implements exactly one of
// the two resume strategies: a lease-coordinated one hands out only runs this node can claim, so
// a restarting node cannot hijack runs whose owner is live; otherwise every active run is local
// and the whole active set is scanned. Go cannot express "exactly one of these", so a backend
// offering neither is reported rather than silently resuming nothing.
func (m *Manager) resumable(ctx context.Context, f *fsm) ([]*activeResource, error) {
	claimer, ok := m.store.(runClaimer)
	if !ok {
		scanner, ok := m.store.(activeScanner)
		if !ok {
			return nil, fmt.Errorf("%T implements neither resume strategy (runClaimer nor activeScanner)", m.store)
		}
		return scanner.Active(ctx, f.key())
	}

	claimed, err := claimer.claimRuns(ctx, []fsmKey{f.key()})
	if err != nil {
		return nil, err
	}
	resources := make([]*activeResource, 0, len(claimed))
	for _, c := range claimed {
		resources = append(resources, c.resource)
	}
	return resources, nil
}

// coordinate is the lease-coordinated backend's background loop: it heartbeats owned leases,
// cancels local runs whose lease was lost, and periodically claims eligible runs for every
// registered FSM — the claim pass is the object backend's primary work-distribution
// mechanism, not merely failover. A fsm.run.pending event pulls a claim pass forward so an
// idle worker claims immediately instead of on the next periodic tick, which remains the
// correctness floor. It exits when the manager shuts down.
func (m *Manager) coordinate(lc leaseCoordinator) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-m.done
		cancel()
	}()

	heartbeatEvery, claimEvery := lc.coordinationIntervals()
	heartbeat := time.NewTicker(heartbeatEvery)
	defer heartbeat.Stop()
	claim := time.NewTimer(withJitter(claimEvery))
	defer claim.Stop()

	// wake pulls a claim pass forward on a pending event; it stays stopped until then. The
	// jittered claimWakeDelay spreads the woken herd — the claim CAS makes a stampede safe,
	// just wasteful — while the periodic claim stays the floor a dropped event falls back to.
	wake := stoppedTimer()
	defer wake.Stop()
	wakeArmed := false

	pending, unsubscribePending := subscribeSignal(m.bus, subjectPending, m.logger)
	defer unsubscribePending()

	// A cancel is a broadcast: every worker hears it and sweeps, and its owned-intersection —
	// not the subject — decides which node reacts. The heartbeat sweep is the correctness floor;
	// this event just pulls it forward. No jitter: the sweep touches only runs this node owns,
	// so there is no CAS herd to stagger.
	canceled, unsubscribeCancel := subscribeSignal(m.bus, subjectCancel, m.logger)
	defer unsubscribeCancel()

	// runClaim scans for claimable runs unless the manager is shutting down, in which case it
	// reports false so the loop returns without starting a pass it won't wait out.
	runClaim := func() bool {
		select {
		case <-m.done:
			return false
		default:
		}
		m.claimPass(ctx, lc)
		return true
	}

	// armWake pulls the next scan forward, unless one is already armed. The periodic claim timer
	// is left untouched; a redundant scan shortly after is harmless. Both the bus pending event
	// and the local opaque-Start nudge route through it.
	armWake := func() {
		if wakeArmed {
			return
		}
		wakeArmed = true
		wake.Reset(withJitter(claimWakeDelay))
	}

	for {
		select {
		case <-m.done:
			return
		case <-heartbeat.C:
			lc.extendLeases(ctx)
			m.cancelUnleased(lc)
			m.sweepCancellations(ctx, lc)
		case <-claim.C:
			if !runClaim() {
				return
			}
			claim.Reset(withJitter(claimEvery))
		case <-wake.C:
			wakeArmed = false
			if !runClaim() {
				return
			}
		case <-pending:
			armWake()
		case <-m.claimNudge:
			// A local opaque Start persisted an unowned run; claim it without waiting for the
			// periodic tick or a bus round trip.
			armWake()
		case <-canceled:
			m.sweepCancellations(ctx, lc)
		}
	}
}

// sweepCancellations reacts to the cancel sentinels covering this node's runs, driving each to
// cancellation. It is the owner-side of subject-addressed cancel, run on every heartbeat as the
// floor and pulled forward by a cancel broadcast. A run mid-execution is stopped by canceling
// its local context — the run loop then records CANCEL and FINISH with the cause. A run this
// node owns but has not begun executing (pending, delayed, queued) has no context to cancel, so
// it is driven to a terminal canceled manifest directly, or its waiters would poll to their
// deadline.
func (m *Manager) sweepCancellations(ctx context.Context, sweeper cancelSweeper) {
	cancels, err := sweeper.pendingCancellations(ctx)
	if err != nil {
		m.logger.WithError(err).Error("cancel sweep failed")
		return
	}
	for version, cause := range cancels {
		if m.cancelRunning(version, cause) {
			continue
		}
		if err := sweeper.cancelOwnedRun(ctx, version, cause); err != nil {
			m.logger.WithError(err).WithField("run_version", version.String()).Error("failed to cancel owned run")
		}
	}
}

// stoppedTimer returns a timer that will not fire until Reset, with its channel drained.
func stoppedTimer() *time.Timer {
	t := time.NewTimer(0)
	if !t.Stop() {
		<-t.C
	}
	return t
}

// cancelUnleased enforces the invariant that an executing run holds its lease: any locally
// running run whose lease is gone — fenced in Append, stolen at heartbeat, released — is
// canceled with ErrLeaseLost. Runs not yet executing are covered by the same ownership check
// at execute-start; fencing remains the correctness backstop either way, cancellation is a
// latency courtesy. A run in its finish tail (lease already dropped, goroutine not yet
// deregistered) may be swept benignly: its durable writes are done, and Append is
// cancellation-immune regardless.
func (m *Manager) cancelUnleased(f fencer) {
	for _, version := range m.runningVersions() {
		if _, owned := f.ownedEpoch(version); owned {
			continue
		}
		m.logger.WithField("run_version", version.String()).Warn("run lease lost")
		m.cancelRunning(version, ErrLeaseLost)
	}
}

func (m *Manager) runningVersions() []ulid.ULID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Collect(maps.Keys(m.running))
}

// claimPass claims and dispatches eligible runs across every registered FSM.
func (m *Manager) claimPass(ctx context.Context, claimer runClaimer) {
	claimed, err := claimer.claimRuns(ctx, m.registeredKeys())
	if err != nil {
		m.logger.WithError(err).Error("claim pass failed")
		return
	}

	for _, c := range claimed {
		logger := m.logger.WithField("run_version", c.resource.version.String())
		f, ok := m.registeredFSM(c.key)
		if !ok {
			// Registration is append-only, so a key this pass supplied is always present; a miss
			// would mean holding a lease on a run nothing here can resume.
			logger.WithField("action", c.key.action).Error("claimed a run for an unregistered FSM")
			continue
		}
		logger.Info("claimed run")
		// TODO: a run that repeatedly fails to resume is released by ForgetRun and re-claimed
		// by every node's next pass; add per-run claim backoff or dead-lettering.
		if err := f.resumeOne(ctx, c.resource); err != nil {
			logger.WithError(err).Error("failed to resume claimed run")
		}
	}
}

// withJitter spreads periodic work across nodes: d scaled uniformly into [0.75d, 1.25d).
func withJitter(d time.Duration) time.Duration {
	if d < 2 {
		return d
	}
	return 3*d/4 + rand.N(d/2)
}
