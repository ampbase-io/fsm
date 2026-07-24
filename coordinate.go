package fsm

import (
	"context"
	"maps"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/oklog/ulid/v2"
)

// leaseCoordinator is implemented by backends whose runs are owned via leases. The BoltDB
// backend does not implement it; the Manager starts no coordination loop without one.
type leaseCoordinator interface {
	// extendLeases performs one heartbeat pass over every held lease, dropping any found
	// lost; the caller sweeps for executing runs left without a lease.
	extendLeases(ctx context.Context)
	// claimRuns claims every eligible run of the given FSMs, paired with the FSM that will
	// resume each.
	claimRuns(ctx context.Context, fsms []*fsm) ([]claimedRun, error)
	// owns reports whether this node currently holds the run's lease.
	owns(version ulid.ULID) bool
	// coordinationIntervals returns the heartbeat and claim cadence for the coordinate loop.
	coordinationIntervals() (heartbeatEvery, claimEvery time.Duration)
}

// claimWakeDelay is the small, jittered pause before an idle worker scans on a pending-event
// wakeup. It spreads the woken herd across a short window; it is deliberately absolute rather
// than a fraction of the claim interval, since it exists to stagger a single broadcast, not to
// track the periodic cadence — and the claim CAS makes any residual overlap safe.
const claimWakeDelay = 50 * time.Millisecond

// claimedRun pairs a claimed resource with the FSM that will resume it.
type claimedRun struct {
	f *fsm

	resource *activeResource
}

// resumable returns the runs of f this node should resume. A lease-coordinated backend hands
// out only runs this node can claim, so a restarting node cannot hijack runs whose owner is
// live; otherwise every active run is local by definition.
func (m *Manager) resumable(ctx context.Context, f *fsm) ([]*activeResource, error) {
	if m.lc == nil {
		return m.store.Active(ctx, f)
	}

	claimed, err := m.lc.claimRuns(ctx, []*fsm{f})
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

	pending, unsubscribe := subscribeSignal(m.bus, subjectPending, m.logger)
	defer unsubscribe()

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

	for {
		select {
		case <-m.done:
			return
		case <-heartbeat.C:
			lc.extendLeases(ctx)
			m.cancelUnleased(lc)
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
			// Pull the next scan forward, unless one is already armed. The periodic claim timer
			// is left untouched; a redundant scan shortly after is harmless.
			if !wakeArmed {
				wakeArmed = true
				wake.Reset(withJitter(claimWakeDelay))
			}
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
func (m *Manager) cancelUnleased(lc leaseCoordinator) {
	for _, version := range m.runningVersions() {
		if lc.owns(version) {
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
func (m *Manager) claimPass(ctx context.Context, lc leaseCoordinator) {
	claimed, err := lc.claimRuns(ctx, m.registeredFSMs())
	if err != nil {
		m.logger.WithError(err).Error("claim pass failed")
		return
	}

	for _, c := range claimed {
		logger := m.logger.WithField("run_version", c.resource.version.String())
		logger.Info("claimed run")
		// TODO: a run that repeatedly fails to resume is released by ForgetRun and re-claimed
		// by every node's next pass; add per-run claim backoff or dead-lettering.
		if err := c.f.resumeOne(ctx, c.resource); err != nil {
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
