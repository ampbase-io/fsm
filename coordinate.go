package fsm

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/oklog/ulid/v2"
)

// leaseCoordinator is implemented by backends whose runs are owned via leases. The BoltDB
// backend does not implement it; the Manager starts no coordination loop without one.
type leaseCoordinator interface {
	// extendLeases performs one heartbeat pass over every run this node owns and returns the
	// versions whose lease has been lost.
	extendLeases(ctx context.Context) []ulid.ULID
	// claimRuns claims every eligible run of the given FSMs, paired with the FSM that will
	// resume each.
	claimRuns(ctx context.Context, fsms []*fsm) ([]claimedRun, error)
	// owns reports whether this node currently holds the run's lease.
	owns(version ulid.ULID) bool
	// coordinationIntervals returns the heartbeat and claim cadence for the coordinate loop.
	coordinationIntervals() (heartbeatEvery, claimEvery time.Duration)
}

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
// mechanism, not merely failover. It exits when the manager shuts down.
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

	for {
		select {
		case <-m.done:
			return
		case <-heartbeat.C:
			for _, version := range lc.extendLeases(ctx) {
				m.logger.WithField("run_version", version.String()).Warn("run lease lost")
				// A run with no cancelable context yet (delayed or queued dispatch) is covered
				// by the ownership check at execute-start; fencing remains the correctness
				// backstop either way. Cancellation is a latency courtesy.
				m.cancelRunning(version, ErrLeaseLost)
			}
		case <-claim.C:
			// A tick can race shutdown; don't start a claim pass the manager won't wait out.
			select {
			case <-m.done:
				return
			default:
			}
			m.claimPass(ctx, lc)
			claim.Reset(withJitter(claimEvery))
		}
	}
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
