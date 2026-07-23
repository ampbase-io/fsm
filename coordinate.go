package fsm

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

// leaseCoordinator is implemented by backends whose runs are owned via leases. The BoltDB
// backend does not implement it; the Manager starts no coordination loop without one.
type leaseCoordinator interface {
	// extendLeases performs one heartbeat pass over every run this node owns and returns the
	// versions whose lease has been lost.
	extendLeases(ctx context.Context) []ulid.ULID
	// claimRuns claims every eligible run of f and returns the claimed runs ready for resume.
	claimRuns(ctx context.Context, f *fsm) ([]*activeResource, error)
}

// resumable returns the runs of f this node should resume. A lease-coordinated backend hands
// out only runs this node can claim, so a restarting node cannot hijack runs whose owner is
// live; otherwise every active run is local by definition.
func (m *Manager) resumable(ctx context.Context, f *fsm) ([]*activeResource, error) {
	lc, ok := m.store.(leaseCoordinator)
	if !ok {
		return m.store.Active(ctx, f)
	}
	return lc.claimRuns(ctx, f)
}

// cancelPendingRetries bounds how many heartbeat ticks a lost-lease cancellation is retried
// for a run that has no cancelable context yet (a delayed or queued run registers one only
// when it starts executing). Fencing is the correctness backstop; the cancel is a latency
// courtesy.
const cancelPendingRetries = 3

// coordinate is the lease-coordinated backend's background loop: it heartbeats owned leases,
// cancels local runs whose lease was lost, and periodically claims eligible runs for every
// registered FSM — the claim pass is the object backend's primary work-distribution
// mechanism, not merely failover. It exits when the manager shuts down.
func (m *Manager) coordinate(lc leaseCoordinator, heartbeatEvery, claimEvery time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-m.done
		cancel()
	}()

	heartbeat := time.NewTicker(heartbeatEvery)
	defer heartbeat.Stop()
	claim := time.NewTimer(withJitter(claimEvery))
	defer claim.Stop()

	pending := map[ulid.ULID]int{}
	for {
		select {
		case <-m.done:
			return
		case <-heartbeat.C:
			for _, version := range lc.extendLeases(ctx) {
				m.logger.WithField("run_version", version.String()).Warn("run lease lost")
				pending[version] = 0
			}
			for version, tries := range pending {
				if m.cancelRunning(version, ErrLeaseLost) || tries >= cancelPendingRetries {
					delete(pending, version)
					continue
				}
				pending[version] = tries + 1
			}
		case <-claim.C:
			m.claimPass(ctx, lc)
			claim.Reset(withJitter(claimEvery))
		}
	}
}

// claimPass claims and dispatches eligible runs for every registered FSM.
func (m *Manager) claimPass(ctx context.Context, lc leaseCoordinator) {
	for _, f := range m.registeredFSMs() {
		if f.resumeOne == nil {
			continue
		}
		claimed, err := lc.claimRuns(ctx, f)
		if err != nil {
			m.logger.WithError(err).WithFields(logrus.Fields{
				"run_type":   f.typeName,
				"run_action": f.action,
			}).Error("claim pass failed")
			continue
		}
		for _, resource := range claimed {
			logger := m.logger.WithField("run_version", resource.version.String())
			logger.Info("claimed run")
			if err := f.resumeOne(ctx, resource); err != nil {
				logger.WithError(err).Error("failed to resume claimed run")
			}
		}
	}
}

func (m *Manager) registeredFSMs() []*fsm {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fsms := make([]*fsm, 0, len(m.fsms))
	for _, f := range m.fsms {
		fsms = append(fsms, f)
	}
	return fsms
}

// cancelRunning cancels the run's local context with the given cause, reporting whether a
// running context was found.
func (m *Manager) cancelRunning(version ulid.ULID, cause error) bool {
	m.mu.RLock()
	cancel, ok := m.running[version]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	cancel(cause)
	return true
}

// withJitter spreads periodic work across nodes: d scaled uniformly into [0.75d, 1.25d).
func withJitter(d time.Duration) time.Duration {
	return 3*d/4 + rand.N(d/2)
}
