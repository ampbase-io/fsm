package fsm

import (
	"context"
	"errors"
	"maps"
	"sync"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// leaseReleaseTimeout bounds the best-effort lease releases done outside a caller's context
// (shutdown, failed resume).
const leaseReleaseTimeout = 5 * time.Second

// errClaimLost reports that a competitor claimed the run first; the loser simply moves on.
var errClaimLost = errors.New("run claimed by another node")

// checkFence returns ErrLeaseLost unless the manifest still records this node as owner at the
// epoch it claimed with. The epoch is the fencing token: a stale owner whose lease was taken
// over fails this check on every manifest CAS, no matter what its local clock believes.
func (s *objectStore) checkFence(m *fsmv1.RunManifest, epoch int64) error {
	if m.GetOwnerNode() != s.nodeID || m.GetLeaseEpoch() != epoch {
		return ErrLeaseLost
	}
	return nil
}

func (s *objectStore) trackLease(version ulid.ULID, epoch int64) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	s.owned[version] = epoch
}

func (s *objectStore) ownedEpoch(version ulid.ULID) (int64, bool) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	epoch, ok := s.owned[version]
	return epoch, ok
}

func (s *objectStore) dropLease(version ulid.ULID) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	delete(s.owned, version)
}

// noteFence drops the lease and buffers the run for the next heartbeat pass to report, so the
// Manager cancels the local run context even though the fence tripped inside Append.
func (s *objectStore) noteFence(version ulid.ULID) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if _, ok := s.owned[version]; !ok {
		return
	}
	delete(s.owned, version)
	s.fenced = append(s.fenced, version)
}

func (s *objectStore) snapshotOwned() map[ulid.ULID]int64 {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	return maps.Clone(s.owned)
}

// owns reports whether this node currently holds the run's lease.
func (s *objectStore) owns(version ulid.ULID) bool {
	_, ok := s.ownedEpoch(version)
	return ok
}

func (s *objectStore) coordinationIntervals() (heartbeatEvery, claimEvery time.Duration) {
	return s.cfg.heartbeatPeriod(), s.cfg.claimInterval()
}

// forEachOwned runs fn over a snapshot of the owned leases, scanFanout at a time. Both the
// heartbeat and release passes go through it: serially, a node owning many runs could outrun
// the heartbeat interval and expire the very leases it was extending.
func (s *objectStore) forEachOwned(fn func(version ulid.ULID, epoch int64)) {
	var (
		sem = make(chan struct{}, scanFanout)
		wg  sync.WaitGroup
	)
	for version, epoch := range s.snapshotOwned() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			fn(version, epoch)
		}()
	}
	wg.Wait()
}

func (s *objectStore) drainFenced() []ulid.ULID {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	fenced := s.fenced
	s.fenced = nil
	return fenced
}

// extendLeases performs one heartbeat pass: every owned lease has its expiry pushed out under
// the fence. It returns the runs whose lease has been lost — stolen leases discovered here or
// fences tripped by Append since the last pass — for the caller to cancel locally. Transient
// storage errors keep the lease and retry next tick; liveness only degrades to the lease
// timeout.
func (s *objectStore) extendLeases(ctx context.Context) []ulid.ULID {
	var (
		mu   sync.Mutex
		lost = s.drainFenced()
	)
	s.forEachOwned(func(version ulid.ULID, epoch int64) {
		_, err := s.casManifest(ctx, version, func(m *fsmv1.RunManifest) error {
			if err := s.checkFence(m, epoch); err != nil {
				return err
			}
			m.LeaseExpiry = time.Now().Add(s.cfg.leaseTimeout()).UnixMilli()
			return nil
		})
		switch {
		case err == nil:
		case errors.Is(err, ErrLeaseLost), errors.Is(err, ErrFsmNotFound):
			s.dropLease(version)
			mu.Lock()
			lost = append(lost, version)
			mu.Unlock()
		default:
			s.logger.WithError(err).WithField("run_version", version.String()).Error("failed to extend lease")
		}
	})
	return lost
}

// eligible reports whether this node may claim the run: it is non-terminal, not already held
// by this store, and either unowned, left over from a previous incarnation of this node (the
// epoch bump fences the zombie), or past its lease expiry. Expiry uses the local clock as a
// liveness heuristic only; correctness rests on the epoch CAS.
func (s *objectStore) eligible(version ulid.ULID, m *fsmv1.RunManifest, now time.Time) bool {
	if manifestTerminal(m) {
		return false
	}
	if _, tracked := s.ownedEpoch(version); tracked {
		return false
	}
	switch {
	case m.GetOwnerNode() == "":
		return true
	case m.GetOwnerNode() == s.nodeID:
		return true
	default:
		return now.UnixMilli() > m.GetLeaseExpiry()
	}
}

// claimManifest takes ownership of the run via manifest CAS. Eligibility is re-checked against
// the fresh manifest on every attempt, so losing a race to a competitor surfaces as
// errClaimLost rather than a steal.
func (s *objectStore) claimManifest(ctx context.Context, version ulid.ULID) (*fsmv1.RunManifest, error) {
	manifest, err := s.casManifest(ctx, version, func(m *fsmv1.RunManifest) error {
		if !s.eligible(version, m, time.Now()) {
			return errClaimLost
		}
		m.OwnerNode = s.nodeID
		m.LeaseExpiry = time.Now().Add(s.cfg.leaseTimeout()).UnixMilli()
		m.LeaseEpoch++
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.trackLease(version, manifest.GetLeaseEpoch())
	return manifest, nil
}

// claimRuns claims every eligible run of the given FSMs and returns them paired with the FSM
// that will resume each. This is the object backend's work-acquisition path: pending,
// released, and expired-lease runs are all obtained here, whether through the periodic claim
// pass or a caller-invoked Resume. One lock scan per distinct resource type serves every
// action registered on it; failures on individual runs are logged and skipped so one bad
// manifest cannot block the rest.
func (s *objectStore) claimRuns(ctx context.Context, fsms []*fsm) ([]claimedRun, error) {
	byType := map[string]map[string]*fsm{}
	for _, f := range fsms {
		actions, ok := byType[f.typeName]
		if !ok {
			actions = map[string]*fsm{}
			byType[f.typeName] = actions
		}
		actions[f.action] = f
	}

	var claimed []claimedRun
	for typeName, actions := range byType {
		entries, err := s.scanLocks(ctx, s.lockPrefix(typeName))
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			f, ok := actions[e.action]
			if !ok {
				continue
			}
			if !s.eligible(e.version, e.manifest, time.Now()) {
				continue
			}
			manifest, err := s.claimManifest(ctx, e.version)
			switch {
			case errors.Is(err, errClaimLost), errors.Is(err, ErrFsmNotFound):
				continue
			case err != nil:
				s.logger.WithError(err).WithField("run_version", e.version.String()).Error("failed to claim run")
				continue
			}
			claimed = append(claimed, claimedRun{f: f, resource: manifestResource(e.version, manifest)})
		}
	}
	return claimed, nil
}

// releaseLease clears this node's ownership under the fence so peers can claim the run
// immediately. The epoch is left unchanged; a concurrent claimant's new lease fails the fence
// check and is left intact. Best-effort: an unreleased lease just waits out its expiry.
func (s *objectStore) releaseLease(ctx context.Context, version ulid.ULID, epoch int64) {
	s.dropLease(version)
	_, err := s.casManifest(ctx, version, func(m *fsmv1.RunManifest) error {
		if err := s.checkFence(m, epoch); err != nil {
			return err
		}
		m.OwnerNode = ""
		m.LeaseExpiry = 0
		return nil
	})
	if err != nil && !errors.Is(err, ErrLeaseLost) && !errors.Is(err, ErrFsmNotFound) {
		s.logger.WithError(err).WithField("run_version", version.String()).Error("failed to release lease")
	}
}

func (s *objectStore) releaseLeases(ctx context.Context) {
	s.forEachOwned(func(version ulid.ULID, epoch int64) {
		s.releaseLease(ctx, version, epoch)
	})
}
