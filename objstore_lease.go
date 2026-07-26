package fsm

import (
	"context"
	"errors"
	"sync"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// leaseReleaseTimeout bounds the best-effort lease releases done outside a caller's context
// (shutdown, failed resume).
const leaseReleaseTimeout = 5 * time.Second

// errClaimLost reports that a competitor claimed the run first; the loser simply moves on.
var errClaimLost = errors.New("run claimed by another node")

// leaseState is this node's relationship to a run's lease.
type leaseState int

const (
	leaseClaiming leaseState = iota // claim CAS in flight; reserved against same-store claimers
	leaseHeld                       // lease held at epoch; extended by the heartbeat
)

type lease struct {
	state leaseState

	epoch int64

	// queue is the admission queue the run occupies a slot in, or "" for a run not gated by a
	// queue's cluster-wide capacity. It lets the heartbeat refresh (heartbeatQueues) and the
	// release paths find the run's queue roster after the fact.
	queue string
}

// checkFence returns ErrLeaseLost unless the manifest still records this node as owner at the
// epoch it claimed with. The epoch is the fencing token: a stale owner whose lease was taken
// over fails this check on every manifest CAS, no matter what its local clock believes.
func (s *objectStore) checkFence(m *fsmv1.RunManifest, epoch int64) error {
	if m.GetOwnerNode() != s.node || m.GetLeaseEpoch() != epoch {
		return ErrLeaseLost
	}
	return nil
}

func (s *objectStore) trackLease(version ulid.ULID, epoch int64, queue string) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	s.leases[version] = lease{state: leaseHeld, epoch: epoch, queue: queue}
}

// leaseQueue returns the admission queue this node holds the run's lease under, or "" if the run
// is not queue-gated or this node no longer holds the lease.
func (s *objectStore) leaseQueue(version ulid.ULID) string {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	return s.leases[version].queue
}

// ownedByQueue groups the run versions this node holds admitted-queue leases on by queue name, for
// the piggybacked queue-roster heartbeat.
func (s *objectStore) ownedByQueue() map[string][]ulid.ULID {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	byQueue := map[string][]ulid.ULID{}
	for version, l := range s.leases {
		if l.state != leaseHeld || l.queue == "" {
			continue
		}
		byQueue[l.queue] = append(byQueue[l.queue], version)
	}
	return byQueue
}

func (s *objectStore) ownedEpoch(version ulid.ULID) (int64, bool) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	l, ok := s.leases[version]
	if !ok || l.state != leaseHeld {
		return 0, false
	}
	return l.epoch, true
}

func (s *objectStore) dropLease(version ulid.ULID) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	delete(s.leases, version)
}

// snapshotOwned returns the held leases and their epochs; mid-claim entries are excluded.
func (s *objectStore) snapshotOwned() map[ulid.ULID]int64 {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	owned := make(map[ulid.ULID]int64, len(s.leases))
	for version, l := range s.leases {
		if l.state != leaseHeld {
			continue
		}
		owned[version] = l.epoch
	}
	return owned
}

// owns reports whether this node currently holds the run's lease.
func (s *objectStore) owns(version ulid.ULID) bool {
	_, ok := s.ownedEpoch(version)
	return ok
}

func (s *objectStore) coordinationIntervals() (heartbeatEvery, claimEvery time.Duration) {
	return s.cfg.heartbeatPeriod(), s.cfg.claimInterval()
}

// forEachOwned runs fn over a snapshot of the owned leases, scanFanout at a time — the sem's
// buffer is the bound on in-flight object storage operations. Both the heartbeat and release
// passes go through it: serially, a node owning many runs could outrun the heartbeat interval
// and expire the very leases it was extending.
func (s *objectStore) forEachOwned(fn func(version ulid.ULID, epoch int64)) {
	var (
		sem = make(chan struct{}, scanFanout)
		wg  sync.WaitGroup
	)
	for version, epoch := range s.snapshotOwned() {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			fn(version, epoch)
		}()
	}
	wg.Wait()
}

// extendLeases performs one heartbeat pass: every held lease has its expiry pushed out under
// the fence, and a lease found lost (stolen, or its run's manifest gone) is dropped from the
// local set — the coordinate loop's sweep then cancels the run it belonged to. Transient
// storage errors keep the lease and retry next tick; liveness only degrades to the lease
// timeout.
func (s *objectStore) extendLeases(ctx context.Context) {
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
			leaseRenewalsVec.WithLabelValues("extended").Inc()
		case errors.Is(err, ErrLeaseLost), errors.Is(err, ErrFsmNotFound):
			leaseRenewalsVec.WithLabelValues("lost").Inc()
			s.dropLease(version)
		case errors.Is(err, context.Canceled):
			// Shutdown mid-pass; Close releases the leases.
		default:
			s.logger.WithError(err).WithField("run_version", version.String()).Error("failed to extend lease")
		}
	})

	// Refresh this node's queue-roster entries after lost leases have been dropped above, so a
	// slot this node is losing is not re-warmed; entries past the timeout are reclaimed here too.
	s.heartbeatQueues(ctx)
}

// claimable reports the manifest-side claim conditions: the run is non-terminal and either
// unowned, left over from a previous incarnation of this node (the epoch bump fences the
// zombie), or past its lease expiry. Expiry uses the local clock as a liveness heuristic
// only; correctness rests on the epoch CAS. Takeover is immediate at expiry — no grace
// period; the epoch fences the stale owner (see the RFC's no-grace-period trade-off note).
func (s *objectStore) claimable(m *fsmv1.RunManifest, now time.Time) bool {
	if manifestTerminal(m) {
		return false
	}
	switch {
	case m.GetOwnerNode() == "":
		return true
	case m.GetOwnerNode() == s.node:
		return true
	default:
		return now.UnixMilli() > m.GetLeaseExpiry()
	}
}

// reserveClaim serializes same-store claimers on a run: only one goroutine may be mid-claim,
// and a run already held or mid-claim is never re-claimed. Without it, a caller-invoked
// Resume racing the claim tick could claim the run twice (the winner's lease is recorded only
// after its CAS returns), dispatching it twice.
func (s *objectStore) reserveClaim(version ulid.ULID) bool {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if _, ok := s.leases[version]; ok {
		return false
	}
	s.leases[version] = lease{state: leaseClaiming}
	return true
}

// claimManifest reserves the run against same-store competitors then takes ownership via manifest
// CAS — the non-queued claim path. A queued run reserves and claims through claimQueued, which
// interposes queue admission between the reservation and the manifest CAS.
func (s *objectStore) claimManifest(ctx context.Context, version ulid.ULID) (*fsmv1.RunManifest, error) {
	if !s.reserveClaim(version) {
		return nil, errClaimLost
	}
	return s.claimReserved(ctx, version, "")
}

// claimReserved takes ownership of an already-reserved run via manifest CAS, tracking the lease
// (under queue, so the roster heartbeat can find it) on success and dropping the reservation on
// failure. Claimability is re-checked against the fresh manifest on every CAS attempt, so losing a
// race to another node surfaces as errClaimLost rather than a steal. The caller must already hold
// the reservation (reserveClaim); this never reserves, so it composes with the queued path.
func (s *objectStore) claimReserved(ctx context.Context, version ulid.ULID, queue string) (*fsmv1.RunManifest, error) {
	manifest, err := s.casManifest(ctx, version, func(m *fsmv1.RunManifest) error {
		if !s.claimable(m, time.Now()) {
			return errClaimLost
		}
		m.OwnerNode = s.node
		m.LeaseExpiry = time.Now().Add(s.cfg.leaseTimeout()).UnixMilli()
		m.LeaseEpoch++
		return nil
	})
	if err != nil {
		if errors.Is(err, errClaimLost) {
			leaseRenewalsVec.WithLabelValues("claim_lost").Inc()
		}
		s.dropLease(version)
		return nil, err
	}
	leaseRenewalsVec.WithLabelValues("claimed").Inc()
	s.trackLease(version, manifest.GetLeaseEpoch(), queue)
	return manifest, nil
}

// claimRuns claims every eligible run of the given FSMs and returns them paired with the FSM
// that will resume each. This is the object backend's work-acquisition path: pending,
// released, and expired-lease runs are all obtained here, whether through the periodic claim
// pass or a caller-invoked Resume. One lock scan per distinct resource type serves every
// action registered on it; failures on individual runs are logged and skipped so one bad
// manifest cannot block the rest.
func (s *objectStore) claimRuns(ctx context.Context, fsms []*fsm) ([]claimedRun, error) {
	var claimed []claimedRun
	for typeName, actions := range fsmsByType(fsms) {
		entries, err := s.scanLocks(ctx, s.lockPrefix(typeName))
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			f, ok := actions[e.action]
			if !ok {
				continue
			}
			run, won := s.claimEntry(ctx, f, e)
			if !won {
				continue
			}
			claimed = append(claimed, run)
		}
	}
	return claimed, nil
}

// fsmsByType groups the FSMs by resource type, then action, so one lock scan per distinct
// type serves every action registered on it.
func fsmsByType(fsms []*fsm) map[string]map[string]*fsm {
	byType := map[string]map[string]*fsm{}
	for _, f := range fsms {
		actions, ok := byType[f.typeName]
		if !ok {
			actions = map[string]*fsm{}
			byType[f.typeName] = actions
		}
		actions[f.action] = f
	}
	return byType
}

// claimEntry attempts to claim one scanned run for f, reporting whether it was won. A run
// this store already holds or is mid-claim on falls out at reserveClaim, which checks
// atomically; the claimable pre-filter just avoids pointless CAS attempts. Lost races and
// individual failures are skipped so one bad manifest cannot block the rest of the pass.
func (s *objectStore) claimEntry(ctx context.Context, f *fsm, e lockEntry) (claimedRun, bool) {
	if !s.claimable(e.manifest, time.Now()) {
		return claimedRun{}, false
	}

	if queue := admissionQueue(e.manifest); queue != "" {
		return s.claimQueued(ctx, f, e, queue)
	}

	manifest, err := s.claimManifest(ctx, e.version)
	switch {
	case errors.Is(err, errClaimLost), errors.Is(err, ErrFsmNotFound):
		return claimedRun{}, false
	case err != nil:
		s.logger.WithError(err).WithField("run_version", e.version.String()).Error("failed to claim run")
		return claimedRun{}, false
	}

	return claimedRun{f: f, resource: manifestResource(e.version, manifest)}, true
}

// claimQueued claims a queued run only if its queue admits it under the cluster-wide capacity
// limit. It reserves the run locally first — so a run already held or mid-claim here is skipped
// before the roster is touched, even though claimable() returns true for owner==self — then
// CAS-admits it into the queue roster, then claims the manifest lease, releasing the admitted slot
// if that manifest claim loses the race (per the RFC's "Interaction with run manifests"). A full
// queue leaves the run pending for a later pass, when a finishing run frees a slot.
func (s *objectStore) claimQueued(ctx context.Context, f *fsm, e lockEntry, queue string) (claimedRun, bool) {
	if !s.reserveClaim(e.version) {
		return claimedRun{}, false
	}

	switch admitted, err := s.admitQueued(ctx, queue, e.version); {
	case err != nil:
		s.dropLease(e.version)
		s.logger.WithError(err).WithField("run_version", e.version.String()).WithField("queue", queue).Error("failed to admit queued run")
		return claimedRun{}, false
	case !admitted:
		s.dropLease(e.version)
		return claimedRun{}, false
	}

	// claimReserved drops the reservation on failure; releaseQueued gives the admitted slot back.
	manifest, err := s.claimReserved(ctx, e.version, queue)
	if err != nil {
		// Release on a fresh context: a claim canceled by shutdown leaves ctx canceled, and the
		// compensating release must still land or the admitted slot leaks until its heartbeat goes
		// stale (this run was never tracked as held, so Close's lease sweep won't free it).
		releaseCtx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
		s.releaseQueued(releaseCtx, queue, e.version)
		cancel()
		if !errors.Is(err, errClaimLost) && !errors.Is(err, ErrFsmNotFound) {
			s.logger.WithError(err).WithField("run_version", e.version.String()).Error("failed to claim queued run")
		}
		return claimedRun{}, false
	}

	return claimedRun{f: f, resource: manifestResource(e.version, manifest)}, true
}

// releaseLease clears this node's ownership under the fence so peers can claim the run
// immediately. The epoch is left unchanged; a concurrent claimant's new lease fails the fence
// check and is left intact. Best-effort: an unreleased lease just waits out its expiry.
func (s *objectStore) releaseLease(ctx context.Context, version ulid.ULID, epoch int64) {
	queue := s.leaseQueue(version)
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

	// Free the queue slot alongside the lease so a peer re-admits immediately rather than waiting
	// out the roster heartbeat timeout — the graceful-shutdown (Close) and failed-resume
	// (ForgetRun) paths, where the released job's heartbeat is otherwise still fresh.
	if queue != "" {
		if rerr := s.releaseQueued(ctx, queue, version); rerr != nil {
			s.logger.WithError(rerr).WithField("run_version", version.String()).WithField("queue", queue).Error("failed to release queue slot")
		}
	}
}

func (s *objectStore) releaseLeases(ctx context.Context) {
	s.forEachOwned(func(version ulid.ULID, epoch int64) {
		s.releaseLease(ctx, version, epoch)
	})
}
