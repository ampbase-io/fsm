package fsm

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

// queueStore builds an objectStore over the harness' shared fake S3 with the given queue
// capacities configured, so the admission helpers can be exercised directly.
func (h *leaseHarness) queueStore(nodeID string, leaseTimeout time.Duration, queues map[string]int) *objectStore {
	h.t.Helper()
	store, err := newObjectStore(context.Background(), logrus.New(), &ObjectStorageConfig{
		Bucket:       h.bucket,
		Endpoint:     h.url,
		Region:       "auto",
		LeaseTimeout: leaseTimeout,
	}, nodeID, nil, queues)
	if err != nil {
		h.t.Fatalf("failed to create object store: %v", err)
	}
	h.t.Cleanup(func() { store.Close() })
	return store
}

func mustAdmit(t *testing.T, s *objectStore, queue string, v ulid.ULID, want bool) {
	t.Helper()
	got, err := s.admitQueued(context.Background(), queue, v)
	if err != nil {
		t.Fatalf("admitQueued(%s): %v", v, err)
	}
	if got != want {
		t.Fatalf("admitQueued(%s) = %v, want %v", v, got, want)
	}
}

// TestAdmitQueuedCapacity covers the core admission gate: runs are admitted up to capacity, a full
// roster refuses further admission, and releasing a slot lets a waiting run in.
func TestAdmitQueuedCapacity(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.queueStore("node-a", 30*time.Second, map[string]int{"q": 2})
	ctx := context.Background()

	v1, v2, v3 := ulid.Make(), ulid.Make(), ulid.Make()
	mustAdmit(t, s, "q", v1, true)
	mustAdmit(t, s, "q", v2, true)
	mustAdmit(t, s, "q", v3, false) // full at capacity 2

	if err := s.releaseQueued(ctx, "q", v1); err != nil {
		t.Fatalf("releaseQueued: %v", err)
	}
	mustAdmit(t, s, "q", v3, true) // the freed slot admits v3
}

// TestAdmitQueuedIdempotentAndOwnerChecked verifies that re-admitting a run this node already holds
// is a no-op success, and that a release is owner-checked so a peer cannot free another node's slot.
func TestAdmitQueuedIdempotentAndOwnerChecked(t *testing.T) {
	h := newLeaseHarness(t)
	a := h.queueStore("node-a", 30*time.Second, map[string]int{"q": 1})
	b := h.queueStore("node-b", 30*time.Second, map[string]int{"q": 1})
	ctx := context.Background()

	v := ulid.Make()
	mustAdmit(t, a, "q", v, true)
	mustAdmit(t, a, "q", v, true) // idempotent: already ours

	mustAdmit(t, b, "q", v, false) // held by node-a with a live heartbeat
	if err := b.releaseQueued(ctx, "q", v); err != nil {
		t.Fatalf("releaseQueued: %v", err)
	}

	q, _, err := a.getQueue(ctx, "q")
	if err != nil {
		t.Fatalf("getQueue: %v", err)
	}
	if len(q.GetJobs()) != 1 || q.GetJobs()[0].GetOwnerNode() != "node-a" {
		t.Fatalf("expected v still owned by node-a after node-b's release, got %+v", q.GetJobs())
	}
}

// TestReclaimStaleJobs checks the roster staleness filter: a job whose heartbeat predates the
// lease timeout is dropped, a fresh one is kept.
func TestReclaimStaleJobs(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.queueStore("node-a", 100*time.Millisecond, nil)

	now := time.Now().UnixMilli()
	q := &fsmv1.QueueFile{
		Capacity: 3,
		Jobs: []*fsmv1.QueueJob{
			{RunVersion: versionBytes(ulid.Make()), OwnerNode: "fresh", Heartbeat: now},
			{RunVersion: versionBytes(ulid.Make()), OwnerNode: "stale", Heartbeat: now - 500}, // > 100ms old
		},
	}
	if !s.reclaimStaleJobs(q) {
		t.Fatal("expected the stale entry to be reclaimed")
	}
	if len(q.Jobs) != 1 || q.Jobs[0].GetOwnerNode() != "fresh" {
		t.Fatalf("expected only the fresh job to remain, got %+v", q.Jobs)
	}
	if s.reclaimStaleJobs(q) {
		t.Fatal("expected no further reclamation")
	}
}

// TestHeartbeatQueuesReAddsMissing exercises the self-healing upsert: a run this node holds the
// lease on but whose roster slot is missing (a takeover-race miscount) is re-added by the heartbeat.
func TestHeartbeatQueuesReAddsMissing(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.queueStore("node-a", 30*time.Second, map[string]int{"q": 2})
	ctx := context.Background()

	v := ulid.Make()
	s.trackLease(v, 1, "q") // holds the lease under queue q, but the roster has no slot yet
	s.heartbeatQueues(ctx)

	q, _, err := s.getQueue(ctx, "q")
	if err != nil {
		t.Fatalf("getQueue: %v", err)
	}
	if job := findJob(q, v); job == nil || job.GetOwnerNode() != "node-a" {
		t.Fatalf("expected the heartbeat to re-add v for node-a, got %+v", q.GetJobs())
	}
}

// TestAdmitQueuedUnconfiguredDefersToRoster confirms a node missing the queue from its own config
// still honors the cluster limit: with no roster it defers (does not admit ungated, creates no
// roster), and once a configured node seeds the roster it gates against that authoritative capacity.
func TestAdmitQueuedUnconfiguredDefersToRoster(t *testing.T) {
	h := newLeaseHarness(t)
	unconfigured := h.queueStore("node-x", 30*time.Second, nil)
	configured := h.queueStore("node-a", 30*time.Second, map[string]int{"q": 1})
	ctx := context.Background()

	// No roster yet and no configured capacity to seed one: the unconfigured node does NOT admit
	// (the run stays pending for a configured node) and creates no roster.
	mustAdmit(t, unconfigured, "q", ulid.Make(), false)
	if _, _, err := unconfigured.getQueue(ctx, "q"); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected no roster created by an unconfigured node, got err=%v", err)
	}

	// Once a configured node seeds the roster (capacity 1, one job), the unconfigured node gates
	// against the roster's authoritative capacity instead of bypassing it.
	v := ulid.Make()
	mustAdmit(t, configured, "q", v, true)              // fills the single slot
	mustAdmit(t, unconfigured, "q", ulid.Make(), false) // roster full → not admitted

	// Freeing the slot lets the unconfigured node admit, still within the roster's capacity.
	if err := configured.releaseQueued(ctx, "q", v); err != nil {
		t.Fatalf("releaseQueued: %v", err)
	}
	mustAdmit(t, unconfigured, "q", ulid.Make(), true)
}

// TestQueueCapacityClusterWide is the multi-node contract test: several managers sharing one bucket
// must never run more than a queue's capacity concurrently, cluster-wide — the property the
// in-process runner violates (N x size). A shared inflight counter records the peak concurrency
// observed across all nodes' transitions; it must not exceed capacity.
func TestQueueCapacityClusterWide(t *testing.T) {
	const (
		capacity = 2
		nodes    = 3
		runs     = 6
	)

	bus := newTestBus()
	f := newObjectFactoryWithBus(t, bus, func(cfg *ObjectStorageConfig) {
		cfg.LeaseTimeout = 2 * time.Second
		cfg.HeartbeatPeriod = 200 * time.Millisecond
		cfg.ClaimInterval = 50 * time.Millisecond
	})
	ctx := context.Background()

	var inflight, maxSeen atomic.Int32
	register := func(m *Manager) Start[orderReq, orderResp] {
		start, _, err := m.Register[orderReq, orderResp]("capq").
			Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				n := inflight.Add(1)
				for {
					prev := maxSeen.Load()
					if n <= prev || maxSeen.CompareAndSwap(prev, n) {
						break
					}
				}
				time.Sleep(80 * time.Millisecond)
				inflight.Add(-1)
				return nil, nil
			}).
			End("done").
			Build(ctx)
		if err != nil {
			t.Fatalf("failed to build FSM: %v", err)
		}
		return start
	}

	var starter Start[orderReq, orderResp]
	m0, _ := f.newManager(map[string]int{"deploys": capacity})
	starter = register(m0)
	for i := 1; i < nodes; i++ {
		m, _ := f.newManager(map[string]int{"deploys": capacity})
		register(m)
	}

	// Every node's claim loop must be subscribed before the pending broadcasts fire, so no wakeup
	// is missed and work spreads across the fleet.
	eventually(t, 5*time.Second, func() bool {
		return bus.subscriberCount(subjectPending) >= nodes
	}, "not all nodes subscribed to the pending subject")

	versions := make([]ulid.ULID, 0, runs)
	for i := 0; i < runs; i++ {
		v, err := starter(ctx, fmt.Sprintf("capq-%d", i), NewRequest(&orderReq{}, &orderResp{}), WithQueue("deploys"))
		if err != nil {
			t.Fatalf("failed to start capq-%d: %v", i, err)
		}
		versions = append(versions, v)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, v := range versions {
		if err := m0.Wait(waitCtx, v); err != nil {
			t.Fatalf("queued run %s completed with error: %v", v, err)
		}
	}

	if got := maxSeen.Load(); got > capacity {
		t.Fatalf("cluster-wide concurrency exceeded capacity: peak %d > %d", got, capacity)
	}
}

// TestQueueSlotReclaimedAfterNodeDeath covers heartbeat reclamation end to end: a node claims a
// queued slot and then "dies" (its heartbeat and lease lapse); a peer must reclaim the abandoned
// slot within the timeout and drive the run to completion, even though the queue's capacity is one.
func TestQueueSlotReclaimedAfterNodeDeath(t *testing.T) {
	bus := newTestBus()
	nodes := 0
	f := newObjectFactoryWithBus(t, bus, func(cfg *ObjectStorageConfig) {
		nodes++
		cfg.LeaseTimeout = 150 * time.Millisecond
		cfg.ClaimInterval = time.Hour // only the pending wakeup claims, for determinism
		cfg.HeartbeatPeriod = 75 * time.Millisecond
		if nodes == 1 {
			cfg.HeartbeatPeriod = time.Hour // node-1's lease and queue slot lapse undefended
		}
	})
	ctx := context.Background()

	m1, _ := f.newManager(map[string]int{"deploys": 1})
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	defer close(block)
	start := blockingFSM(t, m1, "reclaimq", entered, block)

	version, err := start(ctx, "reclaimq-1", NewRequest(&orderReq{}, &orderResp{}), WithQueue("deploys"))
	if err != nil {
		t.Fatalf("failed to start queued FSM: %v", err)
	}
	<-entered // node-1 admitted, claimed, and is running the sole slot

	time.Sleep(300 * time.Millisecond) // let node-1's undefended lease and queue slot lapse

	m2, _ := f.newManager(map[string]int{"deploys": 1})
	completingFSM(t, m2, "reclaimq")

	// Wake node-2 once its claim loop is subscribed; it must reclaim node-1's stale slot to admit
	// and complete the run despite the capacity of one.
	eventually(t, 2*time.Second, func() bool {
		return bus.subscriberCount(subjectPending) >= 2
	}, "node-2 never subscribed to the pending subject")
	bus.Publish(subjectPending, &fsmv1.RunEvent{Kind: fsmv1.RunEventKind_RUN_EVENT_KIND_PENDING})

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("reclaimed run completed with error: %v", err)
	}
}

// startQueuedRun persists a queued run unowned (the deferred/ingress shape), so the claim path can
// admit and claim it. It returns the Run whose lock the claim scan will surface.
func startQueuedRun(t *testing.T, s *objectStore, id, queue string) Run {
	t.Helper()
	run := Run{ID: id, StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq", Queue: queue}
	_, err := s.Append(context.Background(), run, &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_START,
		Id:           run.ID,
		ResourceType: run.TypeName,
		Action:       run.Action,
		State:        "created",
	}, queue, withStartOption([]byte("{}"), []string{"created", "done"}), withUnowned())
	if err != nil {
		t.Fatalf("failed to start queued run: %v", err)
	}
	return run
}

// scanOne returns the single lock entry the queued run planted, failing if there isn't exactly one.
func scanOne(t *testing.T, s *objectStore) lockEntry {
	t.Helper()
	entries, err := s.scanLocks(context.Background(), s.lockPrefix("orderReq"))
	if err != nil {
		t.Fatalf("scanLocks: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly one lock entry, got %d", len(entries))
	}
	return entries[0]
}

// TestClaimQueuedSkipsHeldRun asserts the reservation-first gate: a run this store already holds is
// skipped before the roster is touched, so a Resume racing the claim tick cannot double-admit it.
func TestClaimQueuedSkipsHeldRun(t *testing.T) {
	h := newLeaseHarness(t)
	a := h.queueStore("node-a", 10*time.Second, map[string]int{"deploys": 1})
	ctx := context.Background()

	startQueuedRun(t, a, "app-1", "deploys")
	e := scanOne(t, a)

	if _, won := a.claimEntry(ctx, deployFSM, e); !won {
		t.Fatal("expected node-a to win the first queued claim")
	}
	// A second claim of the same run on the same store falls out at reserveClaim without re-admitting.
	if _, won := a.claimEntry(ctx, deployFSM, e); won {
		t.Fatal("expected the repeat claim of a held run to be skipped")
	}

	q, _, err := a.getQueue(ctx, "deploys")
	if err != nil {
		t.Fatalf("getQueue: %v", err)
	}
	if len(q.GetJobs()) != 1 {
		t.Fatalf("expected exactly one roster slot after a repeat claim, got %+v", q.GetJobs())
	}
}

// TestClaimQueuedReleasesSlotOnLostRace covers the RFC's "Interaction with run manifests"
// compensation: when a node admits a queued run into the roster but then loses the manifest claim,
// it must release the admitted slot rather than leak it.
func TestClaimQueuedReleasesSlotOnLostRace(t *testing.T) {
	h := newLeaseHarness(t)
	a := h.queueStore("node-a", 10*time.Second, map[string]int{"deploys": 1})
	b := h.queueStore("node-b", 10*time.Second, map[string]int{"deploys": 1})
	ctx := context.Background()

	run := startQueuedRun(t, a, "app-1", "deploys")
	e := scanOne(t, a) // a's claimable snapshot, captured before b takes the manifest

	// b wins the manifest first, so a's later claimReserved loses the race.
	if _, err := b.claimManifest(ctx, run.StartVersion); err != nil {
		t.Fatalf("node-b claim: %v", err)
	}

	if _, won := a.claimEntry(ctx, deployFSM, e); won {
		t.Fatal("expected node-a to lose the queued claim after b took the manifest")
	}

	q, _, err := a.getQueue(ctx, "deploys")
	if err != nil {
		t.Fatalf("getQueue: %v", err)
	}
	if findJob(q, run.StartVersion) != nil {
		t.Fatalf("expected node-a's admitted slot released after losing the claim, got %+v", q.GetJobs())
	}
}

// TestReleaseLeaseFreesQueueSlot asserts the graceful-release path (Close / ForgetRun): releasing a
// queued run's lease also frees its roster slot, so a peer re-admits at once instead of waiting out
// the heartbeat timeout.
func TestReleaseLeaseFreesQueueSlot(t *testing.T) {
	h := newLeaseHarness(t)
	a := h.queueStore("node-a", 10*time.Second, map[string]int{"deploys": 1})
	ctx := context.Background()

	run := startQueuedRun(t, a, "app-1", "deploys")
	if _, won := a.claimEntry(ctx, deployFSM, scanOne(t, a)); !won {
		t.Fatal("expected node-a to claim the queued run")
	}

	epoch, ok := a.ownedEpoch(run.StartVersion)
	if !ok {
		t.Fatal("expected node-a to hold the lease after claiming")
	}
	a.releaseLease(ctx, run.StartVersion, epoch)

	q, _, err := a.getQueue(ctx, "deploys")
	if err != nil {
		t.Fatalf("getQueue: %v", err)
	}
	if findJob(q, run.StartVersion) != nil {
		t.Fatalf("expected the roster slot freed on lease release, got %+v", q.GetJobs())
	}
}
