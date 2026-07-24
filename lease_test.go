package fsm

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

// leaseHarness shares one fake S3 between several objectStores acting as distinct nodes.
type leaseHarness struct {
	t      *testing.T
	bucket string
	url    string
	fake   *fakeS3
}

func newLeaseHarness(t *testing.T) *leaseHarness {
	t.Helper()

	bucket, url, fake := startFakeS3(t)
	return &leaseHarness{t: t, bucket: bucket, url: url, fake: fake}
}

func mustManifest(t *testing.T, s *objectStore, version ulid.ULID) *fsmv1.RunManifest {
	t.Helper()
	manifest, _, err := s.getManifest(context.Background(), version)
	if err != nil {
		t.Fatalf("failed to read manifest: %v", err)
	}
	return manifest
}

// asymmetricTimings gives the first manager created by the factory a lapsing lease (slow or
// suppressed heartbeat, no claim loop) and every later manager aggressive claim timings, so
// the second manager deterministically takes over the first's runs.
func asymmetricTimings(ownerHeartbeat time.Duration) func(*ObjectStorageConfig) {
	nodes := 0
	return func(cfg *ObjectStorageConfig) {
		nodes++
		cfg.LeaseTimeout = 150 * time.Millisecond
		if nodes == 1 {
			cfg.HeartbeatPeriod = ownerHeartbeat
			cfg.ClaimInterval = time.Hour
			return
		}
		cfg.HeartbeatPeriod = 75 * time.Millisecond
		cfg.ClaimInterval = 75 * time.Millisecond
	}
}

func (h *leaseHarness) store(nodeID string, leaseTimeout time.Duration) *objectStore {
	h.t.Helper()

	store, err := newObjectStore(context.Background(), logrus.New(), &ObjectStorageConfig{
		Bucket:       h.bucket,
		Endpoint:     h.url,
		Region:       "auto",
		LeaseTimeout: leaseTimeout,
	}, nodeID, nil)
	if err != nil {
		h.t.Fatalf("failed to create object store: %v", err)
	}
	return store
}

// deployFSM is the minimal fsm identity claimRuns needs to scan for runs.
var deployFSM = &fsm{typeName: "orderReq", action: "deploy"}

func startRun(t *testing.T, s *objectStore, id string) Run {
	t.Helper()

	run := Run{ID: id, StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq"}
	if err := appendStarted(s, run); err != nil {
		t.Fatalf("failed to append start event: %v", err)
	}
	return run
}

func appendStarted(s *objectStore, run Run) error {
	_, err := s.Append(context.Background(), run, &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_START,
		Id:           run.ID,
		ResourceType: run.TypeName,
		Action:       run.Action,
		State:        "created",
	}, "", withStartOption([]byte("{}"), []string{"created", "done"}))
	return err
}

func appendComplete(s *objectStore, run Run) error {
	_, err := s.Append(context.Background(), run, &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_COMPLETE,
		Id:           run.ID,
		ResourceType: run.TypeName,
		Action:       run.Action,
		State:        "created",
	}, "")
	return err
}

func appendFinished(s *objectStore, run Run) error {
	_, err := s.Append(context.Background(), run, &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_FINISH,
		Id:           run.ID,
		ResourceType: run.TypeName,
		Action:       run.Action,
		State:        "done",
	}, "")
	return err
}

// finishRun drives a started run through completion and finish.
func finishRun(t *testing.T, s *objectStore, run Run) {
	t.Helper()
	if err := appendComplete(s, run); err != nil {
		t.Fatalf("failed to append complete event: %v", err)
	}
	if err := appendFinished(s, run); err != nil {
		t.Fatalf("failed to append finish event: %v", err)
	}
}

// putLock plants a resource lock recording version as its holder.
func putLock(t *testing.T, s *objectStore, key string, version ulid.ULID) {
	t.Helper()
	versionBytes, err := version.MarshalText()
	if err != nil {
		t.Fatalf("failed to marshal version: %v", err)
	}
	if err := s.putIfAbsent(context.Background(), key, versionBytes); err != nil {
		t.Fatalf("failed to create lock: %v", err)
	}
}

func runLockKey(s *objectStore, run Run) string {
	return s.lockKey(run.TypeName, run.ID, run.Action, "", run.StartVersion)
}

func TestLeaseStampedAtStart(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)

	run := startRun(t, s, "lease-1")

	manifest := mustManifest(t, s, run.StartVersion)
	if manifest.GetOwnerNode() != "node-a" {
		t.Fatalf("expected owner node-a, got %q", manifest.GetOwnerNode())
	}
	if manifest.GetLeaseEpoch() != 1 {
		t.Fatalf("expected lease epoch 1, got %d", manifest.GetLeaseEpoch())
	}
	if manifest.GetLeaseExpiry() <= time.Now().UnixMilli() {
		t.Fatalf("expected lease expiry in the future, got %d", manifest.GetLeaseExpiry())
	}
	if epoch, ok := s.ownedEpoch(run.StartVersion); !ok || epoch != 1 {
		t.Fatalf("expected lease tracked at epoch 1, got %d (tracked=%v)", epoch, ok)
	}
}

func TestFencedAppendAfterSteal(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 50*time.Millisecond)
	b := h.store("node-b", 10*time.Second)

	run := startRun(t, a, "steal-1")
	time.Sleep(120 * time.Millisecond)

	claimed, err := b.claimRuns(ctx, []*fsm{deployFSM})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected node-b to claim the expired run, got %v (err=%v)", claimed, err)
	}

	if err := appendComplete(a, run); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected fenced mid-run append to return ErrLeaseLost, got %v", err)
	}
	if err := appendFinished(a, run); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected fenced finish to return ErrLeaseLost, got %v", err)
	}

	manifest := mustManifest(t, a, run.StartVersion)
	if len(manifest.GetCompletedStates()) != 0 {
		t.Fatalf("fenced append still mutated the manifest: %v", manifest.GetCompletedStates())
	}
	if manifestTerminal(manifest) {
		t.Fatal("fenced finish still completed the manifest")
	}
	if _, err := a.readHistory(ctx, run.StartVersion); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("fenced finish still wrote history: %v", err)
	}
	if _, _, err := a.getObject(ctx, runLockKey(a, run)); err != nil {
		t.Fatalf("expected the resource lock to survive a fenced finish, got %v", err)
	}

	// The tripped fence dropped the lease, so the coordinate loop's sweep of executing runs
	// without a lease cancels the local run.
	if a.owns(run.StartVersion) {
		t.Fatal("expected the fenced lease dropped")
	}
}

func TestExtendLeases(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 50*time.Millisecond)
	b := h.store("node-b", 10*time.Second)

	run := startRun(t, a, "extend-1")
	before := mustManifest(t, a, run.StartVersion)

	time.Sleep(5 * time.Millisecond)
	a.extendLeases(ctx)
	if !a.owns(run.StartVersion) {
		t.Fatal("expected the heartbeat to keep a healthy lease")
	}

	after := mustManifest(t, a, run.StartVersion)
	if after.GetLeaseExpiry() <= before.GetLeaseExpiry() {
		t.Fatalf("expected the heartbeat to advance lease expiry: %d -> %d", before.GetLeaseExpiry(), after.GetLeaseExpiry())
	}

	// A steal discovered during the heartbeat drops the lease from the local set.
	time.Sleep(120 * time.Millisecond)
	if claimed, err := b.claimRuns(ctx, []*fsm{deployFSM}); err != nil || len(claimed) != 1 {
		t.Fatalf("expected node-b to claim the expired run, got %v (err=%v)", claimed, err)
	}
	a.extendLeases(ctx)
	if a.owns(run.StartVersion) {
		t.Fatal("expected the stolen lease to be dropped")
	}
}

func TestClaimEligibility(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()

	// A live lease is not claimable.
	a := h.store("node-a", 10*time.Second)
	live := startRun(t, a, "held-1")
	b := h.store("node-b", 10*time.Second)
	if claimed, err := b.claimRuns(ctx, []*fsm{deployFSM}); err != nil || len(claimed) != 0 {
		t.Fatalf("expected no claims against a live lease, got %v (err=%v)", claimed, err)
	}

	// The holder itself never re-claims a run it is executing, even past expiry.
	c := h.store("node-c", 50*time.Millisecond)
	startRun(t, c, "self-1")
	time.Sleep(120 * time.Millisecond)
	if claimed, err := c.claimRuns(ctx, []*fsm{deployFSM}); err != nil || len(claimed) != 0 {
		t.Fatalf("expected no self-claims of tracked runs, got %v (err=%v)", claimed, err)
	}

	// The expired run is claimable by a peer; the live one stays with its owner.
	claimed, err := b.claimRuns(ctx, []*fsm{deployFSM})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected exactly the expired run claimed, got %v (err=%v)", claimed, err)
	}
	if claimed[0].resource.version == live.StartVersion {
		t.Fatal("claimed the live run instead of the expired one")
	}

	manifest := mustManifest(t, b, claimed[0].resource.version)
	if manifest.GetOwnerNode() != "node-b" || manifest.GetLeaseEpoch() != 2 {
		t.Fatalf("expected owner node-b at epoch 2, got %q/%d", manifest.GetOwnerNode(), manifest.GetLeaseEpoch())
	}
}

func TestClaimCarriesResumeState(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 50*time.Millisecond)
	b := h.store("node-b", 10*time.Second)

	run := startRun(t, a, "resume-1")
	if err := appendComplete(a, run); err != nil {
		t.Fatalf("failed to append complete event: %v", err)
	}
	time.Sleep(120 * time.Millisecond)

	claimed, err := b.claimRuns(ctx, []*fsm{deployFSM})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected one claimed run, got %v (err=%v)", claimed, err)
	}
	resource := claimed[0].resource
	if resource.version != run.StartVersion {
		t.Fatalf("unexpected claimed version %s", resource.version)
	}
	if len(resource.completedTransitions) != 1 || resource.completedTransitions[0] != "created" {
		t.Fatalf("expected completed transition [created], got %v", resource.completedTransitions)
	}
}

func TestClaimRace(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 50*time.Millisecond)
	startRun(t, a, "race-1")
	time.Sleep(120 * time.Millisecond)

	b := h.store("node-b", 10*time.Second)
	c := h.store("node-c", 10*time.Second)

	results := make(chan int, 2)
	for _, s := range []*objectStore{b, c} {
		go func() {
			claimed, err := s.claimRuns(ctx, []*fsm{deployFSM})
			if err != nil {
				t.Errorf("claimRuns failed: %v", err)
			}
			results <- len(claimed)
		}()
	}
	if total := <-results + <-results; total != 1 {
		t.Fatalf("expected exactly one node to win the claim race, got %d claims", total)
	}
}

// TestClaimRaceSameStore races two claimers on one store — a caller-invoked Resume against
// the claim tick — which the claim reservation must resolve to a single claim and a single
// epoch bump.
func TestClaimRaceSameStore(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 50*time.Millisecond)
	run := startRun(t, a, "selfrace-1")
	time.Sleep(120 * time.Millisecond)

	b := h.store("node-b", 10*time.Second)
	results := make(chan int, 2)
	for range 2 {
		go func() {
			claimed, err := b.claimRuns(ctx, []*fsm{deployFSM})
			if err != nil {
				t.Errorf("claimRuns failed: %v", err)
			}
			results <- len(claimed)
		}()
	}
	if total := <-results + <-results; total != 1 {
		t.Fatalf("expected exactly one same-store claim, got %d", total)
	}
	if epoch := mustManifest(t, b, run.StartVersion).GetLeaseEpoch(); epoch != 2 {
		t.Fatalf("expected a single epoch bump to 2, got %d", epoch)
	}
}

// TestClaimSurvivesLostResponse covers the lost-200 replay on the claim path: the claim's
// manifest CAS succeeds server-side but its response is lost, so the retry re-reads its own
// half-acknowledged claim (owner == self, which claimable accepts) and bumps the epoch again.
// The double bump is harmless only if nothing retains the older epoch — the tracked lease
// must match the manifest as written, and the claimer must not fence itself.
func TestClaimSurvivesLostResponse(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 50*time.Millisecond)
	run := startRun(t, a, "lost-1")
	time.Sleep(120 * time.Millisecond)

	b := h.store("node-b", 10*time.Second)
	h.fake.lostPuts = 1 // the next conditional PUT is b's claim CAS
	claimed, err := b.claimRuns(ctx, []*fsm{deployFSM})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected the claim to survive a lost response, got %v (err=%v)", claimed, err)
	}

	manifest := mustManifest(t, b, run.StartVersion)
	if got := manifest.GetLeaseEpoch(); got != 3 {
		t.Fatalf("expected the lost response to double-bump the epoch to 3, got %d", got)
	}
	if epoch, ok := b.ownedEpoch(run.StartVersion); !ok || epoch != manifest.GetLeaseEpoch() {
		t.Fatalf("expected the tracked epoch to match the manifest's %d, got %d (tracked=%v)", manifest.GetLeaseEpoch(), epoch, ok)
	}
	// The claimer's next write must pass its own fence.
	if err := appendComplete(b, run); err != nil {
		t.Fatalf("claimer fenced itself after a lost claim response: %v", err)
	}
}

func TestForgetRunReleasesLease(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 10*time.Second)
	b := h.store("node-b", 10*time.Second)

	run := startRun(t, a, "forget-1")
	if err := a.ForgetRun(run); err != nil {
		t.Fatalf("failed to forget run: %v", err)
	}

	if _, tracked := a.ownedEpoch(run.StartVersion); tracked {
		t.Fatal("expected the lease dropped by ForgetRun")
	}
	if owner := mustManifest(t, b, run.StartVersion).GetOwnerNode(); owner != "" {
		t.Fatalf("expected ownership released, still owned by %q", owner)
	}
	if claimed, err := b.claimRuns(ctx, []*fsm{deployFSM}); err != nil || len(claimed) != 1 {
		t.Fatalf("expected the released run claimable, got %v (err=%v)", claimed, err)
	}
}

// TestAdoptRunManifest exercises the crash-retry adoption seam directly: a START whose
// earlier attempt already created the manifest adopts the lease at the manifest's recorded
// epoch when this node owns it, and is fenced when another node does.
func TestAdoptRunManifest(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a1 := h.store("node-a", 10*time.Second)

	run := startRun(t, a1, "adopt-1")
	// Move the epoch off its initial value so adoption provably reads the manifest rather
	// than assuming a fresh lease.
	if _, err := a1.casManifest(ctx, run.StartVersion, func(m *fsmv1.RunManifest) error {
		m.LeaseEpoch = 7
		return nil
	}); err != nil {
		t.Fatalf("failed to bump lease epoch: %v", err)
	}

	// A restarted incarnation of the owning node adopts the existing lease.
	a2 := h.store("node-a", 10*time.Second)
	if err := a2.adoptRunManifest(ctx, run, runLockKey(a2, run)); err != nil {
		t.Fatalf("expected the owning node to adopt its manifest, got %v", err)
	}
	if epoch, ok := a2.ownedEpoch(run.StartVersion); !ok || epoch != 7 {
		t.Fatalf("expected the manifest's epoch 7 adopted, got %d (tracked=%v)", epoch, ok)
	}

	// A foreign node is fenced.
	b := h.store("node-b", 10*time.Second)
	if err := b.adoptRunManifest(ctx, run, runLockKey(b, run)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected a foreign adopter fenced with ErrLeaseLost, got %v", err)
	}
	if b.owns(run.StartVersion) {
		t.Fatal("expected no lease tracked by the fenced adopter")
	}
}

// TestStartRetryAfterFinishRejected covers the manifest-adopt path: a crash-retry of a START
// whose run already completed must not re-execute the run, and must leave no lock behind.
func TestStartRetryAfterFinishRejected(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 10*time.Second)

	run := startRun(t, a, "replay-1")
	finishRun(t, a, run)

	if _, ok := errors.AsType[*AlreadyRunningError](appendStarted(a, run)); !ok {
		t.Fatal("expected AlreadyRunningError for a finished run's START retry")
	}

	if _, tracked := a.ownedEpoch(run.StartVersion); tracked {
		t.Fatal("expected no lease adopted from a terminal manifest")
	}
	if got := mustManifest(t, a, run.StartVersion).GetStatus(); got != fsmv1.RunState_RUN_STATE_COMPLETE {
		t.Fatalf("expected the manifest to stay COMPLETE, got %v", got)
	}
	if _, _, err := a.getObject(ctx, runLockKey(a, run)); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected the retry's lock removed, got %v", err)
	}
}

// TestCompletedRunLockCleanedAtStart covers the crash window between manifest completion and
// lock deletion: a new run for the resource finishes the cleanup instead of reporting a
// spurious AlreadyRunning.
func TestCompletedRunLockCleanedAtStart(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 10*time.Second)

	run := startRun(t, a, "restart-1")
	finishRun(t, a, run)

	lockKey := runLockKey(a, run)
	putLock(t, a, lockKey, run.StartVersion)

	next := startRun(t, a, "restart-1")
	owner, err := a.lockOwner(ctx, lockKey)
	if err != nil || owner != next.StartVersion {
		t.Fatalf("expected the lock held by the new run %s, got %s (err=%v)", next.StartVersion, owner, err)
	}
}

// TestScanLocksBeyondFanout guards the fan-out shape itself: a scan of more locks than
// scanFanout must complete and return every resolvable entry. Both a mis-ordered
// waiter-closer and an undersized channel deadlock exactly here, and no other test scans
// past the bound.
func TestScanLocksBeyondFanout(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 10*time.Second)

	const runs = 2*scanFanout + 1
	for i := range runs {
		startRun(t, a, fmt.Sprintf("fan-%02d", i))
	}

	done := make(chan struct{})
	var entries []lockEntry
	go func() {
		defer close(done)
		var err error
		entries, err = a.scanLocks(ctx, a.lockPrefix("orderReq"))
		if err != nil {
			t.Errorf("scanLocks failed: %v", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("scanLocks hung scanning more locks than scanFanout")
	}
	if len(entries) != runs {
		t.Fatalf("expected %d entries, got %d", runs, len(entries))
	}
}

// TestStaleOrphanLockReaped covers lock-without-manifest recovery: a lock old enough that no
// START can still be writing its manifest is reaped; a fresh one is left alone.
func TestStaleOrphanLockReaped(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 50*time.Millisecond)

	staleVersion := testULID(t, uint64(time.Now().Add(-time.Minute).UnixMilli()))
	staleKey := a.lockKey("orderReq", "stale-1", "deploy", "", staleVersion)
	putLock(t, a, staleKey, staleVersion)

	freshVersion := ulid.Make()
	freshKey := a.lockKey("orderReq", "fresh-1", "deploy", "", freshVersion)
	putLock(t, a, freshKey, freshVersion)

	entries, err := a.scanLocks(ctx, a.lockPrefix("orderReq"))
	if err != nil {
		t.Fatalf("scanLocks failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no resolvable entries, got %v", entries)
	}

	if _, _, err := a.getObject(ctx, staleKey); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected the stale lock reaped, got %v", err)
	}
	if _, _, err := a.getObject(ctx, freshKey); err != nil {
		t.Fatalf("expected the fresh lock left in place, got %v", err)
	}
}

func TestZombieNodeFencedAfterRestart(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a1 := h.store("node-a", 10*time.Second)

	run := startRun(t, a1, "zombie-1")

	// A restarted incarnation of the same node finds its own stale lease and re-claims it;
	// the epoch bump fences the old incarnation even though the NodeID matches.
	a2 := h.store("node-a", 10*time.Second)
	claimed, err := a2.claimRuns(ctx, []*fsm{deployFSM})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected the restarted node to reclaim its stale self-lease, got %v (err=%v)", claimed, err)
	}
	if err := appendComplete(a1, run); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected the zombie incarnation to be fenced, got %v", err)
	}
	if err := appendComplete(a2, run); err != nil {
		t.Fatalf("expected the new incarnation to append freely, got %v", err)
	}
}

func TestFinishDropsLease(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 10*time.Second)
	b := h.store("node-b", 10*time.Second)

	run := startRun(t, a, "finish-1")
	finishRun(t, a, run)

	if _, tracked := a.ownedEpoch(run.StartVersion); tracked {
		t.Fatal("expected the lease dropped at finish")
	}
	if claimed, err := b.claimRuns(ctx, []*fsm{deployFSM}); err != nil || len(claimed) != 0 {
		t.Fatalf("expected nothing claimable after finish, got %v (err=%v)", claimed, err)
	}
}

func TestCloseReleasesLeases(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 10*time.Second)
	b := h.store("node-b", 10*time.Second)

	run := startRun(t, a, "close-1")
	if err := a.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	if owner := mustManifest(t, b, run.StartVersion).GetOwnerNode(); owner != "" {
		t.Fatalf("expected ownership released at close, still owned by %q", owner)
	}

	// Peers claim immediately instead of waiting out the lease timeout.
	claimed, err := b.claimRuns(ctx, []*fsm{deployFSM})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected the released run claimable, got %v (err=%v)", claimed, err)
	}
	if epoch := mustManifest(t, b, run.StartVersion).GetLeaseEpoch(); epoch != 2 {
		t.Fatalf("expected epoch 2 after release and re-claim, got %d", epoch)
	}
}

// --- Manager-level lease tests ---

// TestHeartbeatKeepsOwnership verifies a live owner is never robbed: a second manager with an
// aggressive claim loop and an explicit Resume must not take a run whose owner heartbeats.
func TestHeartbeatKeepsOwnership(t *testing.T) {
	f := newObjectFactoryWith(t, func(cfg *ObjectStorageConfig) {
		cfg.LeaseTimeout = time.Second
		cfg.HeartbeatPeriod = 100 * time.Millisecond
		cfg.ClaimInterval = 100 * time.Millisecond
	})
	ctx := context.Background()

	m1, _ := f.newManager(nil)
	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	start := blockingFSM(t, m1, "keep", entered, block)
	version, err := start(ctx, "keep-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	m2, _ := f.newManager(nil)
	var stolen atomic.Int32
	_, resume, err := m2.Register[orderReq, orderResp]("keep").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			stolen.Add(1)
			return nil, nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	if err := resume(ctx); err != nil {
		t.Fatalf("failed to resume: %v", err)
	}

	// Hold the run well past several lease timeouts while m1's heartbeat defends it.
	time.Sleep(2500 * time.Millisecond)
	close(block)
	if err := m1.Wait(ctx, version); err != nil {
		t.Fatalf("FSM completed with error: %v", err)
	}
	if got := stolen.Load(); got != 0 {
		t.Fatalf("a heartbeating owner's run was claimed %d times", got)
	}
}

// TestLeaseLossCancelsRun verifies the takeover path end to end: an owner whose heartbeat
// cannot keep up loses its lease to a peer's claim loop, the peer completes the run from its
// recorded state, and the old owner's local context is canceled with ErrLeaseLost.
func TestLeaseLossCancelsRun(t *testing.T) {
	// The first manager heartbeats slower than its lease expires, so every extension leaves a
	// takeover window.
	f := newObjectFactoryWith(t, asymmetricTimings(400*time.Millisecond))
	ctx := context.Background()

	m1, _ := f.newManager(nil)
	var (
		entered  = make(chan struct{}, 1)
		canceled = make(chan error, 1)
	)
	start, _, err := m1.Register[orderReq, orderResp]("takeover").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			<-ctx.Done()
			canceled <- context.Cause(ctx)
			return nil, ctx.Err()
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	version, err := start(ctx, "takeover-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	// The second manager's claim loop takes the run once the lease lapses; no explicit
	// Resume call is involved.
	m2, _ := f.newManager(nil)
	completingFSM(t, m2, "takeover")

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("claimed run completed with error: %v", err)
	}

	select {
	case cause := <-canceled:
		if !errors.Is(cause, ErrLeaseLost) {
			t.Fatalf("expected the old owner's run canceled with ErrLeaseLost, got %v", cause)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the old owner's run was never canceled after losing its lease")
	}
}

// TestResumeContinuesPastFailedRun verifies a caller-invoked Resume dispatches every claimed
// run even when one fails: the claims are already this node's, so stopping at the first
// failure would leave the remainder heartbeat-extended but never executed.
func TestResumeContinuesPastFailedRun(t *testing.T) {
	nodes := 0
	f := newObjectFactoryWith(t, func(cfg *ObjectStorageConfig) {
		nodes++
		cfg.LeaseTimeout = 150 * time.Millisecond
		cfg.ClaimInterval = time.Hour // the explicit Resume is the only claim path
		cfg.HeartbeatPeriod = 75 * time.Millisecond
		if nodes == 1 {
			cfg.HeartbeatPeriod = time.Hour
		}
	})
	ctx := context.Background()

	m1, _ := f.newManager(nil)
	var (
		entered = make(chan struct{}, 2)
		block   = make(chan struct{})
	)
	defer close(block)
	start := blockingFSM(t, m1, "poison", entered, block)

	// poison-1 sorts before poison-2 in the lock listing, so the failing run is dispatched
	// first and the healthy one only completes if Resume keeps going.
	v1, err := start(ctx, "poison-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered
	v2, err := start(ctx, "poison-2", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	// Corrupt the first run's persisted resource so its resume fails to unmarshal.
	store := m1.store.(*objectStore)
	if _, err := store.casManifest(ctx, v1, func(m *fsmv1.RunManifest) error {
		m.Resource = []byte("corrupt")
		return nil
	}); err != nil {
		t.Fatalf("failed to corrupt resource: %v", err)
	}

	time.Sleep(300 * time.Millisecond) // let m1's undefended leases lapse

	m2, _ := f.newManager(nil)
	resume := completingFSM(t, m2, "poison")
	if err := resume(ctx); err == nil {
		t.Fatal("expected Resume to report the poisoned run")
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, v2); err != nil {
		t.Fatalf("healthy run after the poisoned one completed with error: %v", err)
	}

	// The poisoned run's claim was released for another node (or a later pass) to retry.
	if owner := mustManifest(t, store, v1).GetOwnerNode(); owner != "" {
		t.Fatalf("expected the poisoned run's lease released, still owned by %q", owner)
	}
}

// TestLostLeaseDelayedRunNeverExecutes verifies the ownership check at execute-start: a
// delayed run whose lease was claimed by another node during the delay must cancel before its
// first transition's side effects run, no matter how long after the loss it dispatches.
func TestLostLeaseDelayedRunNeverExecutes(t *testing.T) {
	f := newObjectFactoryWith(t, asymmetricTimings(400*time.Millisecond))
	ctx := context.Background()

	m1, _ := f.newManager(nil)
	var executed atomic.Int32
	start, _, err := m1.Register[orderReq, orderResp]("dsteal").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			executed.Add(1)
			return nil, nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	dispatchAt := time.Now().Add(1200 * time.Millisecond)
	version, err := start(ctx, "dsteal-1", NewRequest(&orderReq{}, &orderResp{}), WithDelayedStart(dispatchAt))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}

	// m2's claim loop takes the run while it waits out its delay (m1's slow heartbeat lets
	// the lease lapse) and completes it after the delay elapses.
	m2, _ := f.newManager(nil)
	completingFSM(t, m2, "dsteal")

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("claimed run completed with error: %v", err)
	}

	// Wait until well past m1's own dispatch moment (the persisted delay truncates to seconds,
	// so m2 may have finished earlier), then confirm m1 never ran user code.
	time.Sleep(time.Until(dispatchAt) + 500*time.Millisecond)
	if got := executed.Load(); got != 0 {
		t.Fatalf("the old owner executed a stolen delayed run %d times", got)
	}
}

// TestClaimResumesFromCompletedStates verifies a claimed run continues from the transitions
// its previous owner durably completed rather than starting over.
func TestClaimResumesFromCompletedStates(t *testing.T) {
	f := newObjectFactoryWith(t, asymmetricTimings(time.Hour))
	ctx := context.Background()

	m1, _ := f.newManager(nil)
	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	defer close(block)
	start, _, err := m1.Register[orderReq, orderResp]("phased").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return NewResponse(&orderResp{Status: "created"}), nil
		}).
		To("shipped", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			select {
			case <-block:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	version, err := start(ctx, "phased-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	m2, _ := f.newManager(nil)
	var reranCreated, ranShipped atomic.Int32
	if _, _, err := m2.Register[orderReq, orderResp]("phased").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			reranCreated.Add(1)
			return nil, nil
		}).
		To("shipped", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			ranShipped.Add(1)
			return nil, nil
		}).
		End("done").
		Build(ctx); err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("claimed run completed with error: %v", err)
	}

	if got := reranCreated.Load(); got != 0 {
		t.Fatalf("claimed run re-executed a completed transition %d times", got)
	}
	if got := ranShipped.Load(); got != 1 {
		t.Fatalf("expected the interrupted transition to run once on the claimer, ran %d times", got)
	}
}
