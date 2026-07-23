package fsm

import (
	"context"
	"errors"
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
}

func newLeaseHarness(t *testing.T) *leaseHarness {
	t.Helper()

	bucket, url, _ := startFakeS3(t)
	return &leaseHarness{t: t, bucket: bucket, url: url}
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
	}, nodeID)
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
	_, err := s.Append(context.Background(), run, &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_START,
		Id:           id,
		ResourceType: "orderReq",
		Action:       "deploy",
		State:        "created",
	}, "", withStartOption([]byte("{}"), []string{"created", "done"}))
	if err != nil {
		t.Fatalf("failed to append start event: %v", err)
	}
	return run
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

	// The tripped fence is reported by the next heartbeat pass so the local run is canceled.
	lost := a.extendLeases(ctx)
	if len(lost) != 1 || lost[0] != run.StartVersion {
		t.Fatalf("expected the fenced run reported as lost, got %v", lost)
	}
	if lost := a.extendLeases(ctx); len(lost) != 0 {
		t.Fatalf("expected a fence to be reported once, got %v", lost)
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
	if lost := a.extendLeases(ctx); len(lost) != 0 {
		t.Fatalf("expected no lost leases, got %v", lost)
	}

	after := mustManifest(t, a, run.StartVersion)
	if after.GetLeaseExpiry() <= before.GetLeaseExpiry() {
		t.Fatalf("expected the heartbeat to advance lease expiry: %d -> %d", before.GetLeaseExpiry(), after.GetLeaseExpiry())
	}

	// A steal discovered during the heartbeat reports the run as lost and drops the lease.
	time.Sleep(120 * time.Millisecond)
	if claimed, err := b.claimRuns(ctx, []*fsm{deployFSM}); err != nil || len(claimed) != 1 {
		t.Fatalf("expected node-b to claim the expired run, got %v (err=%v)", claimed, err)
	}
	lost := a.extendLeases(ctx)
	if len(lost) != 1 || lost[0] != run.StartVersion {
		t.Fatalf("expected the stolen run reported as lost, got %v", lost)
	}
	if _, tracked := a.ownedEpoch(run.StartVersion); tracked {
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
	if err := appendComplete(a, run); err != nil {
		t.Fatalf("failed to append complete event: %v", err)
	}
	if err := appendFinished(a, run); err != nil {
		t.Fatalf("failed to append finish event: %v", err)
	}

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
