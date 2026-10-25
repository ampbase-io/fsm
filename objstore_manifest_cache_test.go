package fsm

import (
	"context"
	"errors"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// TestCASReReadsAfterAnotherNodeWrote is the guard on the cached CAS target: the owner conditions
// its next write on the manifest it last wrote, so a write by anyone else has to be noticed. It
// is, by the precondition failing — and the retry must then re-read and re-apply, keeping the
// other write rather than replaying stale bytes over it.
func TestCASReReadsAfterAnotherNodeWrote(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a, b := h.store("node-a", 10*time.Second), h.store("node-b", 10*time.Second)

	run := startRun(t, a, "cas-1")

	// Any write by a node that does not hold the lease: a settle, a recorded cancel, a repair.
	// What matters is that node-a's cached ETag no longer describes the object.
	if _, err := b.casManifest(ctx, run.StartVersion, func(m *fsmv1.RunManifest) error {
		m.RetryCount = 7
		return nil
	}); err != nil {
		t.Fatalf("the foreign write failed: %v", err)
	}

	if err := appendComplete(a, run); err != nil {
		t.Fatalf("the owner's next append failed: %v", err)
	}

	manifest := mustManifest(t, a, run.StartVersion)
	if got := manifest.GetRetryCount(); got != 7 {
		t.Fatalf("the owner's append replayed stale bytes over the other write: retry count %d, want 7", got)
	}
	if got := manifest.GetCompletedStates(); len(got) != 1 || got[0] != "created" {
		t.Fatalf("the owner's append did not record its transition: %v", got)
	}
}

// TestCASFencesAfterStealOnCachedTag is the same path with the lease gone: the stale cached tag
// must not let a fenced owner write, and the re-read the failed precondition forces is what
// trips the fence. TestFencedAppendAfterSteal covers the steal an expiry causes; this one takes
// the lease while the owner's cache is warm and its own clock still believes.
func TestCASFencesAfterStealOnCachedTag(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a, b := h.store("node-a", 10*time.Second), h.store("node-b", 10*time.Second)

	run := startRun(t, a, "cas-steal-1")
	if _, err := b.casManifest(ctx, run.StartVersion, func(m *fsmv1.RunManifest) error {
		m.OwnerNode = "node-b"
		m.LeaseEpoch++
		return nil
	}); err != nil {
		t.Fatalf("the steal failed: %v", err)
	}

	if err := appendComplete(a, run); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("the fenced owner's append = %v, want ErrLeaseLost", err)
	}
	if owns(a, run.StartVersion) {
		t.Fatal("expected the fenced owner to drop its lease")
	}
	if states := mustManifest(t, a, run.StartVersion).GetCompletedStates(); len(states) != 0 {
		t.Fatalf("the fenced append still recorded a transition: %v", states)
	}
}

// TestManifestCacheIsTheLease pins the cache's lifetime and its copying. It lives in the lease
// entry, so a run this node does not hold has none, and losing the lease cannot leave a tag to
// write against.
func TestManifestCacheIsTheLease(t *testing.T) {
	s, _ := newTestObjectStore(t)
	version := ulid.Make()
	manifest := &fsmv1.RunManifest{Status: fsmv1.RunState_RUN_STATE_PENDING, ResourceId: "cached-1"}

	s.cacheManifest(version, manifest, `"etag-1"`)
	if _, _, cached := s.cachedManifest(version); cached {
		t.Fatal("a run this node holds no lease on should cache nothing")
	}

	s.trackLease(version, 1, "")
	s.cacheManifest(version, manifest, `"etag-1"`)
	got, etag, cached := s.cachedManifest(version)
	if !cached || etag != `"etag-1"` {
		t.Fatalf("expected the cached target, got (%v, %q, %v)", got, etag, cached)
	}

	got.ResourceId = "mutated"
	if again, _, _ := s.cachedManifest(version); again.GetResourceId() != "cached-1" {
		t.Fatalf("the cache handed out its own copy: %q", again.GetResourceId())
	}

	s.forgetManifest(version)
	if _, _, cached := s.cachedManifest(version); cached {
		t.Fatal("expected the forgotten target to be gone")
	}

	s.cacheManifest(version, manifest, `"etag-2"`)
	s.dropLease(version)
	if _, _, cached := s.cachedManifest(version); cached {
		t.Fatal("expected a dropped lease to take its cached target with it")
	}
}

// TestResumeWithoutEventObjects pins what the mid-run event objects are: an audit trail, not the
// recovery source. A run whose events are gone — deleted here, archived or lost to a failed write
// in production — is resumed from its manifest by another node and finishes, and its history
// record is intact, since the FINISH event the record names is written by the node that finishes
// it.
func TestResumeWithoutEventObjects(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 50*time.Millisecond)
	b := h.store("node-b", 10*time.Second)

	run := startRun(t, a, "no-events-1")
	if err := appendComplete(a, run); err != nil {
		t.Fatalf("failed to complete the first transition: %v", err)
	}
	if err := a.deletePrefix(ctx, a.eventPrefix(run.ID, run.Action, run.StartVersion)); err != nil {
		t.Fatalf("failed to delete the run's events: %v", err)
	}

	time.Sleep(120 * time.Millisecond)
	claimed, err := b.claimRuns(ctx, []fsmKey{deployKey})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected node-b to claim the run, got %v (err=%v)", claimed, err)
	}
	resumed := claimed[0].resource
	if got := resumed.completedTransitions; len(got) != 1 || got[0] != "created" {
		t.Fatalf("the resumed run lost its progress: %v", got)
	}
	if got := resumed.active.GetTransitions(); len(got) == 0 {
		t.Fatal("the resumed run lost its transitions")
	}

	if err := appendFinished(b, runFromManifest(run.StartVersion, mustManifest(t, b, run.StartVersion))); err != nil {
		t.Fatalf("the resumed run failed to finish: %v", err)
	}
	history, err := b.History(ctx, run.StartVersion)
	if err != nil {
		t.Fatalf("failed to read the finished run's history: %v", err)
	}
	if history.GetLastEvent().GetType() != fsmv1.EventType_EVENT_TYPE_FINISH {
		t.Fatalf("the history record does not end in FINISH: %v", history.GetLastEvent().GetType())
	}
}
