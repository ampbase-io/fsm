package fsm

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

// pastRetention is an age comfortably beyond the default 7-day ArchiveRetention, so a run aged by
// it reads as reclaimable in every archive test.
const pastRetention = 8 * 24 * time.Hour

// ageRun rewrites a completed run's completion timestamp to age in the past, so a pass reads it as
// past retention without the test waiting out real time.
func ageRun(t *testing.T, s *objectStore, version ulid.ULID, age time.Duration) {
	t.Helper()
	if _, err := s.casManifest(context.Background(), version, func(m *fsmv1.RunManifest) error {
		m.CompletedAt = time.Now().Add(-age).Unix()
		return nil
	}); err != nil {
		t.Fatalf("failed to age run %s: %v", version, err)
	}
}

// objectGone reports whether the key is absent, failing the test on any error other than not-found.
func objectGone(t *testing.T, s *objectStore, key string) bool {
	t.Helper()
	switch _, _, err := s.getObject(context.Background(), key); {
	case err == nil:
		return false
	case errors.Is(err, ErrFsmNotFound):
		return true
	default:
		t.Fatalf("unexpected error reading %s: %v", key, err)
		return false
	}
}

func TestFinishSetsCompletedAt(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)

	run := startRun(t, s, "arch-completed-at")
	before := time.Now().Unix()
	finishRun(t, s, run)

	// completed_at is the archive loop's retention clock; a finish that does not set it leaves the
	// run unreapable forever.
	if got := mustManifest(t, s, run.StartVersion).GetCompletedAt(); got < before {
		t.Fatalf("expected completed_at set at finish (>= %d), got %d", before, got)
	}
}

func TestArchiveReapsCompletedRun(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "arch-reap")
	if err := s.writeChild(ctx, run.StartVersion, ulid.Make()); err != nil {
		t.Fatalf("failed to write child: %v", err)
	}
	finishRun(t, s, run)
	plantSentinel(t, s, run.StartVersion, "inert")
	ageRun(t, s, run.StartVersion, pastRetention)

	eventPrefix := s.eventPrefix(run.ID, run.Action, run.StartVersion)
	if keys, _ := s.listKeys(ctx, eventPrefix); len(keys) == 0 {
		t.Fatal("expected event objects before the pass")
	}

	s.runArchive(ctx)

	// Transient objects reclaimed.
	if _, _, err := s.getManifest(ctx, run.StartVersion); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected the manifest deleted, got %v", err)
	}
	if keys, _ := s.listKeys(ctx, eventPrefix); len(keys) != 0 {
		t.Fatalf("expected events deleted, %d remain", len(keys))
	}
	if children, _ := s.listChildren(ctx, run.StartVersion); len(children) != 0 {
		t.Fatalf("expected children deleted, %d remain", len(children))
	}
	if !objectGone(t, s, s.cancelKey(run.StartVersion)) {
		t.Fatal("expected the cancel sentinel deleted")
	}

	// Durable record survives.
	if _, err := s.History(ctx, run.StartVersion); err != nil {
		t.Fatalf("history must survive archival, got %v", err)
	}
	runs, err := s.Runs(ctx, run.TypeName, run.ID)
	if err != nil {
		t.Fatalf("Runs failed: %v", err)
	}
	if len(runs) != 1 || runs[0] != run.StartVersion {
		t.Fatalf("index must survive archival, got %v", runs)
	}
}

func TestArchiveKeepsWithinRetentionAndActive(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	// Completed but fresh: completed_at is now, within the default 7-day retention.
	fresh := startRun(t, s, "arch-fresh")
	finishRun(t, s, fresh)

	// Active: never finished, so still PENDING and still holding its resource lock.
	active := startRun(t, s, "arch-active")

	s.runArchive(ctx)

	if _, _, err := s.getManifest(ctx, fresh.StartVersion); err != nil {
		t.Fatalf("a within-retention run must be untouched, got %v", err)
	}
	if _, _, err := s.getManifest(ctx, active.StartVersion); err != nil {
		t.Fatalf("an active run must be untouched, got %v", err)
	}
	if objectGone(t, s, runLockKey(s, active)) {
		t.Fatal("an active run's lock must be untouched")
	}
}

func TestArchiveResumesPartialReap(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "arch-partial")
	finishRun(t, s, run)
	ageRun(t, s, run.StartVersion, pastRetention)

	// Simulate a crash after the events were deleted but before the manifest. Because the manifest
	// is deleted last, the run is still findable and the next pass finishes the reap.
	if err := s.deletePrefix(ctx, s.eventPrefix(run.ID, run.Action, run.StartVersion)); err != nil {
		t.Fatalf("failed to pre-delete events: %v", err)
	}

	s.runArchive(ctx)

	if _, _, err := s.getManifest(ctx, run.StartVersion); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected the resumed pass to delete the manifest, got %v", err)
	}
	if _, err := s.History(ctx, run.StartVersion); err != nil {
		t.Fatalf("history must survive a resumed reap, got %v", err)
	}

	// A repeat pass over the already-reaped run is a clean no-op.
	s.runArchive(ctx)
}

func TestArchiveDeletesManifestLast(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "arch-order")
	if err := s.writeChild(ctx, run.StartVersion, ulid.Make()); err != nil {
		t.Fatalf("failed to write child: %v", err)
	}
	finishRun(t, s, run)
	plantSentinel(t, s, run.StartVersion, "inert")
	ageRun(t, s, run.StartVersion, pastRetention)

	// Fail the manifest delete: a reap that crashes at its last step. Its earlier deletes still
	// ran, proving the manifest is deleted after the events, children, and cancel sentinel.
	h.fake.setFailDelete(s.manifestKey(run.StartVersion))
	if err := s.reapRun(ctx, run.StartVersion, mustManifest(t, s, run.StartVersion)); err == nil {
		t.Fatal("expected reapRun to fail when the manifest delete fails")
	}

	if keys, _ := s.listKeys(ctx, s.eventPrefix(run.ID, run.Action, run.StartVersion)); len(keys) != 0 {
		t.Fatalf("events must be deleted before the manifest, %d remain", len(keys))
	}
	if children, _ := s.listChildren(ctx, run.StartVersion); len(children) != 0 {
		t.Fatalf("children must be deleted before the manifest, %d remain", len(children))
	}
	if !objectGone(t, s, s.cancelKey(run.StartVersion)) {
		t.Fatal("the cancel sentinel must be deleted before the manifest")
	}
	if _, _, err := s.getManifest(ctx, run.StartVersion); err != nil {
		t.Fatalf("the manifest must still exist after its delete failed, got %v", err)
	}

	// Clearing the fault lets the next pass finish the reap.
	h.fake.setFailDelete("")
	s.runArchive(ctx)
	if _, _, err := s.getManifest(ctx, run.StartVersion); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected the manifest reaped once the fault cleared, got %v", err)
	}
	if _, err := s.History(ctx, run.StartVersion); err != nil {
		t.Fatalf("history must survive, got %v", err)
	}
}

func TestArchiveRepairsMissingHistory(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "arch-repair")
	finishRun(t, s, run)
	ageRun(t, s, run.StartVersion, pastRetention)

	// Simulate a finish that crashed after the manifest flip but before the history write.
	histKey := s.historyKey(ulid.Time(run.StartVersion.Time()).Format(time.DateOnly), run.StartVersion)
	if err := s.deleteObject(ctx, histKey); err != nil {
		t.Fatalf("failed to remove history: %v", err)
	}
	if _, err := s.History(ctx, run.StartVersion); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected history gone before the pass, got %v", err)
	}

	s.runArchive(ctx)

	// History is reconstructed from the manifest and the FINISH event, then the run is reaped.
	hist, err := s.History(ctx, run.StartVersion)
	if err != nil {
		t.Fatalf("expected history reconstructed, got %v", err)
	}
	if hist.GetLastEvent().GetType() != fsmv1.EventType_EVENT_TYPE_FINISH {
		t.Fatalf("reconstructed history missing the finish event, got %v", hist.GetLastEvent().GetType())
	}
	if _, _, err := s.getManifest(ctx, run.StartVersion); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected the manifest reaped after repair, got %v", err)
	}
}

func TestArchiveSkipsUnreconstructableHistory(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "arch-norepair")
	finishRun(t, s, run)
	ageRun(t, s, run.StartVersion, pastRetention)

	// Both the history record and the events (including the FINISH event) are gone, so the record
	// cannot be rebuilt: the run must be left intact rather than deleted.
	histKey := s.historyKey(ulid.Time(run.StartVersion.Time()).Format(time.DateOnly), run.StartVersion)
	if err := s.deleteObject(ctx, histKey); err != nil {
		t.Fatalf("failed to remove history: %v", err)
	}
	if err := s.deletePrefix(ctx, s.eventPrefix(run.ID, run.Action, run.StartVersion)); err != nil {
		t.Fatalf("failed to remove events: %v", err)
	}

	s.runArchive(ctx)

	if _, _, err := s.getManifest(ctx, run.StartVersion); err != nil {
		t.Fatalf("a run whose history cannot be rebuilt must not be deleted, got %v", err)
	}
	if _, err := s.History(ctx, run.StartVersion); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected history to remain absent, got %v", err)
	}
}

func TestArchivedFailedRunPreservesError(t *testing.T) {
	h := newLeaseHarness(t)
	a := h.store("node-a", 10*time.Second)
	b := h.store("node-b", 10*time.Second)
	ctx := context.Background()

	// node-a starts and finishes a run with a halt error; the FINISH event and manifest both
	// record the cause.
	run := startRun(t, a, "arch-failed")
	run.fsmErr = RunErr{Err: errors.New("boom"), State: "exploding"}
	if _, err := a.Append(ctx, run, finishEvent(run, "exploding"), nil); err != nil {
		t.Fatalf("failed to finish run with error: %v", err)
	}
	ageRun(t, a, run.StartVersion, pastRetention)

	a.runArchive(ctx)
	if _, _, err := a.getManifest(ctx, run.StartVersion); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected the run reaped, got %v", err)
	}

	// A peer that never ran the finish (no local finish state, manifest gone) must still observe
	// the run's error through the history fallback — otherwise archival turns a failure into a
	// silent success.
	switch err := b.WaitRun(ctx, run.StartVersion); {
	case err == nil:
		t.Fatal("archived failed run reported success; its error was lost when the manifest was reaped")
	case err.Error() != "boom":
		t.Fatalf("expected the run's error preserved through archival, got %v", err)
	}
}

func TestArchiveConcurrentPassesAreSafe(t *testing.T) {
	h := newLeaseHarness(t)
	a := h.store("node-a", 10*time.Second)
	b := h.store("node-b", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, a, "arch-conc")
	finishRun(t, a, run)
	ageRun(t, a, run.StartVersion, pastRetention)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.runArchive(ctx) }()
	go func() { defer wg.Done(); b.runArchive(ctx) }()
	wg.Wait()

	if _, _, err := a.getManifest(ctx, run.StartVersion); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected the run reaped, got %v", err)
	}
	if _, err := a.History(ctx, run.StartVersion); err != nil {
		t.Fatalf("history must survive concurrent reaps, got %v", err)
	}
}

func TestArchiveLoopConsumesSignal(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "arch-loop")
	finishRun(t, s, run)
	ageRun(t, s, run.StartVersion, pastRetention)

	// Waking the loop over archiveCh must run a pass; the send blocks until the loop receives.
	s.archiveCh <- struct{}{}

	deadline := time.After(5 * time.Second)
	for {
		if _, _, err := s.getManifest(ctx, run.StartVersion); errors.Is(err, ErrFsmNotFound) {
			return
		}
		select {
		case <-deadline:
			t.Fatal("archive loop did not reap after the signal")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestArchiveDisabledStartsNoLoop(t *testing.T) {
	bucket, url, _ := startFakeS3(t)
	s, err := newObjectStore(context.Background(), logrus.New(), &ObjectStorageConfig{
		Bucket:          bucket,
		Endpoint:        url,
		Region:          "auto",
		ArchiveDisabled: true,
	}, "node-a", nil, nil)
	if err != nil {
		t.Fatalf("failed to create object store: %v", err)
	}

	if s.archiveCh != nil || s.archiveDone != nil || s.archiveCancel != nil {
		t.Fatal("no archive loop state should be set when the loop is disabled")
	}
	// Close must not block waiting on a loop that never started.
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
