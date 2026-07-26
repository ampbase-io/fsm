package fsm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

// archive is the object backend's background cleanup loop, the counterpart to boltStore.archive.
// A completed run's durable record lives in history/ and index/; its manifest, events, children,
// and cancel sentinel are transient working state this loop reclaims once the run has aged past
// ArchiveRetention (see the RFC's "Archive and History" section). Deletes are idempotent, so
// every node running the pass concurrently is safe — a redundant scan, never a corrupted one.
//
// The pass runs on a long jittered interval and exits when ctx is canceled (Close). archiveCh
// lets a test wake the loop between intervals; a test asserting on the effect drives runArchive
// directly.
func (s *objectStore) archive(ctx context.Context) {
	defer close(s.archiveDone)

	timer := time.NewTimer(withJitter(s.cfg.archiveInterval()))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.archiveCh:
			s.runArchive(ctx)
		case <-timer.C:
			s.runArchive(ctx)
			timer.Reset(withJitter(s.cfg.archiveInterval()))
		}
	}
}

// runArchive performs one cleanup pass: find the completed runs past retention and reap each,
// scanFanout at a time. A per-run failure is logged and left for the next pass, so one
// unreapable run never stalls the rest.
func (s *objectStore) runArchive(ctx context.Context) {
	entries, err := s.archivableRuns(ctx)
	switch {
	case errors.Is(err, context.Canceled):
		return
	case err != nil:
		s.logger.WithError(err).Error("archive: failed to scan for archivable runs")
		return
	case len(entries) == 0:
		return
	}

	s.logger.WithField("count", len(entries)).Info("archive: reclaiming completed runs past retention")

	var (
		sem = make(chan struct{}, scanFanout)
		wg  sync.WaitGroup
	)
	for _, entry := range entries {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.reapRun(ctx, entry.version, entry.manifest); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.WithError(err).WithField("run_version", entry.version.String()).Error("archive: failed to reap run")
			}
		}()
	}
	wg.Wait()
}

// reapEntry pairs a run selected for reaping with the terminal manifest the scan already read, so
// reapRun works from it instead of GETting the manifest a second time in the same pass.
type reapEntry struct {
	version ulid.ULID

	manifest *fsmv1.RunManifest
}

// archivableRuns lists the run manifests and returns an entry for each run that is terminal and
// aged past retention — resolving each candidate scanFanout at a time (the listing itself is
// keys-only), then compacting away the misses, the shape scanLocks uses.
//
// This re-GETs active and within-retention manifests every pass — acceptable for an infrequent
// background loop. Scale lever if it profiles hot: a finished/<completion_ms>/<run_version>
// staging marker at FINISH would make the scan time-ordered and skip the manifest GET.
func (s *objectStore) archivableRuns(ctx context.Context) ([]reapEntry, error) {
	keys, err := s.listKeys(ctx, s.runsPrefix())
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().Add(-s.cfg.archiveRetention()).Unix()
	var (
		resolved = make([]*reapEntry, len(keys))
		sem      = make(chan struct{}, scanFanout)
		wg       sync.WaitGroup
	)
	for i, key := range keys {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			resolved[i] = s.archivable(ctx, key, cutoff)
		}()
	}
	wg.Wait()

	entries := make([]reapEntry, 0, len(keys))
	for _, entry := range resolved {
		if entry != nil {
			entries = append(entries, *entry)
		}
	}
	return entries, nil
}

// archivable resolves one manifest key to a reapEntry when its run is terminal and completed at or
// before the retention cutoff, or nil when the run should be left alone. A manifest already gone
// (raced by another node), still running, or unreadable is skipped — a transient read error is
// retried next pass, so the loop never selects a run for deletion off a read it could not complete.
// A terminal manifest with no completion timestamp cannot be aged, so it too waits for a later pass
// rather than being deleted on an unknown age.
func (s *objectStore) archivable(ctx context.Context, key string, cutoff int64) *reapEntry {
	version, err := versionFromKey(key)
	if err != nil {
		s.logger.WithError(err).WithField("key", key).Warn("archive: malformed manifest key")
		return nil
	}

	manifest, _, err := s.getManifest(ctx, version)
	switch {
	case errors.Is(err, ErrFsmNotFound), errors.Is(err, context.Canceled):
		return nil
	case err != nil:
		s.logger.WithError(err).WithField("run_version", version.String()).Error("archive: failed to read manifest")
		return nil
	}
	if !manifestTerminal(manifest) {
		return nil
	}
	if completedAt := manifest.GetCompletedAt(); completedAt == 0 || completedAt > cutoff {
		return nil
	}
	return &reapEntry{version: version, manifest: manifest}
}

// reapRun reclaims a single completed run's transient objects, working from the manifest the scan
// already read. The ordering is crash-safe and idempotent: history is verified (repaired from the
// manifest if a crashed finish left it missing) before anything is deleted, and the manifest is
// deleted last. A run whose history is missing and cannot be reconstructed is skipped, never
// deleted.
func (s *objectStore) reapRun(ctx context.Context, version ulid.ULID, manifest *fsmv1.RunManifest) error {
	if err := s.ensureHistory(ctx, version, manifest); err != nil {
		return err
	}

	// Delete the transient objects: the events (audit trail), the children pointers this run owns
	// as a parent, and the now-inert cancel sentinel (closing the cancel-sentinel GC gap).
	if err := s.deletePrefix(ctx, s.eventPrefix(manifest.GetResourceId(), manifest.GetAction(), version)); err != nil {
		return err
	}
	if err := s.deletePrefix(ctx, s.childPrefix(version)); err != nil {
		return err
	}
	if err := s.deleteObject(ctx, s.cancelKey(version)); err != nil {
		return err
	}

	// The manifest is what the scan finds the run by, so delete it last: a crash before this point
	// leaves a terminal manifest the next pass re-reaps, never orphaned events.
	return s.deleteObject(ctx, s.manifestKey(version))
}

// ensureHistory verifies the run's durable history record exists, reconstructing it from the
// manifest and the FINISH event it points at when a finish that crashed after the manifest flip
// but before the history write left it missing (the RFC's history-verify step, which repairs that
// window for free). It errors only on a storage failure or when history is missing and cannot be
// rebuilt (the FINISH event is gone too), so the caller skips a run it cannot back up rather than
// deleting it.
func (s *objectStore) ensureHistory(ctx context.Context, version ulid.ULID, manifest *fsmv1.RunManifest) error {
	switch _, err := s.readHistory(ctx, version); {
	case err == nil:
		return nil // already durable
	case !errors.Is(err, ErrFsmNotFound):
		return err
	}

	finishEvent, err := s.finishEventFromManifest(ctx, manifest)
	if err != nil {
		return fmt.Errorf("reconstruct history for %s: %w", version, err)
	}
	return s.writeHistory(ctx, version, historyFromManifest(manifest, finishEvent))
}

// finishEventFromManifest reads the FINISH event the terminal manifest points at, for history
// reconstruction. A manifest with no end_event_key, or whose event object is already gone, cannot
// be backed up.
func (s *objectStore) finishEventFromManifest(ctx context.Context, manifest *fsmv1.RunManifest) (*fsmv1.StateEvent, error) {
	endKey := manifest.GetEndEventKey()
	if len(endKey) == 0 {
		return nil, errors.New("manifest has no end event key")
	}

	body, _, err := s.getObject(ctx, string(endKey))
	if err != nil {
		return nil, fmt.Errorf("read finish event %s: %w", endKey, err)
	}

	var event fsmv1.StateEvent
	if err := proto.Unmarshal(body, &event); err != nil {
		return nil, fmt.Errorf("unmarshal finish event %s: %w", endKey, err)
	}
	return &event, nil
}
