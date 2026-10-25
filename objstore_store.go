package fsm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/cenkalti/backoff/v4"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
)

// maxManifestCASRetries bounds re-read-and-retry loops on manifest If-Match failures. In
// practice contention on a single run's manifest is near-zero because one goroutine drives a
// run sequentially; the bound exists for Phase 4, where lease takeovers introduce competitors.
const maxManifestCASRetries = 10

var _ Store = (*objectStore)(nil)

func (s *objectStore) manifestKey(runVersion ulid.ULID) string {
	return s.key("runs", runVersion.String())
}

// runsPrefix lists every run manifest; the archive loop scans it for completed runs to reclaim.
func (s *objectStore) runsPrefix() string {
	return s.key("runs") + "/"
}

// lockedRun is the run a manifest's lock key is built from: the identity and the two fields the
// key depends on, without the state and outcome runFromManifest reconstructs.
func lockedRun(version ulid.ULID, m *fsmv1.RunManifest) Run {
	return Run{
		ID:           m.GetResourceId(),
		StartVersion: version,
		Action:       m.GetAction(),
		TypeName:     m.GetResourceType(),
		Queue:        m.GetQueue(),
		Exclusive:    m.GetExclusive(),
	}
}

// lockKey is the run's resource lock: locks/<type>/<id>/<action>, which only one run holds at a
// time, plus the run version for a run that stacks behind it (Run.stacks).
func (s *objectStore) lockKey(run Run) string {
	resource := s.key("locks", escapeSegment(run.TypeName), escapeSegment(run.ID), escapeSegment(run.Action))
	if !run.stacks() {
		return resource
	}
	return resource + "/" + run.StartVersion.String()
}

func (s *objectStore) lockPrefix(resourceType string) string {
	return s.key("locks", escapeSegment(resourceType)) + "/"
}

func (s *objectStore) locksPrefix() string {
	return s.key("locks") + "/"
}

// lockResourcePrefix narrows a lock listing to a single resource. The trailing slash keeps ids
// that are prefixes of one another (machine-1, machine-10) from matching each other's locks.
func (s *objectStore) lockResourcePrefix(resourceType, resourceID string) string {
	return s.key("locks", escapeSegment(resourceType), escapeSegment(resourceID)) + "/"
}

// getManifest reads a run manifest with a consistent read, returning the manifest and the ETag
// to CAS against.
func (s *objectStore) getManifest(ctx context.Context, runVersion ulid.ULID) (*fsmv1.RunManifest, string, error) {
	body, etag, err := s.getObject(ctx, s.manifestKey(runVersion))
	if err != nil {
		return nil, "", err
	}

	var manifest fsmv1.RunManifest
	if err := proto.Unmarshal(body, &manifest); err != nil {
		return nil, "", fmt.Errorf("unmarshal manifest %s: %w", runVersion, err)
	}
	return &manifest, etag, nil
}

// casManifest applies mutate to the manifest under an If-Match compare-and-swap, re-reading and
// retrying on concurrent modification, so mutate always observes fresh state. A mutate error
// (a tripped fence, a lost claim race) aborts the CAS and is returned as-is. It returns the
// manifest as written.
//
// The first attempt CASes against the manifest this node last wrote (cachedManifest), so a run's
// steady state is one conditional PUT per checkpoint rather than a read and a write. Nothing is
// taken on trust: a write by anyone else moves the ETag, so the CAS fails its precondition and
// the retry reads the fresh manifest and re-runs mutate against it — which is how a lease taken
// over still trips checkFence. A node that has not written since it claimed the run (a resume)
// has no cached tag and reads once, as before.
func (s *objectStore) casManifest(ctx context.Context, runVersion ulid.ULID, mutate func(*fsmv1.RunManifest) error) (*fsmv1.RunManifest, error) {
	key := s.manifestKey(runVersion)
	var updated *fsmv1.RunManifest
	op := func() error {
		manifest, etag, cached := s.cachedManifest(runVersion)
		if !cached {
			var err error
			if manifest, etag, err = s.getManifest(ctx, runVersion); err != nil {
				return backoff.Permanent(err)
			}
		}

		if err := mutate(manifest); err != nil {
			return backoff.Permanent(err)
		}
		manifest.UpdatedAt = time.Now().Unix()

		body, err := proto.Marshal(manifest)
		if err != nil {
			return backoff.Permanent(fmt.Errorf("marshal manifest %s: %w", runVersion, err))
		}

		switch written, err := s.putIfMatch(ctx, key, body, etag); {
		case err == nil:
			s.cacheManifest(runVersion, manifest, written)
			updated = manifest
			return nil
		case errors.Is(err, errEtagMismatch):
			s.forgetManifest(runVersion)
			s.instruments.casRetry(ctx, "manifest")
			s.logger.DebugContext(ctx, "manifest changed concurrently, retrying", "key", key)
			return err
		default:
			return backoff.Permanent(err)
		}
	}
	if err := backoff.Retry(op, retryBackoff(ctx, maxManifestCASRetries)); err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *objectStore) Start(ctx context.Context, run Run, event *fsmv1.StateEvent, start *startRecord) (ulid.ULID, error) {
	return s.record(ctx, run, event, start)
}

func (s *objectStore) Append(ctx context.Context, run Run, event *fsmv1.StateEvent) (ulid.ULID, error) {
	return s.record(ctx, run, event, nil)
}

// record writes an event object and updates the run manifest according to the event type. It
// is the object storage implementation of the single mutation path all run state flows through;
// start is set for START only.
func (s *objectStore) record(ctx context.Context, run Run, event *fsmv1.StateEvent, start *startRecord) (ulid.ULID, error) {
	// Persistence must complete even when the run itself is being canceled — the CANCEL and
	// FINISH events are appended from an already-canceled run context, and BoltDB's local
	// writes are likewise not interruptible. Values (e.g. trace context) are preserved.
	ctx = context.WithoutCancel(ctx)

	if start == nil && event.GetType() == fsmv1.EventType_EVENT_TYPE_START {
		return ulid.ULID{}, errors.New("start record must be set")
	}

	if run.StartVersion.Compare(ulid.ULID{}) == 0 {
		return ulid.ULID{}, errors.New("runVersion must be set")
	}
	runVersionBytes, err := run.StartVersion.MarshalText()
	if err != nil {
		return ulid.ULID{}, err
	}
	event.RunVersion = runVersionBytes

	// The event version is derived once, before any write attempt, so retries inside this call
	// re-put the same key and observe create-if-absent semantics.
	eventVersion := ulid.Make()
	eventKey := s.eventKey(event.GetId(), event.GetAction(), run.StartVersion, eventVersion)

	eventBytes, err := proto.Marshal(event)
	if err != nil {
		return ulid.ULID{}, err
	}

	switch event.GetType() {
	case fsmv1.EventType_EVENT_TYPE_START:
		err = s.appendStart(ctx, run, event, start, eventKey, eventBytes)
	case fsmv1.EventType_EVENT_TYPE_ERROR,
		fsmv1.EventType_EVENT_TYPE_COMPLETE,
		fsmv1.EventType_EVENT_TYPE_CANCEL:
		err = s.appendMidRun(ctx, run, event, eventKey, eventBytes)
	case fsmv1.EventType_EVENT_TYPE_FINISH:
		err = s.appendFinish(ctx, run, event, eventKey, eventBytes)
	default:
		err = fmt.Errorf("%T: %w", event.Type, errInvalidEventType)
	}
	switch {
	case errors.Is(err, errRunStarted):
		return ulid.ULID{}, err
	case errors.Is(err, ErrLeaseLost):
		// Expected control flow after a takeover, not a storage failure.
		s.logger.WarnContext(ctx, "append fenced, run no longer owned", "error", err)
		return ulid.ULID{}, err
	case err != nil:
		s.logger.ErrorContext(ctx, "failed to append event", "error", err)
		return ulid.ULID{}, err
	}

	// Publish after the durable write, skipping construction entirely under the no-op bus — this
	// runs on every transition. FINISH is the exception: fsm.run.done is emitted from inside the
	// finish cleanup (after the lock is deleted and history written), never here at the manifest
	// flip, or a released waiter could race the not-yet-deleted lock.
	if busIsLive(s.bus) {
		switch event.GetType() {
		case fsmv1.EventType_EVENT_TYPE_START:
			s.publishControl(subjectPending, fsmv1.RunEventKind_RUN_EVENT_KIND_PENDING, run.StartVersion, "")
		case fsmv1.EventType_EVENT_TYPE_COMPLETE,
			fsmv1.EventType_EVENT_TYPE_ERROR,
			fsmv1.EventType_EVENT_TYPE_CANCEL:
			s.publishTransition(run, event, eventVersion)
		}
	}

	return eventVersion, nil
}

// publishControl emits a control event — pending, done, cancel, or signal — carrying only
// the run identity (and, for a cancel, its cause). Its meaning is the kind and the subject it
// rides on. Best-effort per the EventBus contract; the durable log and sentinels are the source
// of truth.
func (s *objectStore) publishControl(subject string, kind fsmv1.RunEventKind, version ulid.ULID, cause string) {
	runVersion, _ := version.MarshalText() // a valid ULID never fails to marshal
	s.bus.Publish(subject, &fsmv1.RunEvent{
		Kind:       kind,
		RunVersion: runVersion,
		Error:      cause,
	})
}

// publishTransition emits a durable transition on the run's event stream, reusing the
// StateEvent's EventType so the live stream and the durable log speak one schema. Best-effort.
func (s *objectStore) publishTransition(run Run, event *fsmv1.StateEvent, eventVersion ulid.ULID) {
	runVersion, _ := run.StartVersion.MarshalText()
	eventBytes, _ := eventVersion.MarshalText()
	s.bus.Publish(eventSubject(run.StartVersion), &fsmv1.RunEvent{
		Kind:         fsmv1.RunEventKind_RUN_EVENT_KIND_TRANSITION,
		RunVersion:   runVersion,
		EventVersion: eventBytes,
		Type:         event.GetType(),
		ResourceType: event.GetResourceType(),
		ResourceId:   event.GetId(),
		Action:       event.GetAction(),
		State:        event.GetState(),
		Error:        event.GetError(),
	})
}

// lockOwner reads the run version a resource lock records as its holder.
func (s *objectStore) lockOwner(ctx context.Context, lockKey string) (ulid.ULID, error) {
	owner, _, err := s.getObject(ctx, lockKey)
	if err != nil {
		return ulid.ULID{}, fmt.Errorf("resource lock %s is held but unreadable: %w", lockKey, err)
	}
	var ownerVersion ulid.ULID
	if err := ownerVersion.UnmarshalText(owner); err != nil {
		return ulid.ULID{}, fmt.Errorf("resource lock %s has invalid owner: %w", lockKey, err)
	}
	return ownerVersion, nil
}

// acquireRunLock takes the resource lock. A lock held by a run whose manifest is already
// terminal is a crash (or in-flight cleanup) window between manifest completion and lock
// deletion: the cleanup is finished here and the acquire retried once, so a caller that
// restarts a resource right after waiting on its completion doesn't see a spurious
// AlreadyRunning.
func (s *objectStore) acquireRunLock(ctx context.Context, lockKey string, runVersionBytes []byte) error {
	switch _, err := s.putIfAbsent(ctx, lockKey, runVersionBytes); {
	case err == nil:
		return nil
	case !errors.Is(err, errPreconditionFailed):
		return err
	}

	ownerVersion, err := s.lockOwner(ctx, lockKey)
	if err != nil {
		return err
	}
	manifest, _, err := s.getManifest(ctx, ownerVersion)
	if err != nil || !manifestTerminal(manifest) {
		return &AlreadyRunningError{Version: ownerVersion}
	}

	if err := s.reapTerminalLock(ctx, lockKey); err != nil {
		return fmt.Errorf("failed to delete orphaned lock %s: %w", lockKey, err)
	}
	switch _, err := s.putIfAbsent(ctx, lockKey, runVersionBytes); {
	case err == nil:
		return nil
	case !errors.Is(err, errPreconditionFailed):
		return err
	}

	// Lost the re-acquire race to another starter.
	ownerVersion, err = s.lockOwner(ctx, lockKey)
	if err != nil {
		return err
	}
	return &AlreadyRunningError{Version: ownerVersion}
}

func (s *objectStore) appendStart(ctx context.Context, run Run, event *fsmv1.StateEvent, start *startRecord, eventKey string, eventBytes []byte) error {
	runVersionBytes, err := run.StartVersion.MarshalText()
	if err != nil {
		return err
	}

	// 1. Acquire the resource lock. A 412 means another run holds it.
	lockKey := s.lockKey(run)
	if err := s.acquireRunLock(ctx, lockKey, runVersionBytes); err != nil {
		return err
	}

	// 2. The three objects the manifest depends on none of, at once: the START event; the
	// per-resource index marker, which unlike the lock (deleted at FINISH) and the events
	// (deleted at archival) persists, so past runs stay enumerable; and the parent-child link,
	// which has to precede the manifest, since a failure after it would leave a live run its
	// parent can never find. Each is provisional on its own — a crash among them leaves a lock
	// the reaper deletes, an index marker no run claims, a link ActiveChildren skips, and an
	// event nothing reads — so losing any subset is no worse than losing the tail of them.
	indexKey := s.runIndexKey(event.GetResourceType(), event.GetId(), event.GetAction(), run.StartVersion)
	if err := writeAll(
		func() error { return s.putIdempotent(ctx, eventKey, eventBytes) },
		func() error { return s.putIdempotent(ctx, indexKey, nil) },
		func() error { return s.linkParent(ctx, run.Parent, run.StartVersion) },
	); err != nil {
		return err
	}

	// 3. Create the manifest, leased to this node: the run exists from here.
	manifest := s.startManifest(ctx, run, event, start, eventKey, runVersionBytes)
	return s.createRunManifest(ctx, run, lockKey, manifest, start.Unowned)
}

// startManifest materializes a new run's initial manifest. By default it is leased to this node
// from the start — the embedded start executes the run immediately and heartbeats from the first
// tick. An unowned START (the RPC ingress' persist-then-ack) leaves the lease empty so the claim
// loop distributes the run across the worker pool; its epoch is bumped from zero on the first
// claim.
func (s *objectStore) startManifest(ctx context.Context, run Run, event *fsmv1.StateEvent, start *startRecord, eventKey string, runVersionBytes []byte) *fsmv1.RunManifest {
	ownerNode := s.node
	leaseExpiry := time.Now().Add(s.cfg.leaseTimeout()).UnixMilli()
	leaseEpoch := int64(1)
	if start.Unowned {
		ownerNode = ""
		leaseExpiry = 0
		leaseEpoch = 0
	}

	manifest := &fsmv1.RunManifest{
		Status:         fsmv1.RunState_RUN_STATE_PENDING,
		ResourceType:   event.GetResourceType(),
		ResourceId:     event.GetId(),
		Action:         event.GetAction(),
		RunVersion:     runVersionBytes,
		StartEventKey:  []byte(eventKey),
		LatestEventKey: []byte(eventKey),
		EventCount:     1,
		Transitions:    start.Transitions,
		Resource:       start.Resource,
		Queue:          run.Queue,
		Exclusive:      run.Exclusive,
		Parent:         optionalVersionBytes(run.Parent),
		DelayUntil:     delayMillis(start.DelayUntil),
		RunAfter:       optionalVersionBytes(start.RunAfter),
		OwnerNode:      ownerNode,
		LeaseExpiry:    leaseExpiry,
		LeaseEpoch:     leaseEpoch,
		TraceContext:   map[string]string{},
		CreatedAt:      time.Now().Unix(),
		UpdatedAt:      time.Now().Unix(),
	}
	(propagation.TraceContext{}).Inject(ctx, propagation.MapCarrier(manifest.TraceContext))

	return manifest
}

// createRunManifest writes the run's initial manifest and tracks its lease, leaving the manifest
// it wrote as the CAS target of the run's first write — this node has the bytes, so that write
// needs no read. An unowned START carries no lease, so nothing is tracked or cached — the claim
// loop leases it on pickup. A manifest that already exists belongs to a crashed earlier attempt
// of this same run and is adopted instead.
func (s *objectStore) createRunManifest(ctx context.Context, run Run, lockKey string, manifest *fsmv1.RunManifest, unowned bool) error {
	body, err := proto.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal manifest %s: %w", run.StartVersion, err)
	}

	switch etag, err := s.putIfAbsent(ctx, s.manifestKey(run.StartVersion), body); {
	case err == nil:
		if !unowned {
			// A leased START is never admission-controlled — queued runs are persisted unowned so
			// the claim loop admits them (see Manager.start) — so it holds no queue-roster slot.
			s.trackLease(run.StartVersion, 1, "")
			s.cacheManifest(run.StartVersion, manifest, etag)
		}
		return nil
	case errors.Is(err, errPreconditionFailed):
		return s.adoptRunManifest(ctx, run, lockKey)
	default:
		return err
	}
}

// adoptRunManifest resumes a START whose earlier attempt already created the manifest; the
// existing manifest is authoritative. Its lease is adopted if this node still owns it — but
// never from a terminal manifest: a START retry arriving after the run finished must not
// re-execute it, and the lock the retry re-acquired is undone so the duplicate leaves no
// trace.
func (s *objectStore) adoptRunManifest(ctx context.Context, run Run, lockKey string) error {
	existing, _, err := s.getManifest(ctx, run.StartVersion)
	if err != nil {
		return err
	}

	if manifestTerminal(existing) {
		if err := s.deleteObject(ctx, lockKey); err != nil {
			s.logger.ErrorContext(ctx, "failed to delete lock of completed run", "error", err, "key", lockKey)
		}
		return &AlreadyRunningError{Version: run.StartVersion}
	}
	if existing.GetOwnerNode() != s.node {
		return ErrLeaseLost
	}

	s.trackLease(run.StartVersion, existing.GetLeaseEpoch(), admissionQueue(existing))
	return nil
}

// linkParent records the parent-child relationship for runs started with a parent.
func (s *objectStore) linkParent(ctx context.Context, parent, child ulid.ULID) error {
	if parent.Compare(ulid.ULID{}) == 0 {
		return nil
	}
	return s.writeChild(ctx, parent, child)
}

func (s *objectStore) appendMidRun(ctx context.Context, run Run, event *fsmv1.StateEvent, eventKey string, eventBytes []byte) error {
	// The local ownership guard keeps a fenced writer from creating orphan event objects in
	// the common case; the fence check inside the CAS is authoritative for the true race.
	epoch, ok := s.ownedEpoch(run.StartVersion)
	if !ok {
		return ErrLeaseLost
	}

	// A COMPLETE also prunes the consumed IDs whose markers this node has since deleted.
	pruned := s.deletedSignals(run.StartVersion)
	err := s.besideEvent(ctx, eventKey, eventBytes, func() error {
		_, err := s.casManifest(ctx, run.StartVersion, func(m *fsmv1.RunManifest) error {
			if err := s.checkFence(m, epoch); err != nil {
				return err
			}
			m.LatestEventKey = []byte(eventKey)
			m.EventCount++
			if m.Status == fsmv1.RunState_RUN_STATE_PENDING {
				m.Status = fsmv1.RunState_RUN_STATE_RUNNING
			}

			switch event.GetType() {
			case fsmv1.EventType_EVENT_TYPE_COMPLETE:
				m.CompletedStates = appendUniqueState(m.CompletedStates, event.GetState())
				if event.GetResponse() != nil {
					m.LatestResponse = event.GetResponse()
				}
				m.Iterations = recordIteration(m.Iterations, event)
				m.ConsumedSignals = append(withoutIDs(m.ConsumedSignals, pruned), event.GetConsumedSignals()...)
			case fsmv1.EventType_EVENT_TYPE_CANCEL:
				m.CompletedStates = appendUniqueState(m.CompletedStates, event.GetState())
				recordOutcome(m, event)
			case fsmv1.EventType_EVENT_TYPE_ERROR:
				m.RetryCount = event.GetRetryCount()
			}
			return nil
		})
		return err
	})
	if errors.Is(err, ErrLeaseLost) {
		s.dropLease(run.StartVersion)
	}
	if err != nil {
		return err
	}
	if event.GetType() == fsmv1.EventType_EVENT_TYPE_COMPLETE {
		s.forgetDeletedSignals(run.StartVersion, pruned)
	}
	s.deleteConsumedSignals(ctx, run.StartVersion, event.GetConsumedSignals())
	return nil
}

// besideEvent writes the run's event object with write — the manifest write it records — in
// flight beside it, and returns write's error. Only the FINISH event is ever read back on this
// backend (the history record names it), so a mid-run event is an audit object: it rides beside
// the manifest instead of ahead of it, and a failure to write one is logged rather than failing
// a transition the manifest recorded. A fenced writer could already leave behind an event its
// manifest never names, so no reader sees anything new.
func (s *objectStore) besideEvent(ctx context.Context, eventKey string, eventBytes []byte, write func() error) error {
	var (
		eventErr error
		wg       sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		eventErr = s.putIdempotent(ctx, eventKey, eventBytes)
	}()

	err := write()
	wg.Wait()
	if eventErr != nil {
		s.logger.ErrorContext(ctx, "failed to record a run's event", "error", eventErr, "key", eventKey)
	}
	return err
}

// dropFinishedMarkers deletes what a finished run no longer needs: its resource lock, and a
// canceled run's now-inert sentinel, which would otherwise leave the cancel/ prefix every
// heartbeat sweep lists carrying a week of finished cancels. Only a cancel outcome has a
// sentinel, so every other finish pays one delete. Failures are logged: a lock outliving its
// terminal manifest is reaped by the next lock scan, and a sentinel by the archive pass.
func (s *objectStore) dropFinishedMarkers(ctx context.Context, run Run, manifest *fsmv1.RunManifest) {
	lockKey := s.lockKey(lockedRun(run.StartVersion, manifest))
	if err := s.deleteObject(ctx, lockKey); err != nil {
		s.logger.ErrorContext(ctx, "failed to delete resource lock", "error", err, "key", lockKey)
	}

	if _, canceled := errors.AsType[*CancelError](run.fsmErr.Err); !canceled {
		return
	}
	if err := s.deleteObject(ctx, s.cancelKey(run.StartVersion)); err != nil {
		s.logger.ErrorContext(ctx, "failed to delete cancel sentinel", "error", err, versionAttr(run.StartVersion))
	}
}

// withoutIDs is ids with every one in drop removed.
func withoutIDs(ids, drop []string) []string {
	return slices.DeleteFunc(ids, func(id string) bool { return slices.Contains(drop, id) })
}

func (s *objectStore) appendFinish(ctx context.Context, run Run, event *fsmv1.StateEvent, eventKey string, eventBytes []byte) error {
	s.beginFinish(run.StartVersion)
	defer s.settleFinish(run.StartVersion)

	epoch, ok := s.ownedEpoch(run.StartVersion)
	if !ok {
		return ErrLeaseLost
	}

	if err := s.putIdempotent(ctx, eventKey, eventBytes); err != nil {
		return err
	}

	manifest, err := s.casManifest(ctx, run.StartVersion, func(m *fsmv1.RunManifest) error {
		if err := s.checkFence(m, epoch); err != nil {
			return err
		}
		// A settle read PENDING before it got here; SetRunning can have moved the run on since,
		// and the run is then executing under this same epoch.
		if run.unstarted && m.Status != fsmv1.RunState_RUN_STATE_PENDING {
			return errRunStarted
		}
		m.Status = fsmv1.RunState_RUN_STATE_COMPLETE
		m.EndEventKey = []byte(eventKey)
		m.LatestEventKey = []byte(eventKey)
		m.EventCount++
		// The retention clock the archive loop reads. Set once at the flip to terminal, never
		// moved by a later CAS (see completed_at in manifest.proto).
		if m.CompletedAt == 0 {
			m.CompletedAt = time.Now().Unix()
		}
		// LatestResponse is not set here: the last transition's COMPLETE already materialized it
		// (appendMidRun), so RunResult reads it from the terminal manifest, race-free and
		// cross-node. The FINISH event still carries that same response for the history record.
		if run.fsmErr.Err != nil {
			recordOutcome(m, event)
		}
		return nil
	})
	switch {
	case errors.Is(err, ErrFsmNotFound):
		s.logger.WarnContext(ctx, "manifest not found for finish event", versionAttr(run.StartVersion))
		return nil
	case errors.Is(err, ErrLeaseLost):
		s.dropLease(run.StartVersion)
		return err
	case err != nil:
		// errRunStarted included: the run keeps its lease, and the FINISH object just put is
		// named by no manifest, as a fenced owner's can be.
		return err
	}

	s.dropLease(run.StartVersion)

	// What the terminal manifest makes safe, at once: the markers the run no longer needs, and
	// the history record that outlives it. Neither waits on the other, and both land before the
	// done publish below, so a waiter released by it still observes a complete finish.
	if err := writeAll(
		func() error { s.dropFinishedMarkers(ctx, run, manifest); return nil },
		func() error { return s.writeHistory(ctx, run.StartVersion, historyFromManifest(manifest, event)) },
	); err != nil {
		return err
	}

	s.completeFinish(run.StartVersion, run.fsmErr)

	// Free the queue slot this run held, then wake peers so a run pending on this queue is admitted
	// at once instead of on the next periodic pass. Gated on admission-controlled runs — a
	// delayed/run-after run holds no roster slot, and gating the wakeup keeps an ordinary finish
	// from stampeding every node's claim loop. The publish follows the release so a woken peer
	// reads the already-freed slot.
	if releaseQueue := admissionQueue(manifest); releaseQueue != "" {
		if err := s.releaseQueued(ctx, releaseQueue, run.StartVersion); err != nil {
			s.logger.ErrorContext(ctx, "failed to release queue slot", "error", err, versionAttr(run.StartVersion), "queue", releaseQueue)
		}
		if busIsLive(s.bus) {
			s.publishControl(subjectPending, fsmv1.RunEventKind_RUN_EVENT_KIND_PENDING, run.StartVersion, "")
		}
	}

	// The finish cleanup is complete — lock deleted, history written, outcome recorded — so a
	// waiter released now observes everything fsm.run.done implies.
	if busIsLive(s.bus) {
		s.publishControl(doneSubject(run.StartVersion), fsmv1.RunEventKind_RUN_EVENT_KIND_DONE, run.StartVersion, "")
	}

	return nil
}

func appendUniqueState(states []string, state string) []string {
	for _, s := range states {
		if s == state {
			return states
		}
	}
	return append(states, state)
}

// activeEventFromManifest rebuilds the ActiveEvent representation used by the resume path and
// history records from the manifest's materialized fields.
func activeEventFromManifest(m *fsmv1.RunManifest) *fsmv1.ActiveEvent {
	return &fsmv1.ActiveEvent{
		StartEvent:   m.GetStartEventKey(),
		StartVersion: m.GetRunVersion(),
		EndEvent:     m.GetEndEventKey(),
		Action:       m.GetAction(),
		ResourceId:   m.GetResourceId(),
		Resource:     m.GetResource(),
		Transitions:  m.GetTransitions(),
		Options: &fsmv1.EventOptions{
			DelayUntil: m.GetDelayUntil(),
			RunAfter:   m.GetRunAfter(),
			Queue:      m.GetQueue(),
			Exclusive:  m.GetExclusive(),
			Parent:     m.GetParent(),
		},
		TraceContext: m.GetTraceContext(),
	}
}

// historyFromManifest builds the durable history record for a completed run from its manifest and
// its FINISH event. Both the finish path and the archive loop's crash-repair go through it, so the
// record the two write cannot drift.
func historyFromManifest(m *fsmv1.RunManifest, finishEvent *fsmv1.StateEvent) *fsmv1.HistoryEvent {
	return &fsmv1.HistoryEvent{
		ActiveEvent: activeEventFromManifest(m),
		LastEvent:   finishEvent,
	}
}

// manifestTerminal reports whether the manifest records a run that will make no further
// progress.
func manifestTerminal(m *fsmv1.RunManifest) bool {
	return m.GetStatus() == fsmv1.RunState_RUN_STATE_COMPLETE || m.GetStatus() == fsmv1.RunState_RUN_STATE_ARCHIVED
}

// admissionQueue returns the queue whose cluster-wide capacity gates this run, or "" when the run
// does not pass through a queue's shared limit — the manifest-side counterpart of
// admissionControlled, classifying through the same queueGated rule. A delay or run-after
// dependency takes precedence over the queue, so such a run executes locally on its owner and holds
// no queue-roster slot.
func admissionQueue(m *fsmv1.RunManifest) string {
	if !queueGated(m.GetQueue(), m.GetDelayUntil() != 0, len(m.GetRunAfter()) != 0) {
		return ""
	}
	return m.GetQueue()
}

// recordOutcome projects a stamped event's error triple onto the manifest: the one way the
// manifest learns how a run halted, so it and the event log never disagree.
func recordOutcome(m *fsmv1.RunManifest, event *fsmv1.StateEvent) {
	m.Error, m.HaltKind, m.ErrorState = event.GetError(), event.GetHaltKind(), event.GetErrorState()
}

// runFromManifest rebuilds a Run from the manifest's materialized fields. The resource alias is
// not recorded in the manifest; the Manager restores it from the registered FSM.
func runFromManifest(version ulid.ULID, m *fsmv1.RunManifest) Run {
	var parent ulid.ULID
	if parentBytes := m.GetParent(); parentBytes != nil {
		if err := parent.UnmarshalText(parentBytes); err != nil {
			parent = ulid.ULID{}
		}
	}
	return Run{
		ID:           m.GetResourceId(),
		StartVersion: version,
		Action:       m.GetAction(),
		CurrentState: nextState(m.GetTransitions(), m.GetCompletedStates(), m.GetIterations()),
		TypeName:     m.GetResourceType(),
		Queue:        m.GetQueue(),
		Exclusive:    m.GetExclusive(),
		Parent:       parent,
		fsmErr:       recordedRunErr(m),
	}
}

// lockEntry pairs an active resource lock with the manifest of the run holding it. A scanHeld
// entry may carry no manifest: the run holds the resource, and nothing about its state is
// readable yet.
type lockEntry struct {
	action string

	version ulid.ULID

	manifest *fsmv1.RunManifest
}

// lockScan is what a scan makes of a held lock it cannot resolve to a run — a START that has
// written its lock but not yet its manifest, or a lock that will not read.
type lockScan int

const (
	// scanResumable skips it: the run's state is what the resume and claim paths came for, and
	// a later pass retries once the manifest is there.
	scanResumable lockScan = iota

	// scanHeld reports it, manifest or not. A lock left out of a query reads as an idle
	// resource, while a Start on that resource is refused for as long as the lock exists.
	scanHeld
)

// scanFanout bounds the concurrent object storage operations of a lock scan or a lease pass;
// a serial pass over many runs could outrun the interval that scheduled it.
const scanFanout = 8

// scanLocks lists resource locks under prefix and resolves each to its owning run's manifest
// (keys-only listing plus one lock GET and one manifest GET per active run, scanFanout at a
// time — event bodies are never read, per the RFC's resumability contract). Crash windows are
// handled inline: a lock whose manifest is terminal is deleted, and what a lock with no manifest
// means to the caller is the scan mode's to say.
func (s *objectStore) scanLocks(ctx context.Context, prefix string, scan lockScan) ([]lockEntry, error) {
	lockKeys, err := s.listKeys(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var (
		resolved = make([]*lockEntry, len(lockKeys))
		errs     = make([]error, len(lockKeys))
		sem      = make(chan struct{}, scanFanout)
		wg       sync.WaitGroup
	)
	for i, lockKey := range lockKeys {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			resolved[i], errs[i] = s.resolveLock(ctx, lockKey, scan)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	entries := make([]lockEntry, 0, len(resolved))
	for _, entry := range resolved {
		if entry == nil {
			continue
		}
		entries = append(entries, *entry)
	}
	return entries, nil
}

// resolveLock resolves a single lock key to its owning run's manifest. Skips — a malformed key,
// a lock already released, terminal-manifest cleanup — return (nil, nil); a held lock whose run
// is not readable is the scan mode's to interpret.
func (s *objectStore) resolveLock(ctx context.Context, lockKey string, scan lockScan) (*lockEntry, error) {
	logger := s.logger.With("key", lockKey)

	// locks/<type>/<id>/<action>[/<run_version>]
	segments := strings.Split(strings.TrimPrefix(lockKey, s.locksPrefix()), "/")
	if len(segments) < 3 {
		logger.WarnContext(ctx, "malformed lock key")
		return nil, nil
	}
	action, err := unescapeSegment(segments[2])
	if err != nil {
		logger.WarnContext(ctx, "malformed lock key", "error", err)
		return nil, nil
	}

	version, err := s.lockOwner(ctx, lockKey)
	switch {
	case errors.Is(err, ErrFsmNotFound):
		// Released between the listing and the read: the run finished, so nothing holds the
		// resource any more and there is nothing to report.
		logger.DebugContext(ctx, "lock released during the scan")
		return nil, nil
	case err != nil && scan == scanHeld:
		return nil, err
	case err != nil:
		logger.ErrorContext(ctx, "failed to resolve lock owner", "error", err)
		return nil, nil
	}

	manifest, _, err := s.getManifest(ctx, version)
	switch {
	case errors.Is(err, ErrFsmNotFound):
		// The window between lock acquisition and manifest creation: an in-flight START, which
		// refuses a competing Start from the moment its lock lands, or a crash inside one. A
		// lock old enough that no START can still be writing it (the run version carries its
		// creation time) is reaped.
		if time.Since(ulid.Time(version.Time())) > 2*s.cfg.leaseTimeout() {
			logger.WarnContext(ctx, "stale lock with no manifest, removing")
			if err := s.deleteObject(ctx, lockKey); err != nil {
				logger.ErrorContext(ctx, "failed to delete stale lock", "error", err)
			}
			return nil, nil
		}
		if scan == scanResumable {
			logger.WarnContext(ctx, "lock held but manifest missing, skipping")
			return nil, nil
		}
		logger.DebugContext(ctx, "lock held but manifest missing, reporting the lock")
		return &lockEntry{action: action, version: version}, nil
	case err != nil:
		return nil, err
	}

	if manifestTerminal(manifest) {
		if err := s.reapTerminalLock(ctx, lockKey); err != nil {
			logger.ErrorContext(ctx, "failed to delete orphaned lock", "error", err)
		}
		return nil, nil
	}

	return &lockEntry{action: action, version: version, manifest: manifest}, nil
}

// reapTerminalLock finishes the cleanup of a completed run's lock — the crash window between
// manifest completion and lock deletion.
func (s *objectStore) reapTerminalLock(ctx context.Context, lockKey string) error {
	s.logger.InfoContext(ctx, "completed run still holds its lock, removing", "key", lockKey)
	return s.deleteObject(ctx, lockKey)
}

// Active returns every incomplete run of one FSM, enumerated from the locks/ prefix. The Manager
// resumes through claimRuns on this backend, so Active is the query form of the same scan: every
// active run, whether or not this node may take it — and, being a resume path, only runs whose
// recorded state it can hand back.
func (s *objectStore) Active(ctx context.Context, key fsmKey) ([]*activeResource, error) {
	entries, err := s.scanLocks(ctx, s.lockPrefix(key.typeName), scanResumable)
	if err != nil {
		return nil, err
	}

	var active []*activeResource
	for _, e := range entries {
		if e.action != key.action {
			continue
		}
		active = append(active, manifestResource(e.version, e.manifest))
	}
	return active, nil
}

// manifestResource rebuilds the resume-path DTO from the manifest's materialized fields.
func manifestResource(version ulid.ULID, m *fsmv1.RunManifest) *activeResource {
	return &activeResource{
		version:              version,
		active:               activeEventFromManifest(m),
		completedTransitions: m.GetCompletedStates(),
		consumedSignals:      m.GetConsumedSignals(),
		iterations:           m.GetIterations(),
		response:             m.GetLatestResponse(),
		retryCount:           m.GetRetryCount(),
		fsmError:             recordedRunErr(m),
	}
}

// ActiveRuns answers from the resource's locks, and answers for every one of them: a caller
// deciding whether the resource is busy must never read it as idle while a lock on it would
// refuse a Start. A run whose START has not yet written its manifest is therefore reported with
// RUN_STATE_UNSPECIFIED — held, state not known yet.
func (s *objectStore) ActiveRuns(ctx context.Context, resourceType, resourceID string) (ActiveSet, error) {
	entries, err := s.scanLocks(ctx, s.lockResourcePrefix(resourceType, resourceID), scanHeld)
	if err != nil {
		return nil, err
	}

	active := ActiveSet{}
	for _, e := range entries {
		active[ActiveKey{Action: e.action, Version: e.version}] = e.manifest.GetStatus()
	}
	return active, nil
}

func (s *objectStore) ActiveChildren(ctx context.Context, parent ulid.ULID) ([]Run, error) {
	children, err := s.listChildren(ctx, parent)
	if err != nil {
		return nil, err
	}

	active := []Run{}
	for _, child := range children {
		manifest, _, err := s.getManifest(ctx, child)
		switch {
		case errors.Is(err, ErrFsmNotFound):
			continue
		case err != nil:
			return nil, err
		}
		if manifestTerminal(manifest) {
			continue
		}
		active = append(active, runFromManifest(child, manifest))
	}
	return active, nil
}

// startVisibilityWindow is how long a run version may exist with no manifest to read: a START
// takes the resource lock with its first write and creates the manifest last, so a version
// minted moments ago may be a run being written rather than one that never existed. A second is
// several times the conditional writes a START makes against a multi-region bucket, and is the
// longest a wait on a version that really is unknown waits to be told so.
const startVisibilityWindow = time.Second

// startInFlight reports whether a version is young enough that a START could still be writing
// its run. Neither the manifest nor the history record is readable in that window, so nothing
// but the version's own age tells a run that is landing from a run that does not exist.
func startInFlight(runVersion ulid.ULID) bool {
	return time.Since(ulid.Time(runVersion.Time())) < startVisibilityWindow
}

// WaitRun blocks until the run records a terminal state, polling its manifest under exponential
// backoff with the bus as a fast path: a fsm.run.done event wakes the wait immediately instead
// of on the next poll tick, and a live bus relaxes the poll floor to a rare insurance interval
// (waitBackoff). Outcomes of runs this process completed are answered from the in-process
// finished map, which preserves typed run errors; a missing manifest is answered from history,
// unless the run may be one a START is still writing (startVisibilityWindow). A terminal
// manifest whose finish is still being cleaned up by this process keeps the waiter polling
// until the outcome is recorded.
func (s *objectStore) WaitRun(ctx context.Context, runVersion ulid.ULID) error {
	// Subscribe before the first manifest read: a completion landing between the read and the
	// wait still wakes us, and one that landed before Subscribe returned is caught by that
	// first read.
	done, unsubscribe := subscribeWakeup(s.bus, doneSubject(runVersion), s.logger)
	defer unsubscribe()

	var outcome error
	terminal := func() (bool, error) {
		if runErr, state := s.localFinish(runVersion); state == finishDone {
			outcome = runErr.Err
			return true, nil
		}

		manifest, _, err := s.getManifest(ctx, runVersion)
		switch {
		case errors.Is(err, ErrFsmNotFound):
			outcome = historyOutcome(ctx, s, runVersion)
			if errors.Is(outcome, ErrFsmNotFound) && startInFlight(runVersion) {
				return false, nil
			}
			return true, nil
		case err != nil:
			return false, err
		}
		if !manifestTerminal(manifest) {
			return false, nil
		}

		// The terminal manifest is durable truth; localFinish decides atomically whether this
		// process's finish cleanup supplies a typed outcome, is still landing, or never ran
		// here (a run finished by another node).
		switch runErr, state := s.localFinish(runVersion); state {
		case finishDone:
			outcome = runErr.Err
			return true, nil
		case finishInFlight:
			return false, nil
		}
		outcome = recordedRunErr(manifest).Err
		return true, nil
	}

	b := s.waitBackoff(ctx)
	timer := stoppedTimer()
	defer timer.Stop()
	for {
		switch done, err := terminal(); {
		case err != nil:
			return err
		case done:
			return outcome
		}

		d := b.NextBackOff()
		if d == backoff.Stop { // the ctx-bound backoff stops only when ctx ends
			return ctx.Err()
		}
		timer.Reset(d)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		case <-done:
			if !timer.Stop() {
				<-timer.C
			}
			b.Reset() // the bus is delivering; tighten the next poll back to the floor
		}
	}
}

// ListActive lists every incomplete run for the admin API. A lock whose manifest is not written
// yet is skipped rather than listed as a row with no state, queue or current state in it.
func (s *objectStore) ListActive(ctx context.Context) ([]runSnapshot, error) {
	entries, err := s.scanLocks(ctx, s.locksPrefix(), scanResumable)
	if err != nil {
		return nil, err
	}

	active := make([]runSnapshot, 0, len(entries))
	for _, e := range entries {
		active = append(active, runSnapshot{
			Run:   runFromManifest(e.version, e.manifest),
			State: e.manifest.GetStatus(),
			Error: recordedRunErr(e.manifest),
		})
	}
	return active, nil
}

// errRunNotPending aborts the SetRunning CAS, so resuming a mid-flight run costs no write.
var errRunNotPending = errors.New("run is not pending")

// SetRunning flips the manifest to RUNNING under the fence. Appending a transition would set it
// too, but not until the first one finished — too late to tell an executing run from a waiting one.
// A write that fails releases the run and reports ErrLeaseLost, so it is claimed and started again
// rather than executed unrecorded or stranded under a lease nobody is running.
func (s *objectStore) SetRunning(ctx context.Context, run Run) error {
	epoch, ok := s.ownedEpoch(run.StartVersion)
	if !ok {
		return ErrLeaseLost
	}

	_, err := s.casManifest(ctx, run.StartVersion, func(m *fsmv1.RunManifest) error {
		if err := s.checkFence(m, epoch); err != nil {
			return err
		}
		if m.Status != fsmv1.RunState_RUN_STATE_PENDING {
			return errRunNotPending
		}
		m.Status = fsmv1.RunState_RUN_STATE_RUNNING
		return nil
	})
	switch {
	case err == nil, errors.Is(err, errRunNotPending):
		return nil
	case errors.Is(err, ErrLeaseLost):
		return err
	}
	// A run must not execute while its manifest says it never started — a cancel takes that as
	// leave to settle it without finalizers — so hand it back to be claimed again instead.
	releaseCtx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
	defer cancel()
	s.releaseLease(releaseCtx, run.StartVersion, epoch)
	return fmt.Errorf("%w: run start not recorded: %w", ErrLeaseLost, err)
}

// ForgetRun releases this node's claim on the run after a failed resume so another node — or
// this one, after the resume retry interval — can adopt it; the resource lock stays visible
// until recovery succeeds. The deferral holds whether or not the lease was still held: a lost
// lease does not make the run decodable.
func (s *objectStore) ForgetRun(run Run) error {
	if epoch, ok := s.ownedEpoch(run.StartVersion); ok {
		ctx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
		defer cancel()
		s.releaseLease(ctx, run.StartVersion, epoch)
	}
	s.deferClaim(run.StartVersion)
	return nil
}

// finishState is this process's knowledge of a run's finish.
type finishState int

const (
	finishUnknown  finishState = iota // never finished here; the manifest is all there is
	finishInFlight                    // appendFinish is mid-cleanup here; the outcome is coming
	finishDone                        // finished here; the typed outcome is recorded
)

type runFinish struct {
	state finishState

	err RunErr
}

// maxFinishes bounds the finishes map; see evictSettledFinish for the eviction policy.
const maxFinishes = 4096

// beginFinish marks the run's finish as in flight before the manifest turns terminal, so a
// waiter that observes completion holds until the cleanup lands.
func (s *objectStore) beginFinish(version ulid.ULID) {
	s.finishMu.Lock()
	defer s.finishMu.Unlock()
	s.finishes[version] = runFinish{state: finishInFlight}
}

// completeFinish records the run's typed outcome once the finish cleanup has landed.
func (s *objectStore) completeFinish(version ulid.ULID, runErr RunErr) {
	s.finishMu.Lock()
	defer s.finishMu.Unlock()

	if len(s.finishes) >= maxFinishes {
		s.evictSettledFinish()
	}
	s.finishes[version] = runFinish{state: finishDone, err: runErr}
}

// evictSettledFinish drops one arbitrary settled finish to bound the map; its waiters fall
// back to the manifest's recorded error. In-flight entries are never evicted — they gate
// waiter release — and are bounded by concurrent finishes. Callers hold finishMu.
func (s *objectStore) evictSettledFinish() {
	for version, finish := range s.finishes {
		if finish.state != finishDone {
			continue
		}
		delete(s.finishes, version)
		return
	}
}

// settleFinish clears a finish that failed mid-cleanup. A completed finish was already
// overwritten to done, which this leaves in place — the deferred call cannot clobber a
// recorded outcome.
func (s *objectStore) settleFinish(version ulid.ULID) {
	s.finishMu.Lock()
	defer s.finishMu.Unlock()
	if finish, ok := s.finishes[version]; ok && finish.state == finishInFlight {
		delete(s.finishes, version)
	}
}

// localFinish reports the run's finish state and recorded outcome in one atomic lookup, so a
// caller that observed a terminal manifest gets a definitive answer: use the typed outcome,
// keep waiting for it, or fall back to the manifest.
func (s *objectStore) localFinish(version ulid.ULID) (RunErr, finishState) {
	s.finishMu.Lock()
	defer s.finishMu.Unlock()

	finish, ok := s.finishes[version]
	if !ok {
		return RunErr{}, finishUnknown
	}
	return finish.err, finish.state
}

// History returns the terminal record for a completed run, on any node, from the moment the run
// is observably terminal. The history object is the last write of a finish, after the manifest
// flip a peer's WaitRun returns at, so a run without one is answered from its terminal manifest.
func (s *objectStore) History(ctx context.Context, runVersion ulid.ULID) (*fsmv1.HistoryEvent, error) {
	history, err := s.readHistory(ctx, runVersion)
	switch {
	case errors.Is(err, ErrFsmNotFound):
		return s.terminalHistory(ctx, runVersion)
	case err != nil:
		return nil, err
	}
	return history, nil
}

// terminalHistory builds a run's history record from its terminal manifest and the FINISH event
// it points at — the same record the finish writes and the archive loop repairs. It writes
// nothing: the owner's finish, or the archive loop after a crashed one, makes the record durable.
func (s *objectStore) terminalHistory(ctx context.Context, runVersion ulid.ULID) (*fsmv1.HistoryEvent, error) {
	notFound := fmt.Errorf("history event not found, %s, %w", runVersion, ErrFsmNotFound)

	manifest, _, err := s.getManifest(ctx, runVersion)
	switch {
	case errors.Is(err, ErrFsmNotFound):
		return nil, notFound
	case err != nil:
		return nil, err
	}
	if !manifestTerminal(manifest) {
		return nil, notFound
	}

	finishEvent, err := s.finishEventFromManifest(ctx, manifest)
	if err != nil {
		return nil, err
	}
	return historyFromManifest(manifest, finishEvent), nil
}

// RunResult returns the run's marshaled response. It reads the terminal manifest, whose
// LatestResponse the last completed transition materialized — durable and cross-node the moment
// WaitRun returns. A manifest already archived away falls back to the history record.
func (s *objectStore) RunResult(ctx context.Context, runVersion ulid.ULID) ([]byte, error) {
	switch manifest, _, err := s.getManifest(ctx, runVersion); {
	case errors.Is(err, ErrFsmNotFound):
		// Manifest archived away: the history record holds the same finish response.
		return responseFromHistory(s.History(ctx, runVersion))
	case err != nil:
		return nil, err
	default:
		return manifest.GetLatestResponse(), nil
	}
}

// Children is implemented by the children/ prefix listing.
func (s *objectStore) Children(ctx context.Context, parent ulid.ULID) ([]ulid.ULID, error) {
	return s.listChildren(ctx, parent)
}

// Runs returns the versions of all runs ever recorded for the resource, oldest first, from the
// keys-only index/ prefix listing. Index markers outlive locks and events, so completed and
// archived runs remain enumerable.
func (s *objectStore) Runs(ctx context.Context, resourceType, resourceID string) ([]ulid.ULID, error) {
	keys, err := s.listKeys(ctx, s.runIndexPrefix(resourceType, resourceID))
	if err != nil {
		return nil, err
	}

	runs := make([]ulid.ULID, 0, len(keys))
	for _, key := range keys {
		version, err := versionFromKey(key)
		if err != nil {
			s.logger.ErrorContext(ctx, "failed to parse run version from index key", "error", err, "key", key)
			continue
		}
		runs = append(runs, version)
	}

	// The listing groups keys by action, so re-sort chronologically across actions.
	slices.SortFunc(runs, func(a, b ulid.ULID) int { return a.Compare(b) })
	return runs, nil
}

// Close stops the archive loop and releases every lease this node still holds so peers can claim
// its runs immediately instead of waiting out the lease timeout.
func (s *objectStore) Close() error {
	s.logger.Info("shutting down object store")

	if s.archiveCancel != nil {
		s.archiveCancel()
		<-s.archiveDone
	}

	ctx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
	defer cancel()
	s.releaseLeases(ctx)
	return nil
}
