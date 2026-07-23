package fsm

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

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

// lockKey returns the resource lock key enforcing one-active-run-per-resource. Queued runs are
// allowed to stack, so their locks are further keyed by run version.
func (s *objectStore) lockKey(resourceType, resourceID, action, queue string, runVersion ulid.ULID) string {
	if queue == "" {
		return s.key("locks", escapeSegment(resourceType), escapeSegment(resourceID), escapeSegment(action))
	}
	return s.key("locks", escapeSegment(resourceType), escapeSegment(resourceID), escapeSegment(action), runVersion.String())
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
func (s *objectStore) casManifest(ctx context.Context, runVersion ulid.ULID, mutate func(*fsmv1.RunManifest) error) (*fsmv1.RunManifest, error) {
	key := s.manifestKey(runVersion)
	var updated *fsmv1.RunManifest
	op := func() error {
		manifest, etag, err := s.getManifest(ctx, runVersion)
		if err != nil {
			return backoff.Permanent(err)
		}

		if err := mutate(manifest); err != nil {
			return backoff.Permanent(err)
		}
		manifest.UpdatedAt = time.Now().Unix()

		body, err := proto.Marshal(manifest)
		if err != nil {
			return backoff.Permanent(fmt.Errorf("marshal manifest %s: %w", runVersion, err))
		}

		switch err := s.putIfMatch(ctx, key, body, etag); {
		case err == nil:
			updated = manifest
			return nil
		case errors.Is(err, errEtagMismatch):
			s.logger.WithField("key", key).Debug("manifest changed concurrently, retrying")
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

// Append writes an event object and updates the run manifest according to the event type. It is
// the object storage implementation of the single mutation path all run state flows through.
func (s *objectStore) Append(ctx context.Context, run Run, event *fsmv1.StateEvent, queue string, opts ...appendOptionFunc) (ulid.ULID, error) {
	// Persistence must complete even when the run itself is being canceled — the CANCEL and
	// FINISH events are appended from an already-canceled run context, and BoltDB's local
	// writes are likewise not interruptible. Values (e.g. trace context) are preserved.
	ctx = context.WithoutCancel(ctx)

	var ao appendOption
	for _, opt := range opts {
		if err := opt(&ao); err != nil {
			return ulid.ULID{}, err
		}
	}

	if ao.start == nil && event.GetType() == fsmv1.EventType_EVENT_TYPE_START {
		return ulid.ULID{}, errors.New("start option must be set")
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
		err = s.appendStart(ctx, run, event, queue, &ao, eventKey, eventBytes)
	case fsmv1.EventType_EVENT_TYPE_ERROR,
		fsmv1.EventType_EVENT_TYPE_COMPLETE,
		fsmv1.EventType_EVENT_TYPE_CANCEL:
		err = s.appendMidRun(ctx, run, event, eventKey, eventBytes)
	case fsmv1.EventType_EVENT_TYPE_FINISH:
		err = s.appendFinish(ctx, run, event, queue, eventKey, eventBytes)
	default:
		err = fmt.Errorf("%T: %w", event.Type, errInvalidEventType)
	}
	if err != nil {
		s.logger.WithError(err).Error("failed to append event")
		return ulid.ULID{}, err
	}

	return eventVersion, nil
}

func (s *objectStore) appendStart(ctx context.Context, run Run, event *fsmv1.StateEvent, queue string, ao *appendOption, eventKey string, eventBytes []byte) error {
	runVersionBytes, err := run.StartVersion.MarshalText()
	if err != nil {
		return err
	}

	// 1. Acquire the resource lock. A 412 means another run holds it.
	lockKey := s.lockKey(event.GetResourceType(), event.GetId(), event.GetAction(), queue, run.StartVersion)
	if err := s.putIfAbsent(ctx, lockKey, runVersionBytes); err != nil {
		if !errors.Is(err, errPreconditionFailed) {
			return err
		}
		owner, _, gerr := s.getObject(ctx, lockKey)
		if gerr != nil {
			return fmt.Errorf("resource lock %s is held but unreadable: %w", lockKey, gerr)
		}
		var ownerVersion ulid.ULID
		if err := ownerVersion.UnmarshalText(owner); err != nil {
			return fmt.Errorf("resource lock %s has invalid owner: %w", lockKey, err)
		}
		return &AlreadyRunningError{Version: ownerVersion}
	}

	// 2. Write the START event.
	if err := s.putIdempotent(ctx, eventKey, eventBytes); err != nil {
		return err
	}

	// 2b. Record the run in the per-resource index. Unlike the lock (deleted at FINISH) and
	// events (deleted at archival), index markers persist, so past runs stay enumerable.
	indexKey := s.runIndexKey(event.GetResourceType(), event.GetId(), event.GetAction(), run.StartVersion)
	if err := s.putIdempotent(ctx, indexKey, nil); err != nil {
		return err
	}

	// 3. Create the manifest, leased to this node from the start: the starting worker executes
	// the run immediately and heartbeats from the first tick.
	manifest := &fsmv1.RunManifest{
		Status:         fsmv1.RunState_RUN_STATE_PENDING,
		ResourceType:   event.GetResourceType(),
		ResourceId:     event.GetId(),
		Action:         event.GetAction(),
		RunVersion:     runVersionBytes,
		StartEventKey:  []byte(eventKey),
		LatestEventKey: []byte(eventKey),
		EventCount:     1,
		Transitions:    ao.start.transitions,
		Resource:       ao.start.resource,
		Queue:          queue,
		Parent:         ao.parent,
		DelayUntil:     ao.delayUntil,
		RunAfter:       ao.runAfter,
		OwnerNode:      s.nodeID,
		LeaseExpiry:    time.Now().Add(s.cfg.leaseTimeout()).UnixMilli(),
		LeaseEpoch:     1,
		TraceContext:   map[string]string{},
		CreatedAt:      time.Now().Unix(),
		UpdatedAt:      time.Now().Unix(),
	}
	(propagation.TraceContext{}).Inject(ctx, propagation.MapCarrier(manifest.TraceContext))

	manifestBytes, err := proto.Marshal(manifest)
	if err != nil {
		return err
	}
	switch err := s.putIfAbsent(ctx, s.manifestKey(run.StartVersion), manifestBytes); {
	case err == nil:
		s.trackLease(run.StartVersion, 1)
	case errors.Is(err, errPreconditionFailed):
		// A crashed earlier attempt of this same run already created the manifest; it is
		// authoritative. Adopt its lease if this node still owns it.
		existing, _, getErr := s.getManifest(ctx, run.StartVersion)
		if getErr != nil {
			return getErr
		}
		if existing.GetOwnerNode() != s.nodeID {
			return ErrLeaseLost
		}
		s.trackLease(run.StartVersion, existing.GetLeaseEpoch())
	default:
		return err
	}

	// 4. Record the parent-child relationship.
	if ao.parent != nil {
		var parent ulid.ULID
		if err := parent.UnmarshalText(ao.parent); err != nil {
			return fmt.Errorf("invalid parent version: %w", err)
		}
		if err := s.writeChild(ctx, parent, run.StartVersion); err != nil {
			return err
		}
	}

	return nil
}

func (s *objectStore) appendMidRun(ctx context.Context, run Run, event *fsmv1.StateEvent, eventKey string, eventBytes []byte) error {
	// The local ownership guard keeps a fenced writer from creating orphan event objects in
	// the common case; the fence check inside the CAS is authoritative for the true race.
	epoch, ok := s.ownedEpoch(run.StartVersion)
	if !ok {
		return ErrLeaseLost
	}

	if err := s.putIdempotent(ctx, eventKey, eventBytes); err != nil {
		return err
	}

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
		case fsmv1.EventType_EVENT_TYPE_CANCEL:
			m.CompletedStates = appendUniqueState(m.CompletedStates, event.GetState())
			m.Error = event.GetError()
			m.ErrorState = event.GetState()
		case fsmv1.EventType_EVENT_TYPE_ERROR:
			m.RetryCount = event.GetRetryCount()
		}
		return nil
	})
	if errors.Is(err, ErrLeaseLost) {
		s.noteFence(run.StartVersion)
	}
	return err
}

func (s *objectStore) appendFinish(ctx context.Context, run Run, event *fsmv1.StateEvent, queue string, eventKey string, eventBytes []byte) error {
	// Marked before the manifest turns terminal, so a waiter that observes completion holds
	// until the cleanup below lands and the typed outcome is recorded.
	s.markFinishing(run.StartVersion)
	defer s.unmarkFinishing(run.StartVersion)

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
		m.Status = fsmv1.RunState_RUN_STATE_COMPLETE
		m.EndEventKey = []byte(eventKey)
		m.LatestEventKey = []byte(eventKey)
		m.EventCount++
		if run.fsmErr.Err != nil {
			m.Error = run.fsmErr.Err.Error()
			m.ErrorState = run.fsmErr.State
		}
		return nil
	})
	switch {
	case errors.Is(err, ErrFsmNotFound):
		s.logger.WithField("run_version", run.StartVersion.String()).Warn("manifest not found for finish event")
		return nil
	case errors.Is(err, ErrLeaseLost):
		s.noteFence(run.StartVersion)
		return err
	case err != nil:
		return err
	}

	s.dropLease(run.StartVersion)

	// The lock is deleted after the manifest records completion; a crash in between leaves an
	// orphaned lock that Active detects (manifest complete) and removes opportunistically.
	lockKey := s.lockKey(manifest.GetResourceType(), manifest.GetResourceId(), manifest.GetAction(), queue, run.StartVersion)
	if err := s.deleteObject(ctx, lockKey); err != nil {
		s.logger.WithError(err).WithField("key", lockKey).Error("failed to delete resource lock")
	}

	if err := s.writeHistory(ctx, run.StartVersion, &fsmv1.HistoryEvent{
		ActiveEvent: activeEventFromManifest(manifest),
		LastEvent:   event,
	}); err != nil {
		return err
	}

	s.rememberFinished(run.StartVersion, run.fsmErr)

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
			Parent:     m.GetParent(),
		},
		TraceContext: m.GetTraceContext(),
	}
}

// manifestTerminal reports whether the manifest records a run that will make no further
// progress.
func manifestTerminal(m *fsmv1.RunManifest) bool {
	return m.GetStatus() == fsmv1.RunState_RUN_STATE_COMPLETE || m.GetStatus() == fsmv1.RunState_RUN_STATE_ARCHIVED
}

// manifestRunErr rebuilds the run error recorded in the manifest.
func manifestRunErr(m *fsmv1.RunManifest) RunErr {
	if m.GetErrorState() == "" {
		return RunErr{}
	}
	return RunErr{
		Err:   errors.New(m.GetError()),
		State: m.GetErrorState(),
	}
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
		TypeName:     m.GetResourceType(),
		Queue:        m.GetQueue(),
		Parent:       parent,
		fsmErr:       manifestRunErr(m),
	}
}

// lockEntry pairs an active resource lock with the manifest of the run holding it.
type lockEntry struct {
	resourceType, resourceID, action string

	version ulid.ULID

	manifest *fsmv1.RunManifest
}

// scanLocks lists resource locks under prefix and resolves each to its owning run's manifest
// (keys-only listing plus one lock GET and one manifest GET per active run — event bodies are
// never read, per the RFC's resumability contract). Crash windows are handled inline: a lock
// without a manifest is skipped, and a lock whose manifest is terminal is deleted.
func (s *objectStore) scanLocks(ctx context.Context, prefix string) ([]lockEntry, error) {
	lockKeys, err := s.listKeys(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var entries []lockEntry
	for _, lockKey := range lockKeys {
		logger := s.logger.WithField("key", lockKey)

		// locks/<type>/<id>/<action>[/<run_version>]
		segments := strings.Split(strings.TrimPrefix(lockKey, s.locksPrefix()), "/")
		if len(segments) < 3 {
			logger.Warn("malformed lock key")
			continue
		}
		resourceType, typeErr := unescapeSegment(segments[0])
		resourceID, idErr := unescapeSegment(segments[1])
		action, actionErr := unescapeSegment(segments[2])
		if err := errors.Join(typeErr, idErr, actionErr); err != nil {
			logger.WithError(err).Warn("malformed lock key")
			continue
		}

		owner, _, err := s.getObject(ctx, lockKey)
		if err != nil {
			logger.WithError(err).Error("failed to read resource lock")
			continue
		}
		var version ulid.ULID
		if err := version.UnmarshalText(owner); err != nil {
			logger.WithError(err).Error("failed to unmarshal lock owner version")
			continue
		}

		manifest, _, err := s.getManifest(ctx, version)
		switch {
		case errors.Is(err, ErrFsmNotFound):
			// Crash window between lock acquisition and manifest creation. Leave the lock for
			// the periodic recovery scan (Phase 4) rather than racing an in-flight START.
			logger.Warn("lock held but manifest missing, skipping")
			continue
		case err != nil:
			return nil, err
		}

		if manifestTerminal(manifest) {
			// Crash window between manifest completion and lock deletion: finish the cleanup.
			logger.Info("completed run still holds its lock, removing")
			if err := s.deleteObject(ctx, lockKey); err != nil {
				logger.WithError(err).Error("failed to delete orphaned lock")
			}
			continue
		}

		entries = append(entries, lockEntry{
			resourceType: resourceType,
			resourceID:   resourceID,
			action:       action,
			version:      version,
			manifest:     manifest,
		})
	}
	return entries, nil
}

// Active returns all incomplete runs for the given FSM, enumerated from the locks/ prefix.
func (s *objectStore) Active(ctx context.Context, f *fsm) ([]*activeResource, error) {
	entries, err := s.scanLocks(ctx, s.lockPrefix(f.typeName))
	if err != nil {
		return nil, err
	}

	var active []*activeResource
	for _, e := range entries {
		if e.action != f.action {
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
		response:             m.GetLatestResponse(),
		retryCount:           m.GetRetryCount(),
		fsmError:             manifestRunErr(m),
	}
}

func (s *objectStore) ActiveRuns(ctx context.Context, resourceType, resourceID string) (ActiveSet, error) {
	entries, err := s.scanLocks(ctx, s.lockResourcePrefix(resourceType, resourceID))
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

// ResolveRun prefers the oldest active run holding a lock for the resource; with no lock held
// it falls back to the most recent run in the index/ prefix, so a just-finished run's outcome
// remains reachable through WaitRun.
func (s *objectStore) ResolveRun(ctx context.Context, resourceType, resourceID string) (ulid.ULID, error) {
	entries, err := s.scanLocks(ctx, s.lockResourcePrefix(resourceType, resourceID))
	if err != nil {
		return ulid.ULID{}, err
	}
	if len(entries) > 0 {
		oldest := entries[0].version
		for _, e := range entries[1:] {
			if e.version.Compare(oldest) < 0 {
				oldest = e.version
			}
		}
		return oldest, nil
	}

	runs, err := s.Runs(ctx, resourceType, resourceID)
	if err != nil || len(runs) == 0 {
		return ulid.ULID{}, err
	}
	return runs[len(runs)-1], nil
}

var errRunInFlight = errors.New("run still in flight")

// WaitRun polls the run's manifest under exponential backoff until it records a terminal
// state. Outcomes of runs this process completed are answered from the in-process finished
// map, which preserves typed run errors; a missing manifest is answered from history. A
// terminal manifest whose finish is still being cleaned up by this process keeps the waiter
// polling until the outcome is recorded.
func (s *objectStore) WaitRun(ctx context.Context, runVersion ulid.ULID) error {
	var outcome error
	op := func() error {
		if runErr, ok := s.finishedOutcome(runVersion); ok {
			outcome = runErr.Err
			return nil
		}

		manifest, _, err := s.getManifest(ctx, runVersion)
		switch {
		case errors.Is(err, ErrFsmNotFound):
			outcome = historyOutcome(ctx, s, runVersion)
			return nil
		case err != nil:
			return backoff.Permanent(err)
		}

		if !manifestTerminal(manifest) || s.isFinishing(runVersion) {
			return errRunInFlight
		}
		// A finish on this process may have fully landed between the map check above and the
		// manifest read; the outcome is recorded before the finishing mark clears, so check
		// once more before falling back to the manifest's error string.
		if runErr, ok := s.finishedOutcome(runVersion); ok {
			outcome = runErr.Err
			return nil
		}
		if manifest.GetError() != "" {
			outcome = &haltError{err: errors.New(manifest.GetError())}
		}
		return nil
	}
	if err := backoff.Retry(op, s.waitBackoff(ctx)); err != nil {
		return err
	}
	return outcome
}

func (s *objectStore) ListActive(ctx context.Context) ([]runState, error) {
	entries, err := s.scanLocks(ctx, s.locksPrefix())
	if err != nil {
		return nil, err
	}

	active := make([]runState, 0, len(entries))
	for _, e := range entries {
		active = append(active, runState{
			Run:   runFromManifest(e.version, e.manifest),
			State: e.manifest.GetStatus(),
			Error: manifestRunErr(e.manifest),
		})
	}
	return active, nil
}

// SetRunning is a no-op: the manifest's status, maintained by Append, is the object backend's
// run state, so a run reads as PENDING until its first transition writes an event.
func (s *objectStore) SetRunning(run Run) error {
	return nil
}

// ForgetRun releases this node's claim on the run after a failed resume so another node (or a
// later claim pass) can adopt it; the resource lock stays visible until recovery succeeds.
func (s *objectStore) ForgetRun(run Run) error {
	epoch, ok := s.ownedEpoch(run.StartVersion)
	if !ok {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
	defer cancel()
	s.releaseLease(ctx, run.StartVersion, epoch)
	return nil
}

// maxFinishedOutcomes bounds the finished map; when full, arbitrary entries are evicted, and
// their waiters fall back to the manifest's recorded error.
const maxFinishedOutcomes = 4096

func (s *objectStore) rememberFinished(version ulid.ULID, runErr RunErr) {
	s.finishedMu.Lock()
	defer s.finishedMu.Unlock()

	for len(s.finished) >= maxFinishedOutcomes {
		for evict := range s.finished {
			delete(s.finished, evict)
			break
		}
	}
	s.finished[version] = runErr
}

func (s *objectStore) finishedOutcome(version ulid.ULID) (RunErr, bool) {
	s.finishedMu.Lock()
	defer s.finishedMu.Unlock()

	runErr, ok := s.finished[version]
	return runErr, ok
}

func (s *objectStore) markFinishing(version ulid.ULID) {
	s.finishedMu.Lock()
	defer s.finishedMu.Unlock()
	s.finishing[version] = struct{}{}
}

func (s *objectStore) unmarkFinishing(version ulid.ULID) {
	s.finishedMu.Lock()
	defer s.finishedMu.Unlock()
	delete(s.finishing, version)
}

func (s *objectStore) isFinishing(version ulid.ULID) bool {
	s.finishedMu.Lock()
	defer s.finishedMu.Unlock()
	_, ok := s.finishing[version]
	return ok
}

// History returns the archived record for a completed run. History objects are written at
// FINISH time, so any completed run resolves immediately.
func (s *objectStore) History(ctx context.Context, runVersion ulid.ULID) (*fsmv1.HistoryEvent, error) {
	history, err := s.readHistory(ctx, runVersion)
	if err != nil {
		if errors.Is(err, ErrFsmNotFound) {
			return nil, fmt.Errorf("history event not found, %s, %w", runVersion, ErrFsmNotFound)
		}
		return nil, err
	}
	return history, nil
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
		segments := strings.Split(key, "/")

		var version ulid.ULID
		if err := version.UnmarshalText([]byte(segments[len(segments)-1])); err != nil {
			s.logger.WithError(err).WithField("key", key).Error("failed to parse run version from index key")
			continue
		}
		runs = append(runs, version)
	}

	// The listing groups keys by action, so re-sort chronologically across actions.
	slices.SortFunc(runs, func(a, b ulid.ULID) int { return a.Compare(b) })
	return runs, nil
}

// Close releases every lease this node still holds so peers can claim its runs immediately
// instead of waiting out the lease timeout.
func (s *objectStore) Close() error {
	s.logger.Info("shutting down object store")

	ctx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
	defer cancel()
	s.releaseLeases(ctx)
	return nil
}
