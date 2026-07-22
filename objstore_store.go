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
// retrying on concurrent modification. It returns the manifest as written.
func (s *objectStore) casManifest(ctx context.Context, runVersion ulid.ULID, mutate func(*fsmv1.RunManifest)) (*fsmv1.RunManifest, error) {
	key := s.manifestKey(runVersion)
	var updated *fsmv1.RunManifest
	op := func() error {
		manifest, etag, err := s.getManifest(ctx, runVersion)
		if err != nil {
			return backoff.Permanent(err)
		}

		mutate(manifest)
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

	// 3. Create the manifest. A 412 means a crashed earlier attempt of this same run already
	// created it; the existing manifest is authoritative.
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
		TraceContext:   map[string]string{},
		CreatedAt:      time.Now().Unix(),
		UpdatedAt:      time.Now().Unix(),
	}
	(propagation.TraceContext{}).Inject(ctx, propagation.MapCarrier(manifest.TraceContext))

	manifestBytes, err := proto.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := s.putIdempotent(ctx, s.manifestKey(run.StartVersion), manifestBytes); err != nil {
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

	// 5. Mirror into memdb, clearing completed rows for this resource id first (in-flight runs
	// for the same id under a different action are left alone).
	txn := s.memDB.Txn(true)
	defer txn.Abort()
	iter, err := txn.Get(fsmTable, runIndex, run.ID)
	if err != nil {
		return err
	}
	for next := iter.Next(); next != nil; next = iter.Next() {
		rs := next.(runState)
		if rs.State != fsmv1.RunState_RUN_STATE_COMPLETE {
			continue
		}
		if err := txn.Delete(fsmTable, rs); err != nil {
			return err
		}
	}
	if err := txn.Insert(fsmTable, runState{Run: run, State: fsmv1.RunState_RUN_STATE_PENDING}); err != nil {
		return fmt.Errorf("failed to update state: %w", err)
	}
	txn.Commit()

	return nil
}

func (s *objectStore) appendMidRun(ctx context.Context, run Run, event *fsmv1.StateEvent, eventKey string, eventBytes []byte) error {
	if err := s.putIdempotent(ctx, eventKey, eventBytes); err != nil {
		return err
	}

	_, err := s.casManifest(ctx, run.StartVersion, func(m *fsmv1.RunManifest) {
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
	})
	return err
}

func (s *objectStore) appendFinish(ctx context.Context, run Run, event *fsmv1.StateEvent, queue string, eventKey string, eventBytes []byte) error {
	if err := s.putIdempotent(ctx, eventKey, eventBytes); err != nil {
		return err
	}

	manifest, err := s.casManifest(ctx, run.StartVersion, func(m *fsmv1.RunManifest) {
		m.Status = fsmv1.RunState_RUN_STATE_COMPLETE
		m.EndEventKey = []byte(eventKey)
		m.LatestEventKey = []byte(eventKey)
		m.EventCount++
		if run.fsmErr.Err != nil {
			m.Error = run.fsmErr.Err.Error()
			m.ErrorState = run.fsmErr.State
		}
	})
	switch {
	case errors.Is(err, ErrFsmNotFound):
		s.logger.WithField("run_version", run.StartVersion.String()).Warn("manifest not found for finish event")
		return nil
	case err != nil:
		return err
	}

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

	txn := s.memDB.Txn(true)
	defer txn.Abort()
	if err := txn.Insert(fsmTable, runState{
		Run:   run,
		State: fsmv1.RunState_RUN_STATE_COMPLETE,
		Error: run.fsmErr,
	}); err != nil {
		return fmt.Errorf("failed to update state: %w", err)
	}
	txn.Commit()

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

// Active returns all incomplete runs for the given FSM. Enumeration is driven by the locks/
// prefix (keys-only listing plus one lock GET and one manifest GET per active run) — event
// bodies are never read, per the RFC's resumability contract.
func (s *objectStore) Active(ctx context.Context, f *fsm) ([]*activeResource, error) {
	lockKeys, err := s.listKeys(ctx, s.lockPrefix(f.typeName))
	if err != nil {
		return nil, err
	}

	var active []*activeResource
	for _, lockKey := range lockKeys {
		logger := s.logger.WithField("key", lockKey)

		// locks/<type>/<id>/<action>[/<run_version>] — filter to this FSM's action.
		segments := strings.Split(strings.TrimPrefix(lockKey, s.lockPrefix(f.typeName)), "/")
		if len(segments) < 2 {
			logger.Warn("malformed lock key")
			continue
		}
		action, err := unescapeSegment(segments[1])
		if err != nil || action != f.action {
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

		if manifest.GetStatus() == fsmv1.RunState_RUN_STATE_COMPLETE || manifest.GetStatus() == fsmv1.RunState_RUN_STATE_ARCHIVED {
			// Crash window between manifest completion and lock deletion: finish the cleanup.
			logger.Info("completed run still holds its lock, removing")
			if err := s.deleteObject(ctx, lockKey); err != nil {
				logger.WithError(err).Error("failed to delete orphaned lock")
			}
			continue
		}

		var fsmError RunErr
		if manifest.GetErrorState() != "" {
			fsmError = RunErr{
				Err:   errors.New(manifest.GetError()),
				State: manifest.GetErrorState(),
			}
		}

		active = append(active, &activeResource{
			version:              version,
			active:               activeEventFromManifest(manifest),
			completedTransitions: manifest.GetCompletedStates(),
			response:             manifest.GetLatestResponse(),
			retryCount:           manifest.GetRetryCount(),
			fsmError:             fsmError,
		})
	}

	txn := s.memDB.Txn(true)
	defer txn.Abort()
	for _, ar := range active {
		var parent ulid.ULID
		if parentBytes := ar.active.GetOptions().GetParent(); parentBytes != nil {
			if err := parent.UnmarshalText(parentBytes); err != nil {
				s.logger.WithError(err).Error("failed to unmarshal parent")
			}
		}
		rs := runState{
			Run: Run{
				ID:           ar.active.GetResourceId(),
				StartVersion: ar.version,
				Action:       f.action,
				ResourceName: f.alias,
				TypeName:     f.typeName,
				Queue:        ar.active.GetOptions().GetQueue(),
				Parent:       parent,
				fsmErr:       ar.fsmError,
			},
			State: fsmv1.RunState_RUN_STATE_PENDING,
		}
		if err := txn.Insert(fsmTable, rs); err != nil {
			return nil, err
		}
	}
	txn.Commit()

	return active, nil
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

// Close shuts down the store. The object storage backend holds no local resources or background
// loops in Phase 3; history is written at FINISH time rather than by an archive loop.
func (s *objectStore) Close() error {
	s.logger.Info("shutting down object store")
	return nil
}
