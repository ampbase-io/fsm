package fsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/hashicorp/go-memdb"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const (
	stateDB   = "fsm-state.db"
	historyDB = "fsm-history.db"
)

var (
	activeBucket   = []byte("ACTIVE")
	archiveBucket  = []byte("ARCHIVE")
	eventsBucket   = []byte("EVENTS")
	childrenBucket = []byte("CHILDREN")
	historyBucket  = []byte("HISTORY")

	keySeparator = []byte("#")
	emptyPrefix  = []byte{}

	errInvalidEventType = errors.New("invalid event type")

	errEventArchived = errors.New("event archived")
)

// Store is the interface that storage backends must implement.
type Store interface {
	Append(ctx context.Context, run Run, event *fsmv1.StateEvent, queue string, opts ...appendOptionFunc) (ulid.ULID, error)
	Active(ctx context.Context, f *fsm) ([]*activeResource, error)
	History(ctx context.Context, runVersion ulid.ULID) (*fsmv1.HistoryEvent, error)
	Children(ctx context.Context, parent ulid.ULID) ([]ulid.ULID, error)
	// Runs returns the versions of runs recorded for the resource, oldest first, including
	// completed runs. Backends differ in retention: BoltDB serves runs whose events have not
	// yet been archived, while the object storage backend indexes runs durably.
	Runs(ctx context.Context, resourceType, resourceID string) ([]ulid.ULID, error)

	// Run-state queries. The BoltDB backend answers from its private in-memory index; the
	// object storage backend answers from object storage (locks/, index/, children/, and run
	// manifests), so the answers hold across nodes sharing a bucket.

	// ActiveRuns returns the incomplete runs recorded for the resource.
	ActiveRuns(ctx context.Context, resourceType, resourceID string) (ActiveSet, error)
	// ActiveChildren returns the incomplete runs started from the given parent.
	ActiveChildren(ctx context.Context, parent ulid.ULID) ([]Run, error)
	// ResolveRun resolves a resource id to a run version, preferring an active run and falling
	// back to the most recently recorded one. A miss returns a zero version.
	ResolveRun(ctx context.Context, resourceType, resourceID string) (ulid.ULID, error)
	// WaitRun blocks until the run reaches a terminal state or ctx ends, returning the run's
	// recorded error (nil on success, and nil for runs no longer known to the backend).
	WaitRun(ctx context.Context, runVersion ulid.ULID) error
	// ListActive returns every incomplete run the backend knows about.
	ListActive(ctx context.Context) ([]runState, error)

	// Run-state notes from the executor.

	// SetRunning records that the run has begun executing transitions on this node.
	SetRunning(run Run) error
	// ForgetRun discards local run state after a failed resume so waiters consult the backend.
	ForgetRun(run Run) error

	Close() error
}

const (
	fsmTable          = "fsm"
	idIndex           = "id"
	runIndex          = "run"
	parentIndex       = "parent"
	parentPrefixIndex = parentIndex + "_prefix"
)

// fsmSchema indexes run state for the BoltDB backend's run-state queries. It is private to
// boltStore: the object storage backend answers the same queries from object storage.
var fsmSchema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		fsmTable: {
			Name: fsmTable,
			Indexes: map[string]*memdb.IndexSchema{
				idIndex: {
					Name:   idIndex,
					Unique: true,
					Indexer: ulidIndexer{
						fieldFn: func(rs runState) ulid.ULID {
							return rs.StartVersion
						},
					},
				},
				runIndex: {
					Name:    runIndex,
					Unique:  false,
					Indexer: &memdb.StringFieldIndex{Field: "ID"},
				},
				parentIndex: {
					Name:         parentIndex,
					AllowMissing: true,
					Unique:       false,
					Indexer: ulidIndexer{
						fieldFn: func(rs runState) ulid.ULID {
							return rs.Parent
						},
					},
				},
			},
		},
	},
}

// historyOutcome reports a completed run's terminal result from the store, mapping a missing
// record to a nil error (the run is gone) and a recorded error to a haltError.
func historyOutcome(ctx context.Context, s Store, version ulid.ULID) error {
	he, err := s.History(ctx, version)
	switch {
	case errors.Is(err, ErrFsmNotFound):
		return nil
	case err != nil:
		return err
	case he.GetLastEvent().GetError() != "":
		return &haltError{err: errors.New(he.GetLastEvent().GetError())}
	default:
		return nil
	}
}

var _ Store = (*boltStore)(nil)

type boltStore struct {
	logger logrus.FieldLogger

	tracer trace.Tracer

	cancel context.CancelFunc

	db *bbolt.DB

	history *bbolt.DB

	memDB *memdb.MemDB

	// archiveCh is only used in tests to signal the archive loop to run.
	archiveCh chan struct{}
}

func newStore(logger logrus.FieldLogger, tracer trace.Tracer, path string) (*boltStore, error) {
	memDB, err := memdb.NewMemDB(fsmSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create memdb, %w", err)
	}

	db, err := bbolt.Open(filepath.Join(path, stateDB), 0o600, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(activeBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(eventsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(archiveBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(childrenBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	history, err := bbolt.Open(filepath.Join(path, historyDB), 0o600, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &boltStore{
		logger:    logger,
		tracer:    tracer,
		cancel:    cancel,
		archiveCh: make(chan struct{}),
		db:        db,
		history:   history,
		memDB:     memDB,
	}

	go s.archive(ctx)

	return s, nil
}

func (s *boltStore) Close() error {
	s.logger.Info("shutting down store")

	s.cancel()

	s.logger.Info("waiting for archive loop to finish")
	<-s.archiveCh
	s.logger.Info("archive loop finished")

	var err error
	if dbErr := s.db.Close(); dbErr != nil {
		err = fmt.Errorf("failed to close state db, %w", dbErr)
	}

	if historyErr := s.history.Close(); historyErr != nil {
		err = errors.Join(err, fmt.Errorf("failed to close history db, %w", historyErr))
	}

	return err
}

type archiveEvent struct {
	archiveKey []byte

	historyEvent *fsmv1.HistoryEvent
}

func (s *boltStore) archive(ctx context.Context) {
	// TODO: clear out date buckets older than X days
	runArchive := func(ctx context.Context) {
		ctx, rootSpan := s.tracer.Start(ctx, "store.archive")
		defer rootSpan.End()

		archiveEvents := map[string][]*archiveEvent{}

		_, gatherSpan := s.tracer.Start(ctx, "store.archive.gather")
		s.db.View(func(tx *bbolt.Tx) error {
			return tx.Bucket(archiveBucket).ForEach(func(k, v []byte) error {
				var ae fsmv1.ActiveEvent
				if err := proto.Unmarshal(v, &ae); err != nil {
					s.logger.WithError(err).Error("failed to unmarshal active event")
					gatherSpan.RecordError(err)
					return nil
				}

				var version ulid.ULID
				if err := version.UnmarshalText(ae.StartVersion); err != nil {
					s.logger.WithError(err).Error("failed to unmarshal version")
					gatherSpan.RecordError(err)
					// TODO: delete active event
					return nil
				}

				var se fsmv1.StateEvent
				err := proto.Unmarshal(tx.Bucket(eventsBucket).Get(ae.EndEvent), &se)
				if err != nil {
					s.logger.WithError(err).Error("failed to unmarshal end event")
					gatherSpan.RecordError(err)
					// TODO: delete archive event
					return nil
				}

				startTime := ulid.Time(version.Time())
				startDate := startTime.Format(time.DateOnly)

				toArchive, ok := archiveEvents[startDate]
				if !ok {
					toArchive = []*archiveEvent{}
				}
				archiveEvents[startDate] = append(toArchive, &archiveEvent{
					archiveKey: k,
					historyEvent: &fsmv1.HistoryEvent{
						ActiveEvent: &ae,
						LastEvent:   &se,
					},
				})
				return nil
			})
		})
		gatherSpan.End()
		switch {
		case ctx.Err() != nil:
			s.logger.Info("context canceled, exiting archive loop")
			return
		case len(archiveEvents) == 0:
			s.logger.Debug("no active events to archive")
			return
		default:
		}

		_, processSpan := s.tracer.Start(ctx, "store.archive.process",
			trace.WithAttributes(attribute.Int("archive_events", len(archiveEvents))),
		)
		defer processSpan.End()
		for date, events := range archiveEvents {
			if ctx.Err() != nil {
				s.logger.Info("context canceled, exiting archive loop")
				return
			}

			todayBucket := bytes.Join([][]byte{historyBucket, []byte(date)}, keySeparator)
			s.logger.WithField("date", date).WithField("count", len(events)).Info("archiving events")

			// NOTE: we don't care about the error here
			s.history.Update(func(tx *bbolt.Tx) error {
				historyB, err := tx.CreateBucketIfNotExists(todayBucket)
				if err != nil {
					s.logger.WithError(err).Error("failed to create history bucket")
					return err
				}

				for _, event := range events {
					historyEvent := event.historyEvent
					historyBytes, err := proto.Marshal(historyEvent)
					if err != nil {
						s.logger.WithError(err).Error("failed to marshal history event")
						processSpan.RecordError(err)
						continue
					}
					// Keyed by run version to match the archive bucket and History lookups.
					historyB.Put(event.archiveKey, historyBytes)
				}
				return nil
			})

			for _, event := range events {
				if ctx.Err() != nil {
					s.logger.Info("context canceled, exiting archive loop")
					return
				}

				childrenToDelete := [][]byte{}
				parentPrefix := bytes.Join([][]byte{event.archiveKey, emptyPrefix}, keySeparator)
				s.db.View(func(tx *bbolt.Tx) error {
					childrenC := tx.Bucket(childrenBucket).Cursor()
					for k, _ := childrenC.Seek(parentPrefix); k != nil && bytes.HasPrefix(k, parentPrefix); k, _ = childrenC.Next() {
						childrenToDelete = append(childrenToDelete, k)
					}
					return nil
				})

				if ctx.Err() != nil {
					s.logger.Info("context canceled, exiting archive loop")
					return
				}

				eventsToDelete := [][]byte{}
				s.db.View(func(tx *bbolt.Tx) error {
					startEvent := event.historyEvent.GetActiveEvent().GetStartEvent()
					endEvent := event.historyEvent.GetActiveEvent().GetEndEvent()
					eventB := tx.Bucket(eventsBucket)
					eventC := eventB.Cursor()
					for k, _ := eventC.Seek(startEvent); k != nil && bytes.Compare(k, endEvent) <= 0; k, _ = eventC.Next() {
						eventsToDelete = append(eventsToDelete, k)
					}
					return nil
				})

				if ctx.Err() != nil {
					s.logger.Info("context canceled, exiting archive loop")
					return
				}
				s.db.Update(func(tx *bbolt.Tx) error {
					eventsB := tx.Bucket(eventsBucket)
					for _, k := range eventsToDelete {
						if err := eventsB.Delete(k); err != nil {
							s.logger.WithError(err).Error("failed to delete event")
							processSpan.RecordError(err)
						}
					}

					childrenB := tx.Bucket(childrenBucket)
					for _, k := range childrenToDelete {
						if err := childrenB.Delete(k); err != nil {
							s.logger.WithError(err).Error("failed to delete child")
							processSpan.RecordError(err)
						}
					}

					if err := tx.Bucket(archiveBucket).Delete(event.archiveKey); err != nil {
						s.logger.WithError(err).Error("failed to delete archive event")
						processSpan.RecordError(err)
					}

					return nil
				})
			}
		}
	}

	defer close(s.archiveCh)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("context canceled, exiting archive loop")
			return
		case <-s.archiveCh:
			s.logger.Info("archive loop signaled to run")
		case <-time.After(1 * time.Minute):
			s.logger.Info("running event archive")
		}
		runArchive(ctx)
	}
}

type activeResource struct {
	version ulid.ULID

	active *fsmv1.ActiveEvent

	completedTransitions []string

	response []byte

	retryCount uint64

	fsmError RunErr
}

func (s *boltStore) Active(ctx context.Context, f *fsm) ([]*activeResource, error) {
	var (
		resourceType = f.typeName
		activeEvents []*activeResource
		// "<resource_name>#"
		resourcePrefixKey = bytes.Join([][]byte{[]byte(resourceType), emptyPrefix}, keySeparator)
	)

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(activeBucket)
		eventB := tx.Bucket(eventsBucket)

		cursor := b.Cursor()
		for k, v := cursor.Seek(resourcePrefixKey); k != nil && bytes.HasPrefix(k, resourcePrefixKey); k, v = cursor.Next() {
			logger := s.logger.WithField("key", string(k))
			var ae fsmv1.ActiveEvent
			if err := proto.Unmarshal(v, &ae); err != nil {
				logger.WithError(err).Error("failed to unmarshal active event")
				continue
			}

			if ae.EndEvent != nil {
				logger.Info("active event has end event, skipping")
				continue
			}

			var version ulid.ULID
			if err := version.UnmarshalText(ae.StartVersion); err != nil {
				logger.WithError(err).Error("failed to unmarshal version")
				continue
			}

			// EVENT Bucket
			// <resource_id>#<action>#<run_version>
			eventPrefix := bytes.Join([][]byte{[]byte(ae.GetResourceId()), []byte(ae.GetAction()), ae.StartVersion, emptyPrefix}, keySeparator)
			eventCursor := eventB.Cursor()
			logger.WithField("start_event", string(ae.StartEvent)).WithField("event_prefix", string(eventPrefix)).Info("iterating events")
			var (
				completedTransitions []string
				response             []byte
				retryCount           uint64
				fsmError             RunErr
			)
			for eventKey, eventValue := eventCursor.Seek(ae.StartEvent); eventKey != nil && bytes.HasPrefix(eventKey, eventPrefix); eventKey, eventValue = eventCursor.Next() {
				var event fsmv1.StateEvent
				if err := proto.Unmarshal(eventValue, &event); err != nil {
					logger.WithError(err).Error("failed to unmarshal event")
					continue
				}

				switch event.Type {
				case fsmv1.EventType_EVENT_TYPE_COMPLETE:
					completedTransitions = append(completedTransitions, event.GetState())
					if event.GetResponse() != nil {
						response = event.GetResponse()
					}
				case fsmv1.EventType_EVENT_TYPE_CANCEL:
					completedTransitions = append(completedTransitions, event.GetState())
					fsmError = RunErr{
						Err:   errors.New(event.GetError()),
						State: event.GetState(),
					}
				case fsmv1.EventType_EVENT_TYPE_ERROR:
					retryCount = event.GetRetryCount()
				}
			}

			activeEvents = append(activeEvents, &activeResource{
				version:              version,
				active:               &ae,
				completedTransitions: completedTransitions,
				response:             response,
				retryCount:           retryCount,
				fsmError:             fsmError,
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	txn := s.memDB.Txn(true)
	defer txn.Abort()
	for _, ae := range activeEvents {
		// The queue and parent are restored from the persisted event options rather than from f,
		// whose fields only reflect the most recent Start call.
		var parent ulid.ULID
		if parentBytes := ae.active.GetOptions().GetParent(); parentBytes != nil {
			if err := parent.UnmarshalText(parentBytes); err != nil {
				s.logger.WithError(err).Error("failed to unmarshal parent")
			}
		}
		rs := runState{
			Run: Run{
				ID:           ae.active.GetResourceId(),
				StartVersion: ae.version,
				Action:       f.action,
				ResourceName: f.alias,
				TypeName:     f.typeName,
				Queue:        ae.active.GetOptions().GetQueue(),
				Parent:       parent,
				fsmErr:       ae.fsmError,
			},
			State: fsmv1.RunState_RUN_STATE_PENDING,
		}
		if err := txn.Insert(fsmTable, rs); err != nil {
			return nil, err
		}
	}
	txn.Commit()

	return activeEvents, nil
}

// ActiveRuns answers from the in-memory index. resourceType is unused because memdb rows are
// keyed by resource id alone (matching Runs).
func (s *boltStore) ActiveRuns(ctx context.Context, resourceType, resourceID string) (ActiveSet, error) {
	txn := s.memDB.Txn(false)
	defer txn.Abort()

	it, err := txn.Get(fsmTable, runIndex, resourceID)
	if err != nil {
		return nil, err
	}

	active := ActiveSet{}
	for next := it.Next(); next != nil; next = it.Next() {
		rs := next.(runState)
		if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
			continue
		}
		active[ActiveKey{Action: rs.Action, Version: rs.StartVersion}] = rs.State
	}

	return active, nil
}

func (s *boltStore) ActiveChildren(ctx context.Context, parent ulid.ULID) ([]Run, error) {
	txn := s.memDB.Txn(false)
	defer txn.Abort()

	it, err := txn.Get(fsmTable, parentPrefixIndex, parent)
	if err != nil {
		return nil, err
	}

	children := []Run{}
	for next := it.Next(); next != nil; next = it.Next() {
		rs := next.(runState)
		if rs.StartVersion.Compare(ulid.ULID{}) == 0 {
			continue
		}
		if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
			continue
		}
		children = append(children, rs.Run)
	}

	return children, nil
}

func (s *boltStore) ResolveRun(ctx context.Context, resourceType, resourceID string) (ulid.ULID, error) {
	txn := s.memDB.Txn(false)
	defer txn.Abort()

	item, err := txn.First(fsmTable, runIndex, resourceID)
	if err != nil || item == nil {
		return ulid.ULID{}, err
	}
	return item.(runState).StartVersion, nil
}

// WaitRun parks on the index's watch channel, which fires on the next state change of the
// watched run. A run missing from the index is answered from history: the store is
// authoritative for a run that already finished.
func (s *boltStore) WaitRun(ctx context.Context, runVersion ulid.ULID) error {
	for {
		txn := s.memDB.Txn(false)
		ch, item, err := txn.FirstWatch(fsmTable, idIndex, runVersion.String())
		txn.Abort()
		switch {
		case err != nil:
			s.logger.WithError(err).Error("failed to wait for FSM")
			return err
		case item == nil:
			return historyOutcome(ctx, s, runVersion)
		}

		rs, ok := item.(runState)
		if !ok {
			return fmt.Errorf("unexpected type %T", item)
		}
		if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
			return rs.Error.Err
		}

		ws := memdb.NewWatchSet()
		ws.Add(ch)
		if err := ws.WatchCtx(ctx); err != nil {
			return err
		}
	}
}

func (s *boltStore) ListActive(ctx context.Context) ([]runState, error) {
	txn := s.memDB.Txn(false)
	defer txn.Abort()

	it, err := txn.Get(fsmTable, idIndex)
	if err != nil {
		return nil, err
	}

	active := []runState{}
	for next := it.Next(); next != nil; next = it.Next() {
		rs := next.(runState)
		if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
			continue
		}
		active = append(active, rs)
	}
	return active, nil
}

func (s *boltStore) SetRunning(run Run) error {
	txn := s.memDB.Txn(true)
	defer txn.Abort()

	if err := txn.Insert(fsmTable, runState{Run: run, State: fsmv1.RunState_RUN_STATE_RUNNING}); err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (s *boltStore) ForgetRun(run Run) error {
	txn := s.memDB.Txn(true)
	defer txn.Abort()

	if err := txn.Delete(fsmTable, runState{Run: run}); err != nil {
		return err
	}
	txn.Commit()
	return nil
}

type appendOptionFunc func(*appendOption) error

type appendOption struct {
	delayUntil int64

	runAfter []byte

	parent []byte

	start *startOption
}

type startOption struct {
	transitions []string

	resource []byte
}

func withDelayUntil(delayUntil time.Time) appendOptionFunc {
	return func(opt *appendOption) error {
		if !delayUntil.IsZero() {
			opt.delayUntil = delayUntil.Unix()
		}
		return nil
	}
}

func withStartOption(resource []byte, transitions []string) appendOptionFunc {
	return func(opt *appendOption) error {
		opt.start = &startOption{
			resource:    resource,
			transitions: transitions,
		}
		return nil
	}
}

func withRunAfter(version ulid.ULID) appendOptionFunc {
	return func(opt *appendOption) error {
		if version.Compare(ulid.ULID{}) == 0 {
			return nil
		}
		runAfter, err := version.MarshalText()
		if err != nil {
			return err
		}
		opt.runAfter = runAfter
		return nil
	}
}

func withParent(parent ulid.ULID) appendOptionFunc {
	return func(opt *appendOption) error {
		if parent.Compare(ulid.ULID{}) == 0 {
			return nil
		}
		parentBytes, err := parent.MarshalText()
		if err != nil {
			return err
		}
		opt.parent = parentBytes
		return nil
	}
}

func (s *boltStore) Append(ctx context.Context, run Run, event *fsmv1.StateEvent, queue string, opts ...appendOptionFunc) (ulid.ULID, error) {
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

	eventVersion := ulid.Make()
	version, err := eventVersion.MarshalText()
	if err != nil {
		return ulid.ULID{}, err
	}

	eventBytes, err := proto.Marshal(event)
	if err != nil {
		return ulid.ULID{}, err
	}

	eventIdentifier := bytes.Join([][]byte{
		[]byte(event.GetId()),
		[]byte(event.GetAction()),
	}, keySeparator)

	// EVENT Bucket
	// <resource_id>#<action>#<run_version>#<event_version>
	eventKey := bytes.Join([][]byte{eventIdentifier, event.GetRunVersion(), version}, keySeparator)

	// ACTIVE Bucket
	// <resource_name>#<resource_id>#<action>#<run_version_or_empty>
	activeKeyVersion := ulid.ULID{}
	if queue != "" {
		activeKeyVersion = run.StartVersion
	}

	activeKeyVersionBytes, err := activeKeyVersion.MarshalText()
	if err != nil {
		return ulid.ULID{}, err
	}

	aeEventKey := bytes.Join([][]byte{[]byte(event.GetResourceType()), eventIdentifier, activeKeyVersionBytes}, keySeparator)
	var aeEventBytes []byte
	if event.GetType() == fsmv1.EventType_EVENT_TYPE_START {
		ae := &fsmv1.ActiveEvent{
			StartEvent:   eventKey,
			StartVersion: runVersionBytes,
			Action:       event.GetAction(),
			ResourceId:   event.GetId(),
			Resource:     ao.start.resource,
			Transitions:  ao.start.transitions,
			Options: &fsmv1.EventOptions{
				DelayUntil: ao.delayUntil,
				RunAfter:   ao.runAfter,
				Queue:      queue,
				Parent:     ao.parent,
			},
			TraceContext: map[string]string{},
		}
		(propagation.TraceContext{}).Inject(ctx, propagation.MapCarrier(ae.TraceContext))

		aeEventBytes, err = proto.Marshal(ae)
		if err != nil {
			return ulid.ULID{}, err
		}
	}

	rs := runState{
		Run: run,
	}

	txn := s.memDB.Txn(true)
	defer txn.Abort()
	err = s.db.Update(func(tx *bbolt.Tx) error {
		activeB := tx.Bucket(activeBucket)
		eventB := tx.Bucket(eventsBucket)

		switch event.GetType() {
		case fsmv1.EventType_EVENT_TYPE_START:
			// NOTE: we only allow one active event per resource unless it's getting queued
			if queue == "" && activeB.Get(aeEventKey) != nil {
				var ae fsmv1.ActiveEvent
				if err := proto.Unmarshal(activeB.Get(aeEventKey), &ae); err != nil {
					return err
				}

				if ae.EndEvent == nil {
					var sv ulid.ULID
					if err := sv.UnmarshalText(ae.StartVersion); err != nil {
						return err
					}
					return &AlreadyRunningError{Version: sv}
				}
			}

			if err := activeB.Put(aeEventKey, aeEventBytes); err != nil {
				return err
			}

			if err := eventB.Put(eventKey, eventBytes); err != nil {
				return err
			}

			if ao.parent != nil {
				// <parent_run_version>#<child_run_version>
				parentKey := bytes.Join([][]byte{ao.parent, runVersionBytes}, keySeparator)
				if err := tx.Bucket(childrenBucket).Put(parentKey, runVersionBytes); err != nil {
					return err
				}
			}

			// Clear out completed runs for this resource id so stale rows don't accumulate.
			// Runs that are still in flight are left alone: the same id may be actively running
			// under a different action, and deleting its row would make waiters believe it
			// finished.
			switch iter, err := txn.Get(fsmTable, runIndex, run.ID); {
			case err != nil:
				return err
			default:
				deleted := 0
				for next := iter.Next(); next != nil; next = iter.Next() {
					rs := next.(runState)
					if rs.State != fsmv1.RunState_RUN_STATE_COMPLETE {
						continue
					}
					if err := txn.Delete(fsmTable, rs); err != nil {
						return err
					}
					deleted++
				}
				if deleted > 0 {
					s.logger.WithField("id", run.ID).Info("deleted completed runs")
				}
			}

			rs.State = fsmv1.RunState_RUN_STATE_PENDING
			if err := txn.Insert(fsmTable, rs); err != nil {
				return fmt.Errorf("failed to update state: %w", err)
			}

			return nil
		case fsmv1.EventType_EVENT_TYPE_ERROR,
			fsmv1.EventType_EVENT_TYPE_COMPLETE,
			fsmv1.EventType_EVENT_TYPE_CANCEL:
			if err := eventB.Put(eventKey, eventBytes); err != nil {
				return err
			}

			return nil
		case fsmv1.EventType_EVENT_TYPE_FINISH:
			if err := eventB.Put(eventKey, eventBytes); err != nil {
				return err
			}

			activeResource := activeB.Get(aeEventKey)
			if activeResource == nil {
				s.logger.WithField("key", string(aeEventKey)).Warn("active event not found")
				return nil
			}

			var ae fsmv1.ActiveEvent
			if err := proto.Unmarshal(activeResource, &ae); err != nil {
				return err
			}
			ae.EndEvent = eventKey

			activeVersionBytes, err := proto.Marshal(&ae)
			if err != nil {
				return err
			}

			if err := activeB.Delete(aeEventKey); err != nil {
				return err
			}

			// ARCHIVE Bucket
			// <run_version>
			if err := tx.Bucket(archiveBucket).Put(runVersionBytes, activeVersionBytes); err != nil {
				return err
			}

			rs.State = fsmv1.RunState_RUN_STATE_COMPLETE
			rs.Error = run.fsmErr
			if err := txn.Insert(fsmTable, rs); err != nil {
				return fmt.Errorf("failed to update state: %w", err)
			}

			return nil
		default:
			return fmt.Errorf("%T: %w", event.Type, errInvalidEventType)
		}
	})
	if err != nil {
		s.logger.WithError(err).Error("failed to append event")
		return ulid.ULID{}, err
	}
	txn.Commit()

	return eventVersion, nil
}

func (s *boltStore) History(ctx context.Context, runVersion ulid.ULID) (*fsmv1.HistoryEvent, error) {
	runVersionBytes, err := runVersion.MarshalText()
	if err != nil {
		return nil, err
	}

	var historyEvent fsmv1.HistoryEvent
	err = s.db.View(func(tx *bbolt.Tx) error {
		archive := tx.Bucket(archiveBucket)

		aeBytes := archive.Get(runVersionBytes)
		if aeBytes == nil {
			// Lookup in History
			date := ulid.Time(runVersion.Time()).Format(time.DateOnly)
			historyBucket := bytes.Join([][]byte{historyBucket, []byte(date)}, keySeparator)
			return s.history.View(func(tx *bbolt.Tx) error {
				historyB := tx.Bucket(historyBucket)
				if historyB == nil {
					return fmt.Errorf("history bucket not found, %s, %w", date, ErrFsmNotFound)
				}

				historyBytes := historyB.Get(runVersionBytes)
				if historyBytes == nil {
					return fmt.Errorf("history event not found, %s, %w", runVersion, ErrFsmNotFound)
				}

				if err := proto.Unmarshal(historyBytes, &historyEvent); err != nil {
					s.logger.WithError(err).Error("failed to unmarshal history event")
					return err
				}

				return nil
			})
		}

		var ae fsmv1.ActiveEvent
		if err := proto.Unmarshal(aeBytes, &ae); err != nil {
			s.logger.WithError(err).Error("failed to unmarshal active event")
			return err
		}
		historyEvent.ActiveEvent = &ae

		var se fsmv1.StateEvent
		err := proto.Unmarshal(tx.Bucket(eventsBucket).Get(ae.EndEvent), &se)
		if err != nil {
			s.logger.WithError(err).Error("failed to unmarshal end event")
			return err
		}
		historyEvent.LastEvent = &se

		return nil

	})
	return &historyEvent, err
}

// Runs returns the versions of runs recorded for the resource by scanning the events bucket,
// oldest first. Events for archived runs have been deleted, so only unarchived runs appear;
// resourceType is unused because BoltDB event keys are already scoped by resource id.
func (s *boltStore) Runs(ctx context.Context, resourceType, resourceID string) ([]ulid.ULID, error) {
	// EVENT bucket keys: <resource_id>#<action>#<run_version>#<event_version>
	prefix := bytes.Join([][]byte{[]byte(resourceID), emptyPrefix}, keySeparator)

	seen := map[ulid.ULID]struct{}{}
	runs := []ulid.ULID{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(eventsBucket).Cursor()
		for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			parts := bytes.Split(k, keySeparator)
			if len(parts) < 4 {
				continue
			}

			var version ulid.ULID
			if err := version.UnmarshalText(parts[len(parts)-2]); err != nil {
				s.logger.WithError(err).WithField("key", string(k)).Error("failed to parse run version from event key")
				continue
			}
			if _, ok := seen[version]; ok {
				continue
			}
			seen[version] = struct{}{}
			runs = append(runs, version)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(runs, func(a, b ulid.ULID) int { return a.Compare(b) })
	return runs, nil
}

func (s *boltStore) Children(ctx context.Context, parent ulid.ULID) ([]ulid.ULID, error) {
	parentyBytes, err := parent.MarshalText()
	if err != nil {
		return nil, err
	}

	children := []ulid.ULID{}
	err = s.db.View(func(tx *bbolt.Tx) error {
		childrenB := tx.Bucket(childrenBucket)
		cursor := childrenB.Cursor()
		parentPrefix := bytes.Join([][]byte{parentyBytes, emptyPrefix}, keySeparator)
		for k, v := cursor.Seek(parentPrefix); k != nil && bytes.HasPrefix(k, parentPrefix); k, v = cursor.Next() {
			if v == nil {
				continue
			}

			var child ulid.ULID
			if err := child.UnmarshalText(v); err != nil {
				return err
			}
			children = append(children, child)
		}
		return nil
	})
	return children, err
}
