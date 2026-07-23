package fsm

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"
	"github.com/superfly/fsm/gen/fsm/v1/fsmv1connect"

	"github.com/hashicorp/go-memdb"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	fsmTable          = "fsm"
	idIndex           = "id"
	runIndex          = "run"
	runPrefixIndex    = runIndex + "_prefix"
	parentIndex       = "parent"
	parentPrefixIndex = parentIndex + "_prefix"

	tracerName = "fsm"
)

var (
	fsmSchema = &memdb.DBSchema{
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
)

type Manager struct {
	logger logrus.FieldLogger

	tracer trace.Tracer

	wg sync.WaitGroup

	db *memdb.MemDB

	store Store

	fsms map[fsmKey]*fsm

	queues map[string]*queuedRunner

	done chan struct{}

	mu      sync.RWMutex
	running map[ulid.ULID]context.CancelCauseFunc
}

type fsmKey struct {
	name string

	action string
}

type Config struct {
	Logger logrus.FieldLogger

	// DBPath is the directory to use for persisting FSM state with the BoltDB backend.
	// Exactly one of DBPath or ObjectStorage must be set.
	DBPath string

	// ObjectStorage configures the S3-compatible object storage backend. Exactly one of DBPath
	// or ObjectStorage must be set.
	ObjectStorage *ObjectStorageConfig

	// AdminSocketPath is the unix socket path for the admin service. Defaults to
	// "<DBPath>/fsm.sock" for the BoltDB backend; with the object storage backend the admin
	// service is only started when this is set.
	AdminSocketPath string

	// Queues defines which queues are available for FSMs to use. The key is the queue name and the
	// value is the maximum number of FSMs that can run concurrently.
	Queues map[string]int

	// NodeID is a unique identifier for this node. Used by the object storage backend's cluster
	// coordination (Phase 4 of the object storage RFC); ignored for BoltDB.
	NodeID string

	// NodeAddr is the address reachable by other nodes for cancel RPC. Used by the object
	// storage backend's cluster coordination (Phase 4); ignored for BoltDB.
	NodeAddr string
}

// New creates a new FSM manager to register and run FSMs.
func New(cfg Config) (*Manager, error) {
	memDB, err := memdb.NewMemDB(fsmSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create memdb, %w", err)
	}

	if cfg.Logger == nil {
		cfg.Logger = logrus.New()
	}

	if cfg.DBPath == "" && cfg.ObjectStorage == nil {
		return nil, errors.New("a storage backend is required: set DBPath (BoltDB) or ObjectStorage")
	}
	if cfg.DBPath != "" && cfg.ObjectStorage != nil {
		return nil, errors.New("DBPath and ObjectStorage are mutually exclusive; set exactly one")
	}

	tracer := otel.GetTracerProvider().Tracer(tracerName,
		trace.WithInstrumentationVersion("0.1.0"),
		trace.WithSchemaURL(semconv.SchemaURL),
	)

	store, err := newBackend(cfg, memDB, tracer, cfg.Logger.WithField("sys", "fsm-store"))
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})

	man := &Manager{
		logger:  cfg.Logger.WithField("sys", "fsm"),
		tracer:  tracer,
		store:   store,
		db:      memDB,
		fsms:    map[fsmKey]*fsm{},
		queues:  make(map[string]*queuedRunner, len(cfg.Queues)),
		done:    done,
		running: map[ulid.ULID]context.CancelCauseFunc{},
	}

	for name, size := range cfg.Queues {
		q := &queuedRunner{
			name:   name,
			size:   size,
			queue:  make(chan queueItem),
			queued: make([]func(), 0, size),
		}
		man.queues[name] = q
		go q.run(done, cfg.Logger.WithField("queue", name))
	}

	socket := cfg.AdminSocketPath
	if socket == "" && cfg.DBPath != "" {
		socket = filepath.Join(cfg.DBPath, "fsm.sock")
	}
	if socket == "" {
		man.logger.Info("no admin socket path configured, admin service disabled")
		return man, nil
	}
	if err := man.serveAdmin(socket); err != nil {
		return nil, err
	}

	return man, nil
}

// newBackend constructs the storage backend selected by the config. Exactly one of DBPath or
// ObjectStorage is set; the caller validates that.
func newBackend(cfg Config, memDB *memdb.MemDB, tracer trace.Tracer, logger logrus.FieldLogger) (Store, error) {
	if cfg.ObjectStorage != nil {
		return newObjectStore(context.Background(), logger, cfg.ObjectStorage, memDB)
	}
	if err := os.MkdirAll(cfg.DBPath, 0o700); err != nil {
		return nil, fmt.Errorf("failed to setup DB path: %w", err)
	}
	return newStore(logger, tracer, cfg.DBPath, memDB)
}

// serveAdmin starts the admin RPC service on the given unix socket and tears it down when the
// manager shuts down.
func (m *Manager) serveAdmin(socket string) error {
	mux := http.NewServeMux()
	mux.Handle(fsmv1connect.NewFSMServiceHandler(&adminServer{m: m}))
	server := &http.Server{Handler: h2c.NewHandler(mux, &http2.Server{})}

	os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("failed to listen on unix socket %s, %w", socket, err)
	}

	go server.Serve(listener)
	go func() {
		defer os.Remove(socket)
		<-m.done
		if err := listener.Close(); err != nil {
			m.logger.WithError(err).Error("failed to close unix listener")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			m.logger.WithError(err).Error("failed to shutdown http server")
		}
	}()
	return nil
}

// Shutdown sends a stop signal to all FSMs and blocks until they have all stopped.
func (m *Manager) Shutdown(timeout time.Duration) {
	m.logger.WithField("shutdown_timeout", timeout).Info("shutting down")

	m.mu.RLock()
	for id, cancel := range m.running {
		m.logger.WithField("fsm_id", id.String()).Info("shutting down fsm")
		cancel(nil)
	}
	m.mu.RUnlock()

	close(m.done)

	wait := make(chan struct{})
	go func() {
		defer close(wait)
		m.wg.Wait()
	}()

	select {
	case <-wait:
		m.logger.Info("all FSMs have shutdown")
	case <-time.After(timeout):
		m.logger.Warn("timed out waiting for FSMs to shutdown")
	}

	if err := m.store.Close(); err != nil {
		m.logger.WithError(err).Error("failed to close store")
	}

	m.logger.Info("shutdown complete")
}

type ActiveKey struct {
	Action  string
	Version ulid.ULID
}

type ActiveSet map[ActiveKey]fsmv1.RunState

// Active returns a map of active runs for the given id. The map keys are the run type and the
// values are the run version which can be used to wait for the run to complete.
func (m *Manager) Active(ctx context.Context, id string) (ActiveSet, error) {
	txn := m.db.Txn(false)
	defer txn.Abort()

	active := map[ActiveKey]fsmv1.RunState{}

	it, err := txn.Get(fsmTable, runIndex, id)
	if err != nil {
		return nil, err
	}

	for next := it.Next(); next != nil; next = it.Next() {
		rs := next.(runState)
		if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
			continue
		}
		active[ActiveKey{Action: rs.Action, Version: rs.StartVersion}] = rs.State
	}

	return active, nil
}

// Children returns a list of FSMs that are associated with the given parent.
func (m *Manager) Children(ctx context.Context, parent ulid.ULID) ([]ulid.ULID, error) {
	return m.store.Children(ctx, parent)
}

// Runs returns the versions of all runs recorded for the given resource id across the FSM
// types registered with this manager, oldest first, including completed runs. Details for a
// version can be fetched with History, or waited on with Wait. Retention follows the backend:
// BoltDB serves runs whose events have not yet been archived, while the object storage backend
// indexes runs durably.
func (m *Manager) Runs(ctx context.Context, id string) ([]ulid.ULID, error) {
	// Query each registered type once (a type's runs are the same regardless of which action
	// registered it). Dedup the merged result on run version: the Bolt backend keys events by
	// id alone and returns every run regardless of type, so multiple types would repeat runs.
	seenTypes := map[string]struct{}{}
	seen := map[ulid.ULID]struct{}{}
	var runs []ulid.ULID
	for fk := range m.fsms {
		if _, ok := seenTypes[fk.name]; ok {
			continue
		}
		seenTypes[fk.name] = struct{}{}

		typeRuns, err := m.store.Runs(ctx, fk.name, id)
		if err != nil {
			return nil, err
		}
		for _, v := range typeRuns {
			if _, ok := seen[v]; ok {
				continue
			}
			seen[v] = struct{}{}
			runs = append(runs, v)
		}
	}

	slices.SortFunc(runs, func(a, b ulid.ULID) int { return a.Compare(b) })
	return runs, nil
}

// ActiveChildren returns a list of FSMs that were started from the given parent and are still
// active.
func (m *Manager) ActiveChildren(ctx context.Context, parent ulid.ULID) ([]Run, error) {
	txn := m.db.Txn(false)
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

// Cancel sends a cancel signal to the FSM should it exist. It does not block until the FSM has
// completed so callers should use Wait to ensure the FSM has stopped, if needed.
func (m *Manager) Cancel(ctx context.Context, version ulid.ULID, cause string) error {
	m.mu.RLock()
	f, ok := m.running[version]
	m.mu.RUnlock()
	if !ok {
		return ErrFsmNotFound
	}

	f(errors.New(cause))
	return nil
}

// checkRun inspects the current state of the run with the given version. When the run is still in
// progress it returns a watch channel that fires on the next state change and done is false;
// otherwise done is true and err holds the run's result. The read transaction is scoped to this
// call — the watch channel stays valid after the transaction is aborted, as it is only closed by
// a future write.
func (m *Manager) checkRun(ctx context.Context, logger logrus.FieldLogger, version ulid.ULID) (<-chan struct{}, bool, error) {
	txn := m.db.Txn(false)
	defer txn.Abort()

	ch, item, err := txn.FirstWatch(fsmTable, idIndex, version.String())
	if err != nil {
		logger.WithError(err).Error("failed to wait for FSM")
		return nil, true, err
	}
	if item == nil {
		// Not in the local index; the store is authoritative for a run that already finished.
		return nil, true, m.historyOutcome(ctx, version)
	}

	rs, ok := item.(runState)
	if !ok {
		return nil, true, fmt.Errorf("unexpected type %T", item)
	}
	if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
		return nil, true, rs.Error.Err
	}
	return ch, false, nil
}

// historyOutcome reports a completed run's terminal result from the store, mapping a missing
// record to a nil error (the run is gone) and a recorded error to a haltError.
func (m *Manager) historyOutcome(ctx context.Context, version ulid.ULID) error {
	he, err := m.store.History(ctx, version)
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

// watch blocks until ch fires (a state change on the watched run) or ctx ends.
func (m *Manager) watch(ctx context.Context, logger logrus.FieldLogger, ch <-chan struct{}) error {
	ws := memdb.NewWatchSet()
	ws.Add(ch)
	switch err := ws.WatchCtx(ctx); {
	case errors.Is(err, context.Canceled):
		return err
	case err != nil:
		logger.WithError(err).Error("failed to wait for FSM")
		return err
	default:
		return nil
	}
}

// Wait blocks until the run with the given version completes.
func (m *Manager) Wait(ctx context.Context, version ulid.ULID) error {
	logger := m.logger.WithField("start_version", version.String())

	logger.Info("waiting for FSM to finish")
	defer logger.Info("done waiting for FSM to finish")
	for {
		ch, done, err := m.checkRun(ctx, logger, version)
		if done {
			return err
		}
		logger.Info("FSM still running")
		if err := m.watch(ctx, logger, ch); err != nil {
			return err
		}
	}
}

// WaitByID blocks until the run with the given ID completes.
func (m *Manager) WaitByID(ctx context.Context, id string) error {
	logger := m.logger.WithField("fsm_run_id", id)

	logger.Info("waiting for FSM to finish")
	defer logger.Info("done waiting for FSM to finish")
	for {
		// Resolve the id to its run version. On a miss version stays zero and checkRun reports
		// it via the store (completed) or as not found.
		version, done, err := m.resolveByID(id)
		if done {
			return err
		}
		if version.Compare(ulid.ULID{}) != 0 {
			logger = logger.WithField("start_version", version.String())
		}

		ch, done, err := m.checkRun(ctx, logger, version)
		if done {
			return err
		}
		logger.Info("FSM still running")
		if err := m.watch(ctx, logger, ch); err != nil {
			return err
		}
	}
}

// resolveByID looks up the active run version for an id in the local index. done is true when
// the run is already complete (its result is returned in err). A miss returns a zero version
// and done=false so the caller falls through to the store.
func (m *Manager) resolveByID(id string) (version ulid.ULID, done bool, err error) {
	txn := m.db.Txn(false)
	defer txn.Abort()

	item, ferr := txn.First(fsmTable, runIndex, id)
	if ferr != nil || item == nil {
		return ulid.ULID{}, false, nil
	}

	rs := item.(runState)
	if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
		return rs.StartVersion, true, rs.Error.Err
	}
	return rs.StartVersion, false, nil
}
