package fsm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"
	"github.com/superfly/fsm/gen/fsm/v1/fsmv1connect"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const tracerName = "fsm"

type Manager struct {
	logger logrus.FieldLogger

	tracer trace.Tracer

	wg sync.WaitGroup

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

	// NodeID uniquely identifies this node in the object storage backend's run leases, for
	// fencing and observability. It is an identity, never a network address; no component
	// needs a routable address for any other. Defaults to "<hostname>-<ulid>". Ignored for
	// BoltDB.
	NodeID string
}

// New creates a new FSM manager to register and run FSMs.
func New(cfg Config) (*Manager, error) {
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

	store, err := newBackend(cfg, tracer, cfg.Logger.WithField("sys", "fsm-store"))
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})

	man := &Manager{
		logger:  cfg.Logger.WithField("sys", "fsm"),
		tracer:  tracer,
		store:   store,
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

	if lc, ok := store.(leaseCoordinator); ok {
		man.wg.Add(1)
		go func() {
			defer man.wg.Done()
			man.coordinate(lc, cfg.ObjectStorage.heartbeatPeriod(), cfg.ObjectStorage.claimInterval())
		}()
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
func newBackend(cfg Config, tracer trace.Tracer, logger logrus.FieldLogger) (Store, error) {
	if cfg.ObjectStorage != nil {
		return newObjectStore(context.Background(), logger, cfg.ObjectStorage, cfg.NodeID)
	}
	if err := os.MkdirAll(cfg.DBPath, 0o700); err != nil {
		return nil, fmt.Errorf("failed to setup DB path: %w", err)
	}
	return newStore(logger, tracer, cfg.DBPath)
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
	active := ActiveSet{}
	for _, typeName := range m.registeredTypes() {
		typeActive, err := m.store.ActiveRuns(ctx, typeName, id)
		if err != nil {
			return nil, err
		}
		maps.Copy(active, typeActive)
	}
	return active, nil
}

// registeredTypes returns the distinct resource type names registered with this manager. A
// type's runs are the same regardless of which action registered it.
func (m *Manager) registeredTypes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := map[string]struct{}{}
	types := make([]string, 0, len(m.fsms))
	for fk := range m.fsms {
		if _, ok := seen[fk.name]; ok {
			continue
		}
		seen[fk.name] = struct{}{}
		types = append(types, fk.name)
	}
	return types
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
	// Dedup the merged result on run version: the Bolt backend keys events by id alone and
	// returns every run regardless of type, so multiple types would repeat runs.
	seen := map[ulid.ULID]struct{}{}
	var runs []ulid.ULID
	for _, typeName := range m.registeredTypes() {
		typeRuns, err := m.store.Runs(ctx, typeName, id)
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
	children, err := m.store.ActiveChildren(ctx, parent)
	if err != nil {
		return nil, err
	}

	// Manifests don't record the resource alias, so the object backend returns runs without
	// one; restore it from the registered FSM.
	m.mu.RLock()
	for i, child := range children {
		f, ok := m.fsms[fsmKey{name: child.TypeName, action: child.Action}]
		if !ok {
			continue
		}
		children[i].ResourceName = f.alias
	}
	m.mu.RUnlock()

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

// Wait blocks until the run with the given version completes.
func (m *Manager) Wait(ctx context.Context, version ulid.ULID) error {
	logger := m.logger.WithField("start_version", version.String())

	logger.Info("waiting for FSM to finish")
	defer logger.Info("done waiting for FSM to finish")
	return m.store.WaitRun(ctx, version)
}

// WaitByID blocks until the run with the given ID completes.
func (m *Manager) WaitByID(ctx context.Context, id string) error {
	logger := m.logger.WithField("fsm_run_id", id)

	logger.Info("waiting for FSM to finish")
	defer logger.Info("done waiting for FSM to finish")

	// Resolve the id to its run version. On a miss the version stays zero and WaitRun reports
	// it via the store (completed) or as not found.
	version, err := m.resolveRun(ctx, id)
	if err != nil {
		return err
	}
	if version.Compare(ulid.ULID{}) != 0 {
		logger = logger.WithField("start_version", version.String())
	}
	return m.store.WaitRun(ctx, version)
}

// resolveRun resolves an id to its run version across the registered types.
func (m *Manager) resolveRun(ctx context.Context, id string) (ulid.ULID, error) {
	for _, typeName := range m.registeredTypes() {
		version, err := m.store.ResolveRun(ctx, typeName, id)
		if err != nil {
			return ulid.ULID{}, err
		}
		if version.Compare(ulid.ULID{}) != 0 {
			return version, nil
		}
	}
	return ulid.ULID{}, nil
}
