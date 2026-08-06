package fsm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"
	"github.com/ampbase-io/fsm/gen/fsm/v1/fsmv1connect"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const tracerName = "fsm"

// Store is the persistence contract the Manager requires of a storage backend. It is declared
// here, with its consumer, rather than beside either implementation: boltStore (store.go) and
// objectStore (objstore_store.go) satisfy it structurally, and neither owns its shape. The
// narrower views a single call site needs — appender for the transition interceptors, or a
// backend that claims runs, fences them, or records cancels — are declared at those sites
// instead (see interceptor.go, coordinate.go, and cancelRecorder below).
type Store interface {
	appender
	io.Closer

	Active(ctx context.Context, d Descriptor) ([]*activeResource, error)
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
	// RunResult returns the marshaled W response of a completed run, or nil when it recorded
	// none. Safe to read the moment WaitRun returns: the object backend answers from the
	// terminal manifest, the BoltDB backend from the record written at FINISH.
	RunResult(ctx context.Context, runVersion ulid.ULID) ([]byte, error)
	// ListActive returns every incomplete run the backend knows about.
	ListActive(ctx context.Context) ([]runState, error)

	// Run-state notes from the executor.

	// SetRunning records that the run has begun executing transitions on this node.
	SetRunning(run Run) error
	// ForgetRun discards local run state after a failed resume so waiters consult the backend.
	ForgetRun(run Run) error
}

// cancelRecorder is the Cancel path's view of a backend that records a cancel durably for the
// owning node to react to; a backend without it is single-process, so Cancel stops the local run.
type cancelRecorder interface {
	// requestCancel records a cancel durably and broadcasts it; the owner reacts, not the
	// caller. Reports ErrFsmNotFound for a terminal or unknown run.
	requestCancel(ctx context.Context, version ulid.ULID, cause error) error
}

type Manager struct {
	logger logrus.FieldLogger

	tracer trace.Tracer

	wg sync.WaitGroup

	store Store

	// bus is the subscribe half of the event transport; the coordinate loop subscribes to it
	// for claim wakeup. The manager only ever subscribes — the object store owns publishing.
	// Defaults to a no-op bus when none is injected.
	bus EventSubscriber

	fsms map[fsmKey]*fsm

	queues map[string]*queuedRunner

	done chan struct{}

	// claimNudge pulls a claim pass forward after an opaque Start persists an unowned run, so the
	// accepting node participates in claiming it promptly even with no bus injected — the
	// same-process counterpart of the bus pending event. Buffered and coalescing; nil-safe to
	// signal because only the lease-coordinated coordinate loop drains it.
	claimNudge chan struct{}

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

	// EventBus is an optional low-latency transport for run-lifecycle events: it accelerates
	// cross-node Wait, wakes idle workers when a run becomes claimable, and carries cancel
	// signals. It is a best-effort accelerator over the durable event log — a down or absent
	// bus only degrades latency to the polling floors. The default is a no-op bus; the library
	// never imports a broker. Applies to the object storage backend.
	EventBus EventBus
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
		logger:     cfg.Logger.WithField("sys", "fsm"),
		tracer:     tracer,
		store:      store,
		bus:        busOrNoop(cfg.EventBus),
		fsms:       map[fsmKey]*fsm{},
		queues:     make(map[string]*queuedRunner, len(cfg.Queues)),
		done:       done,
		claimNudge: make(chan struct{}, 1),
		running:    map[ulid.ULID]context.CancelCauseFunc{},
	}

	// A store that coordinates run ownership via leases runs the background coordinate loop and
	// distributes work through it; one that does not is single-process. This is the only place the
	// backend's nature is decided — per-run sites assert the narrow capability views instead.
	lc, distributed := store.(leaseCoordinator)

	// The in-process queuedRunner enforces a queue's limit per process — correct for BoltDB, where
	// one process is the whole cluster. The object backend enforces queues cluster-wide through the
	// claim loop's admission (objstore_queue.go) and never routes through these runners, so it
	// starts none (they would otherwise be idle goroutines).
	if !distributed {
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
	}

	socket := cfg.AdminSocketPath
	if socket == "" && cfg.DBPath != "" {
		socket = filepath.Join(cfg.DBPath, "fsm.sock")
	}
	switch {
	case socket == "":
		man.logger.Info("no admin socket path configured, admin service disabled")
	default:
		if err := man.serveAdmin(socket); err != nil {
			// The store was constructed above and has already started its background loop(s)
			// (the object archive loop, bolt's archive loop); close it so a failed New leaks none.
			store.Close()
			return nil, err
		}
	}

	// Started after the last fallible step, so a failed New leaks no coordination goroutine.
	if distributed {
		man.wg.Add(1)
		go func() {
			defer man.wg.Done()
			man.coordinate(lc)
		}()
	}

	return man, nil
}

// newBackend constructs the storage backend selected by the config. Exactly one of DBPath or
// ObjectStorage is set; the caller validates that.
func newBackend(cfg Config, tracer trace.Tracer, logger logrus.FieldLogger) (Store, error) {
	if cfg.ObjectStorage != nil {
		return newObjectStore(context.Background(), logger, cfg.ObjectStorage, cfg.NodeID, cfg.EventBus, cfg.Queues)
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

// registeredFSMs returns a snapshot of the registered FSMs; the claim loop and queries read
// the registry concurrently with registration.
func (m *Manager) registeredFSMs() []*fsm {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fsms := make([]*fsm, 0, len(m.fsms))
	for _, f := range m.fsms {
		fsms = append(fsms, f)
	}
	return fsms
}

// registeredTypes returns the distinct resource type names registered with this manager. A
// type's runs are the same regardless of which action registered it.
func (m *Manager) registeredTypes() []string {
	seen := map[string]struct{}{}
	var types []string
	for _, f := range m.registeredFSMs() {
		if _, ok := seen[f.typeName]; ok {
			continue
		}
		seen[f.typeName] = struct{}{}
		types = append(types, f.typeName)
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
// completed, so callers should use Wait to ensure the FSM has stopped, if needed.
//
// Under a lease-coordinated backend the cancel is subject-addressed: it is recorded durably and
// broadcast, and whichever node owns the run reacts — the run need not be executing on this
// node, or anywhere yet. The local context is canceled too for immediate effect when this node
// is the owner. A single-process backend cancels the local run directly.
func (m *Manager) Cancel(ctx context.Context, version ulid.ULID, cause string) error {
	cerr := errors.New(cause)
	if rec, ok := m.store.(cancelRecorder); ok {
		// Record durably first — while the run is still active — so the owner reacts even if it
		// is another node; then cancel locally for immediacy if we hold it.
		if err := rec.requestCancel(ctx, version, cerr); err != nil {
			return err
		}
		m.cancelRunning(version, cerr)
		return nil
	}
	if !m.cancelRunning(version, cerr) {
		return ErrFsmNotFound
	}
	return nil
}

// cancelRunning cancels the run's local context with the given cause, reporting whether a
// running context was found.
func (m *Manager) cancelRunning(version ulid.ULID, cause error) bool {
	m.mu.RLock()
	cancel, ok := m.running[version]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	cancel(cause)
	return true
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

// History returns the archived terminal record for a completed run.
func (m *Manager) History(ctx context.Context, version ulid.ULID) (*fsmv1.HistoryEvent, error) {
	return m.store.History(ctx, version)
}

// RunResult returns the marshaled W response of a completed run, or nil when the run recorded
// none (an error before any transition produced a result). The bytes are opaque — a client
// decodes them with the same response codec the FSM was registered with.
func (m *Manager) RunResult(ctx context.Context, version ulid.ULID) ([]byte, error) {
	return m.store.RunResult(ctx, version)
}

// startOpaque durably submits a run from an already-serialized request payload and returns its
// version once persisted (persist-then-ack). The client holds only the proto contract, not the
// Go R/W types, so this path never touches the typed request. On a lease-coordinated backend the
// run is persisted unowned and the claim loop executes it — the ingress/execution split; on a
// single-process backend, which has no claim loop, the accepting node runs it directly.
func (m *Manager) startOpaque(ctx context.Context, typeName, action, id string, resource []byte, opts ...StartOptionsFn) (ulid.ULID, error) {
	f, err := m.lookupFSM(typeName, action)
	if err != nil {
		return ulid.ULID{}, err
	}

	if _, ok := m.store.(runClaimer); !ok {
		return f.startFromBytes(ctx, id, resource, opts...)
	}

	// Validate the payload against the request codec so a malformed submission fails here rather
	// than persisting a run every claim pass will choke on.
	if err := f.decodeResource(resource); err != nil {
		return ulid.ULID{}, err
	}

	var startOpt startOptions
	for _, opt := range opts {
		opt(&startOpt)
	}

	runVersion := ulid.Make()
	if _, err := m.persistStart(ctx, f, id, runVersion, resource, &startOpt, withUnowned()); err != nil {
		return ulid.ULID{}, err
	}

	m.nudgeClaim()
	return runVersion, nil
}

// lookupFSM resolves the FSM an opaque Start names. A type disambiguates when the same action is
// registered under more than one; omitting it is allowed only when the action is unique.
func (m *Manager) lookupFSM(typeName, action string) (*fsm, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if typeName != "" {
		f, ok := m.fsms[fsmKey{name: typeName, action: action}]
		if !ok {
			return nil, fmt.Errorf("%w: %s/%s", errFSMNotRegistered, typeName, action)
		}
		return f, nil
	}

	var found *fsm
	for key, f := range m.fsms {
		if key.action != action {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("%w: %q", errAmbiguousAction, action)
		}
		found = f
	}
	if found == nil {
		return nil, fmt.Errorf("%w: action %q", errFSMNotRegistered, action)
	}
	return found, nil
}

// nudgeClaim pulls a claim pass forward on the coordinate loop without waiting for the periodic
// tick or a bus wakeup. Non-blocking and coalescing, matching the pending-event wakeup.
func (m *Manager) nudgeClaim() {
	select {
	case m.claimNudge <- struct{}{}:
	default:
	}
}
