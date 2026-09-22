package fsmtest

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ampbase-io/fsm"
	"github.com/ampbase-io/fsm/fsmtest/fake"
)

// Factory creates Managers over one shared storage backend. Successive NewManager calls see
// state persisted by earlier managers, which is how restart, resume and takeover scenarios are
// modeled: stop one manager, create the next, and the new one finds the first's runs.
type Factory struct {
	// Name is the backend, "bolt" or "object"; RunBackends uses it as the subtest name.
	Name string

	// NewManager creates a manager with the given queue capacities. The returned stop function
	// shuts the manager down and is safe to call more than once; it is also registered as a
	// test cleanup.
	NewManager func(queues map[string]int) (*fsm.Manager, func())

	// Configure, when set before a NewManager call, adjusts the Config a manager is created
	// with — the logger or meter provider, say — on top of the backend the factory wires.
	Configure func(*fsm.Config)

	t testing.TB
}

// RunBackends runs the scenario as a subtest against each storage backend.
func RunBackends(t *testing.T, fn func(t *testing.T, f *Factory)) {
	t.Run("bolt", func(t *testing.T) { fn(t, NewBoltFactory(t)) })
	t.Run("object", func(t *testing.T) { fn(t, NewObjectFactory(t)) })
}

// NewBoltFactory returns a Factory over a BoltDB in a fresh temp directory.
func NewBoltFactory(t testing.TB) *Factory {
	t.Helper()

	// The dir is created directly under /tmp to keep the admin unix socket path under the
	// sun_path length limit.
	dir, err := os.MkdirTemp("/tmp", "fsm-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	f := &Factory{Name: "bolt", t: t}
	f.NewManager = func(queues map[string]int) (*fsm.Manager, func()) {
		return f.newManagerFrom(fsm.Config{DBPath: dir, Queues: queues})
	}
	return f
}

// ObjectOption adjusts how NewObjectFactory wires its managers.
type ObjectOption func(*objectOptions)

type objectOptions struct {
	bus       fsm.EventBus
	configure func(*fsm.ObjectStorageConfig)
}

// WithBus injects one EventBus into every manager the factory creates, so a test can drive the
// cross-node fast paths (wait wakeup, claim wakeup). fake.NewBus is the in-process choice.
func WithBus(bus fsm.EventBus) ObjectOption {
	return func(o *objectOptions) { o.bus = bus }
}

// WithObjectConfig adjusts the object storage config before each manager is created. It runs
// once per NewManager call, so a closure can count calls and treat nodes differently — see
// AsymmetricTimings.
func WithObjectConfig(configure func(*fsm.ObjectStorageConfig)) ObjectOption {
	return func(o *objectOptions) { o.configure = configure }
}

// NewObjectFactory returns a Factory over an in-memory S3 started for the test. Every manager
// gets a distinct NodeID, as distinct nodes sharing a bucket would in production.
func NewObjectFactory(t testing.TB, opts ...ObjectOption) *Factory {
	t.Helper()

	var o objectOptions
	for _, opt := range opts {
		opt(&o)
	}
	s3 := fake.NewS3(t)

	f := &Factory{Name: "object", t: t}
	nodes := 0
	f.NewManager = func(queues map[string]int) (*fsm.Manager, func()) {
		cfg := &fsm.ObjectStorageConfig{
			Bucket: s3.Bucket(),
			Client: s3.Client(),
			// The fake S3 answers instantly, so tight poll intervals keep object-backend waits
			// from eating the default floor per Wait.
			WaitPollInterval:    1 * time.Millisecond,
			WaitPollMaxInterval: 10 * time.Millisecond,
		}
		if o.configure != nil {
			o.configure(cfg)
		}
		nodes++
		return f.newManagerFrom(fsm.Config{
			ObjectStorage: cfg,
			NodeID:        fmt.Sprintf("node-%d", nodes),
			Queues:        queues,
			EventBus:      o.bus,
		})
	}
	return f
}

// newManagerFrom applies the Configure hook, creates the manager and registers its shutdown.
func (f *Factory) newManagerFrom(cfg fsm.Config) (*fsm.Manager, func()) {
	f.t.Helper()
	if f.Configure != nil {
		f.Configure(&cfg)
	}
	m, err := fsm.New(cfg)
	if err != nil {
		f.t.Fatalf("failed to create %s manager: %v", f.Name, err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { m.Shutdown(5 * time.Second) }) }
	f.t.Cleanup(stop)
	return m, stop
}
