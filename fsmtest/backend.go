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

// Backend is one storage backend under test, handing out Managers over it. Successive
// NewManager calls see state persisted by earlier managers, which is how restart, resume and
// takeover scenarios are modeled: stop one manager, create the next, and the new one finds the
// first's runs.
type Backend struct {
	// Name is the backend, "bolt" or "object"; RunBackends uses it as the subtest name.
	Name string

	// Configure, when set before a NewManager call, adjusts the Config a manager is created
	// with — the logger or meter provider, say — on top of the backend's own wiring.
	Configure func(*fsm.Config)

	// config builds the Config for the next manager; each backend's constructor supplies it.
	config func(queues map[string]int) fsm.Config

	t testing.TB
}

// NewManager creates a manager with the given queue capacities. The returned stop function
// shuts the manager down and is safe to call more than once; it is also registered as a test
// cleanup.
func (b *Backend) NewManager(queues map[string]int) (*fsm.Manager, func()) {
	b.t.Helper()

	cfg := b.config(queues)
	if b.Configure != nil {
		b.Configure(&cfg)
	}
	m, err := fsm.New(cfg)
	if err != nil {
		b.t.Fatalf("failed to create %s manager: %v", b.Name, err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { m.Shutdown(5 * time.Second) }) }
	b.t.Cleanup(stop)
	return m, stop
}

// RunBackends runs the scenario as a subtest against each storage backend.
func RunBackends(t *testing.T, fn func(t *testing.T, b *Backend)) {
	t.Run("bolt", func(t *testing.T) { fn(t, NewBoltBackend(t)) })
	t.Run("object", func(t *testing.T) { fn(t, NewObjectBackend(t)) })
}

// NewBoltBackend returns a Backend over a BoltDB in a fresh temp directory.
func NewBoltBackend(t testing.TB) *Backend {
	t.Helper()

	// The dir is created directly under /tmp to keep the admin unix socket path under the
	// sun_path length limit.
	dir, err := os.MkdirTemp("/tmp", "fsm-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	return &Backend{
		Name: "bolt",
		t:    t,
		config: func(queues map[string]int) fsm.Config {
			return fsm.Config{DBPath: dir, Queues: queues}
		},
	}
}

// ObjectOption adjusts how NewObjectBackend wires its managers.
type ObjectOption func(*objectOptions)

type objectOptions struct {
	bus       fsm.EventBus
	configure func(*fsm.ObjectStorageConfig)
}

// WithBus injects one EventBus into every manager the backend creates, so a test can drive the
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

// NewObjectBackend returns a Backend over an in-memory S3 started for the test. Every manager
// gets a distinct NodeID, as distinct nodes sharing a bucket would in production.
func NewObjectBackend(t testing.TB, opts ...ObjectOption) *Backend {
	t.Helper()

	var o objectOptions
	for _, opt := range opts {
		opt(&o)
	}
	s3 := fake.NewS3(t)
	nodes := 0

	return &Backend{
		Name: "object",
		t:    t,
		config: func(queues map[string]int) fsm.Config {
			cfg := &fsm.ObjectStorageConfig{
				Bucket: s3.Bucket(),
				Client: s3.Client(),
				// The fake S3 answers instantly, so tight poll intervals keep object-backend
				// waits from eating the default floor per Wait.
				WaitPollInterval:    1 * time.Millisecond,
				WaitPollMaxInterval: 10 * time.Millisecond,
			}
			if o.configure != nil {
				o.configure(cfg)
			}
			nodes++
			return fsm.Config{
				ObjectStorage: cfg,
				NodeID:        fmt.Sprintf("node-%d", nodes),
				Queues:        queues,
				EventBus:      o.bus,
			}
		},
	}
}
