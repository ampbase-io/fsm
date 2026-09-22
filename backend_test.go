package fsm

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ampbase-io/fsm/fsmtest/fake"
)

// backend is one storage backend under test, handing out Managers over it. Successive
// newManager calls see state persisted by earlier managers, which is how restart/resume
// scenarios are modeled.
//
// This mirrors fsmtest.Backend; the in-package tests cannot import fsmtest (it imports fsm),
// so the type exists twice.
type backend struct {
	name string

	// configureManager, when set before a newManager call, adjusts the Config a manager is
	// created with — the logger, say — on top of the backend's own wiring.
	configureManager func(*Config)

	config func(queues map[string]int) Config

	t *testing.T
}

// newManager creates a manager with the given queue capacities. The returned stop function
// shuts the manager down and is safe to call more than once; it is also registered as a test
// cleanup.
func (b *backend) newManager(queues map[string]int) (*Manager, func()) {
	b.t.Helper()

	cfg := b.config(queues)
	if b.configureManager != nil {
		b.configureManager(&cfg)
	}
	m, err := New(cfg)
	if err != nil {
		b.t.Fatalf("failed to create %s manager: %v", b.name, err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { m.Shutdown(5 * time.Second) }) }
	b.t.Cleanup(stop)
	return m, stop
}

// runBackends runs the given scenario against both storage backends.
func runBackends(t *testing.T, fn func(t *testing.T, b *backend)) {
	t.Run("bolt", func(t *testing.T) { fn(t, newBoltBackend(t)) })
	t.Run("object", func(t *testing.T) { fn(t, newObjectBackend(t)) })
}

func newBoltBackend(t *testing.T) *backend {
	t.Helper()

	// The dir is created directly under /tmp to keep the admin unix socket path under the
	// sun_path length limit.
	dir, err := os.MkdirTemp("/tmp", "fsm-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	return &backend{
		name: "bolt",
		t:    t,
		config: func(queues map[string]int) Config {
			return Config{DBPath: dir, Queues: queues}
		},
	}
}

func newObjectBackend(t *testing.T) *backend {
	return newObjectBackendWith(t, nil)
}

// newObjectBackendWith lets a test adjust the object storage config (e.g. lease timings)
// before each manager is created. Every manager gets a distinct NodeID, as distinct nodes
// sharing a bucket would in production.
func newObjectBackendWith(t *testing.T, configure func(*ObjectStorageConfig)) *backend {
	return newObjectBackendWithBus(t, nil, configure)
}

// newObjectBackendWithBus is newObjectBackendWith with a shared EventBus injected into every
// manager, so a test can drive the cross-node fast paths (wait wakeup, claim wakeup). A nil bus
// leaves the no-op default in place.
func newObjectBackendWithBus(t *testing.T, bus EventBus, configure func(*ObjectStorageConfig)) *backend {
	t.Helper()

	s3 := fake.NewS3(t)
	nodes := 0

	return &backend{
		name: "object",
		t:    t,
		config: func(queues map[string]int) Config {
			cfg := &ObjectStorageConfig{
				Bucket: s3.Bucket(),
				Client: s3.Client(),
				// The fake S3 answers instantly, so tight poll intervals keep object-backend
				// waits from eating the default 100ms floor per Wait.
				WaitPollInterval:    1 * time.Millisecond,
				WaitPollMaxInterval: 10 * time.Millisecond,
			}
			if configure != nil {
				configure(cfg)
			}
			nodes++
			return Config{
				ObjectStorage: cfg,
				NodeID:        fmt.Sprintf("node-%d", nodes),
				Queues:        queues,
				EventBus:      bus,
			}
		},
	}
}
