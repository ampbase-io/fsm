package fsm

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ampbase-io/fsm/fsmtest/fake"
)

// managerFactory creates Managers over a shared storage backend. Successive newManager calls
// see state persisted by earlier managers, which is how restart/resume scenarios are modeled.
// The returned stop function shuts the manager down and is safe to call more than once; it is
// also registered as a test cleanup.
type managerFactory struct {
	name       string
	newManager func(queues map[string]int) (*Manager, func())

	// configureManager, when set before a newManager call, adjusts the Config a manager is
	// created with — the logger, say — on top of the backend the factory wires.
	configureManager func(*Config)
}

// newManagerFrom applies the factory's Config hook and creates the manager.
func (f *managerFactory) newManagerFrom(t *testing.T, cfg Config) (*Manager, func()) {
	t.Helper()
	if f.configureManager != nil {
		f.configureManager(&cfg)
	}
	m, err := New(cfg)
	return f.manage(t, m, err)
}

// runBackends runs the given scenario against both storage backends.
func runBackends(t *testing.T, fn func(t *testing.T, f *managerFactory)) {
	t.Run("bolt", func(t *testing.T) { fn(t, newBoltFactory(t)) })
	t.Run("object", func(t *testing.T) { fn(t, newObjectFactory(t)) })
}

func (f *managerFactory) manage(t *testing.T, m *Manager, err error) (*Manager, func()) {
	t.Helper()
	if err != nil {
		t.Fatalf("failed to create %s manager: %v", f.name, err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { m.Shutdown(5 * time.Second) }) }
	t.Cleanup(stop)
	return m, stop
}

func newBoltFactory(t *testing.T) *managerFactory {
	t.Helper()

	// The dir is created directly under /tmp to keep the admin unix socket path under the
	// sun_path length limit.
	dir, err := os.MkdirTemp("/tmp", "fsm-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	f := &managerFactory{name: "bolt"}
	f.newManager = func(queues map[string]int) (*Manager, func()) {
		return f.newManagerFrom(t, Config{DBPath: dir, Queues: queues})
	}
	return f
}

func newObjectFactory(t *testing.T) *managerFactory {
	return newObjectFactoryWith(t, nil)
}

// newObjectFactoryWith lets a test adjust the object storage config (e.g. lease timings)
// before each manager is created. Every manager gets a distinct NodeID, as distinct nodes
// sharing a bucket would in production.
func newObjectFactoryWith(t *testing.T, configure func(*ObjectStorageConfig)) *managerFactory {
	return newObjectFactoryWithBus(t, nil, configure)
}

// newObjectFactoryWithBus is newObjectFactoryWith with a shared EventBus injected into every
// manager, so a test can drive the cross-node fast paths (wait wakeup, claim wakeup). A nil bus
// leaves the no-op default in place.
//
// This mirrors fsmtest.NewObjectFactory; the in-package tests cannot import fsmtest (it imports
// fsm), so the factory exists twice.
func newObjectFactoryWithBus(t *testing.T, bus EventBus, configure func(*ObjectStorageConfig)) *managerFactory {
	t.Helper()

	s3 := fake.NewS3(t)

	f := &managerFactory{name: "object"}
	nodes := 0
	f.newManager = func(queues map[string]int) (*Manager, func()) {
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
		return f.newManagerFrom(t, Config{
			ObjectStorage: cfg,
			NodeID:        fmt.Sprintf("node-%d", nodes),
			Queues:        queues,
			EventBus:      bus,
		})
	}
	return f
}
