package fsm

import (
	"fmt"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// managerFactory creates Managers over a shared storage backend. Successive newManager calls
// see state persisted by earlier managers, which is how restart/resume scenarios are modeled.
// The returned stop function shuts the manager down and is safe to call more than once; it is
// also registered as a test cleanup.
type managerFactory struct {
	name       string
	newManager func(queues map[string]int) (*Manager, func())
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
		m, err := New(Config{DBPath: dir, Queues: queues})
		return f.manage(t, m, err)
	}
	return f
}

func newObjectFactory(t *testing.T) *managerFactory {
	return newObjectFactoryWith(t, nil)
}

// startFakeS3 boots the in-process S3 server and credentials every object-backend test needs.
func startFakeS3(t *testing.T) (bucket, url string, fake *fakeS3) {
	t.Helper()

	const b = "test-bucket"
	fake = newFakeS3()
	server := httptest.NewServer(fake.handler(b))
	t.Cleanup(server.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	return b, server.URL, fake
}

// newObjectFactoryWith lets a test adjust the object storage config (e.g. lease timings)
// before each manager is created. Every manager gets a distinct NodeID, as distinct nodes
// sharing a bucket would in production.
func newObjectFactoryWith(t *testing.T, configure func(*ObjectStorageConfig)) *managerFactory {
	t.Helper()

	bucket, url, _ := startFakeS3(t)

	f := &managerFactory{name: "object"}
	nodes := 0
	f.newManager = func(queues map[string]int) (*Manager, func()) {
		cfg := &ObjectStorageConfig{
			Bucket:   bucket,
			Endpoint: url,
			Region:   "auto",
			// The fake S3 answers instantly, so tight poll intervals keep object-backend
			// waits from eating the default 100ms floor per Wait.
			WaitPollInterval:    1 * time.Millisecond,
			WaitPollMaxInterval: 10 * time.Millisecond,
		}
		if configure != nil {
			configure(cfg)
		}
		nodes++
		m, err := New(Config{
			ObjectStorage: cfg,
			NodeID:        fmt.Sprintf("node-%d", nodes),
			Queues:        queues,
		})
		return f.manage(t, m, err)
	}
	return f
}
