package fsmtest

import (
	"testing"
	"time"

	"github.com/ampbase-io/fsm"
)

// AsymmetricTimings gives the first manager a Backend creates a lapsing lease (a heartbeat of
// ownerHeartbeat against a 150ms lease, and no claim loop) and every later manager aggressive
// claim timings, so a second manager deterministically takes over the first's runs. A heartbeat
// longer than the lease leaves a takeover window on every extension; time.Hour never defends
// the lease at all. Pass it to NewObjectBackend through WithObjectConfig.
func AsymmetricTimings(ownerHeartbeat time.Duration) func(*fsm.ObjectStorageConfig) {
	nodes := 0
	return func(cfg *fsm.ObjectStorageConfig) {
		nodes++
		cfg.LeaseTimeout = 150 * time.Millisecond
		if nodes == 1 {
			cfg.HeartbeatPeriod = ownerHeartbeat
			cfg.ClaimInterval = time.Hour
			return
		}
		cfg.HeartbeatPeriod = 75 * time.Millisecond
		cfg.ClaimInterval = 75 * time.Millisecond
	}
}

// Eventually fails the test unless cond becomes true within d, polling every few milliseconds.
func Eventually(t testing.TB, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}
