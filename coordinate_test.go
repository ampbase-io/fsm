package fsm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

func TestWithJitter(t *testing.T) {
	const d = time.Second
	for range 100 {
		j := withJitter(d)
		if j < 3*d/4 || j >= 5*d/4 {
			t.Fatalf("expected jitter within [0.75d, 1.25d), got %s", j)
		}
	}

	// Durations too small to jitter pass through instead of panicking rand.N.
	for _, d := range []time.Duration{0, 1} {
		if got := withJitter(d); got != d {
			t.Fatalf("expected %d passed through, got %s", d, got)
		}
	}
}

// stubCoordinator is a leaseCoordinator with a fixed set of held leases.
type stubCoordinator struct {
	held map[ulid.ULID]bool
}

func (s *stubCoordinator) extendLeases(context.Context) {}

func (s *stubCoordinator) claimRuns(context.Context, []*fsm) ([]claimedRun, error) {
	return nil, nil
}

func (s *stubCoordinator) owns(version ulid.ULID) bool { return s.held[version] }

func (s *stubCoordinator) coordinationIntervals() (time.Duration, time.Duration) {
	return time.Hour, time.Hour
}

// wakeCoordinator counts claim passes. It embeds stubCoordinator — whose intervals are an hour,
// so the periodic scan never fires and only the pending wakeup drives a pass — and inherits any
// methods the interface later grows, overriding only claimRuns to count.
type wakeCoordinator struct {
	stubCoordinator

	claimCount atomic.Int32
}

func (w *wakeCoordinator) claimRuns(context.Context, []*fsm) ([]claimedRun, error) {
	w.claimCount.Add(1)
	return nil, nil
}

func (w *wakeCoordinator) claims() int { return int(w.claimCount.Load()) }

// runWakeLoop starts a coordinate loop over a wakeCoordinator (hour-long cadence, so only the
// pending wakeup can drive a claim pass) and waits until it has subscribed to the pending
// subject. It returns the bus and coordinator for driving and asserting the wake.
func runWakeLoop(t *testing.T) (*testBus, *wakeCoordinator) {
	t.Helper()

	bus := newTestBus()
	lc := &wakeCoordinator{}
	m := &Manager{
		logger:  logrus.New(),
		bus:     bus,
		done:    make(chan struct{}),
		fsms:    map[fsmKey]*fsm{},
		running: map[ulid.ULID]context.CancelCauseFunc{},
	}

	loopDone := make(chan struct{})
	go func() { defer close(loopDone); m.coordinate(lc) }()
	t.Cleanup(func() { close(m.done); <-loopDone })

	eventually(t, 2*time.Second, func() bool {
		return bus.subscriberCount(subjectPending) >= 1
	}, "coordinate never subscribed to the pending subject")
	return bus, lc
}

func publishPending(bus *testBus) {
	bus.Publish(subjectPending, &fsmv1.RunEvent{Kind: fsmv1.RunEventKind_RUN_EVENT_KIND_PENDING})
}

// TestClaimWakeSurvivesPendingStream covers the wakeArmed guard, which the single-event consumer
// test cannot: a continuous stream of pending events faster than claimWakeDelay must not starve
// the claim pass. With the guard the first event arms the wake and it fires on schedule; without
// it, each event would reset the timer and the pass would never run.
func TestClaimWakeSurvivesPendingStream(t *testing.T) {
	bus, lc := runWakeLoop(t)

	stop, streamDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(streamDone)
		ticker := time.NewTicker(claimWakeDelay / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				publishPending(bus)
			}
		}
	}()
	defer func() { close(stop); <-streamDone }()

	eventually(t, 2*time.Second, func() bool { return lc.claims() >= 1 },
		"a continuous pending stream starved the claim pass — the wake never fired")
}

// TestClaimWakeRearmsAfterFiring covers the wakeArmed reset: after a wake fires, a later pending
// event must arm a fresh one. A guard left stuck would wake exactly once and ignore every
// subsequent event.
func TestClaimWakeRearmsAfterFiring(t *testing.T) {
	bus, lc := runWakeLoop(t)

	publishPending(bus)
	eventually(t, 2*time.Second, func() bool { return lc.claims() >= 1 }, "the first pending event never woke a claim pass")

	// After the first wake has fired, a fresh event must wake another pass.
	time.Sleep(claimWakeDelay + 150*time.Millisecond)
	publishPending(bus)
	eventually(t, 2*time.Second, func() bool { return lc.claims() >= 2 }, "a pending event after the first wake never re-armed it")
}

// TestCancelUnleased verifies the sweep's selectivity: exactly the executing runs whose lease
// is gone are canceled, with cause ErrLeaseLost.
func TestCancelUnleased(t *testing.T) {
	m := &Manager{
		logger:  logrus.New(),
		running: map[ulid.ULID]context.CancelCauseFunc{},
	}

	leased, unleased := ulid.Make(), ulid.Make()
	leasedCtx, leasedCancel := context.WithCancelCause(context.Background())
	unleasedCtx, unleasedCancel := context.WithCancelCause(context.Background())
	defer leasedCancel(nil)
	defer unleasedCancel(nil)
	m.running[leased] = leasedCancel
	m.running[unleased] = unleasedCancel

	m.cancelUnleased(&stubCoordinator{held: map[ulid.ULID]bool{leased: true}})

	if err := leasedCtx.Err(); err != nil {
		t.Fatalf("expected the leased run left running, got %v", err)
	}
	if unleasedCtx.Err() == nil {
		t.Fatal("expected the unleased run canceled")
	}
	if cause := context.Cause(unleasedCtx); !errors.Is(cause, ErrLeaseLost) {
		t.Fatalf("expected cancellation cause ErrLeaseLost, got %v", cause)
	}
}

// TestClaimPassContinuesPastFailedDispatch is the claim-loop twin of
// TestResumeContinuesPastFailedRun: a run whose dispatch fails must not stop the periodic
// claim pass from completing the runs claimed after it.
func TestClaimPassContinuesPastFailedDispatch(t *testing.T) {
	nodes := 0
	f := newObjectFactoryWith(t, func(cfg *ObjectStorageConfig) {
		nodes++
		cfg.LeaseTimeout = 150 * time.Millisecond
		cfg.HeartbeatPeriod = 75 * time.Millisecond
		cfg.ClaimInterval = 75 * time.Millisecond
		if nodes == 1 {
			cfg.HeartbeatPeriod = time.Hour
			cfg.ClaimInterval = time.Hour
		}
	})
	ctx := context.Background()

	m1, _ := f.newManager(nil)
	var (
		entered = make(chan struct{}, 2)
		block   = make(chan struct{})
	)
	defer close(block)
	start := blockingFSM(t, m1, "cpoison", entered, block)

	// cpoison-1 sorts before cpoison-2 in the lock listing, so the poisoned run is dispatched
	// first each pass and the healthy one only completes if the pass keeps going.
	v1, err := start(ctx, "cpoison-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered
	v2, err := start(ctx, "cpoison-2", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	store := m1.store.(*objectStore)
	if _, err := store.casManifest(ctx, v1, func(m *fsmv1.RunManifest) error {
		m.Resource = []byte("corrupt")
		return nil
	}); err != nil {
		t.Fatalf("failed to corrupt resource: %v", err)
	}

	// No explicit Resume: only m2's claim loop can complete the healthy run.
	m2, _ := f.newManager(nil)
	completingFSM(t, m2, "cpoison")

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, v2); err != nil {
		t.Fatalf("healthy run behind the poisoned one completed with error: %v", err)
	}
}

// TestCoordinateStopsPromptly pins that shutdown does not wait out heartbeat or claim ticks:
// the coordinate goroutine exits as soon as done closes.
func TestCoordinateStopsPromptly(t *testing.T) {
	f := newObjectFactoryWith(t, func(cfg *ObjectStorageConfig) {
		cfg.LeaseTimeout = time.Second
		cfg.HeartbeatPeriod = 20 * time.Millisecond
		cfg.ClaimInterval = 20 * time.Millisecond
	})
	m, stop := f.newManager(nil)
	completingFSM(t, m, "tick")
	time.Sleep(100 * time.Millisecond) // let a few heartbeat and claim ticks run

	begun := time.Now()
	stop()
	if elapsed := time.Since(begun); elapsed > 2*time.Second {
		t.Fatalf("shutdown with an active coordinate loop took %s", elapsed)
	}
}
