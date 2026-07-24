package fsm

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// testBus is an in-process EventBus for exercising the fast paths. It fans a publish out to the
// matching subscribers asynchronously (a slow subscriber never blocks the publisher, per the
// contract) and records every publish for assertions.
type testBus struct {
	mu        sync.Mutex
	subs      map[string]map[int]func(*fsmv1.RunEvent)
	nextID    int
	published []publishedEvent
}

type publishedEvent struct {
	subject string
	event   *fsmv1.RunEvent
}

func newTestBus() *testBus {
	return &testBus{subs: map[string]map[int]func(*fsmv1.RunEvent){}}
}

func (b *testBus) Publish(subject string, e *fsmv1.RunEvent) {
	b.mu.Lock()
	b.published = append(b.published, publishedEvent{subject: subject, event: e})
	fns := make([]func(*fsmv1.RunEvent), 0, len(b.subs[subject]))
	for _, fn := range b.subs[subject] {
		fns = append(fns, fn)
	}
	b.mu.Unlock()

	for _, fn := range fns {
		go fn(e)
	}
}

func (b *testBus) Subscribe(subject string, fn func(*fsmv1.RunEvent)) (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subs[subject] == nil {
		b.subs[subject] = map[int]func(*fsmv1.RunEvent){}
	}
	id := b.nextID
	b.nextID++
	b.subs[subject][id] = fn

	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.subs[subject], id)
	}, nil
}

// subscriberCount reports how many live subscriptions a subject has, so a test can wait for a
// subscriber to register before publishing rather than racing it with a sleep.
func (b *testBus) subscriberCount(subject string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs[subject])
}

// hasPublished reports whether an event of the given kind was published on the subject for the run.
func (b *testBus) hasPublished(kind fsmv1.RunEventKind, subject string, version ulid.ULID) bool {
	want, _ := version.MarshalText()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.published {
		if p.subject == subject && p.event.GetKind() == kind && bytes.Equal(p.event.GetRunVersion(), want) {
			return true
		}
	}
	return false
}

// eventually fails the test unless cond becomes true within d.
func eventually(t *testing.T, d time.Duration, cond func() bool, msg string) {
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

// TestEventBusPublishesLifecycle verifies the publish points fire against the durable writes:
// fsm.run.pending at start and fsm.run.done at the end of finish cleanup, both keyed to the run
// version.
func TestEventBusPublishesLifecycle(t *testing.T) {
	bus := newTestBus()
	f := newObjectFactoryWithBus(t, bus, nil)
	ctx := context.Background()

	m, _ := f.newManager(nil)
	start, _, err := m.Register[orderReq, orderResp]("bus-life").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return NewResponse(&orderResp{Status: "ok"}), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "life-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	if err := m.Wait(ctx, version); err != nil {
		t.Fatalf("run completed with error: %v", err)
	}

	if !bus.hasPublished(fsmv1.RunEventKind_RUN_EVENT_KIND_PENDING, subjectPending, version) {
		t.Fatal("expected a pending event published at start")
	}
	// done publishes just after the waiter is released, so give it a moment to land.
	eventually(t, 2*time.Second, func() bool {
		return bus.hasPublished(fsmv1.RunEventKind_RUN_EVENT_KIND_DONE, doneSubject(version), version)
	}, "expected a done event published at finish cleanup")
}

// TestWaitWakesOnBusDone verifies WaitRun's fast path: a waiter parked on a poll floor far
// longer than the test is released by the fsm.run.done event, cross-node, the moment the run's
// finish cleanup lands.
func TestWaitWakesOnBusDone(t *testing.T) {
	bus := newTestBus()
	f := newObjectFactoryWithBus(t, bus, func(cfg *ObjectStorageConfig) {
		// A poll floor far beyond the test window, so only the bus can deliver completion in time.
		cfg.WaitPollInterval = 30 * time.Second
		cfg.WaitPollMaxInterval = 30 * time.Second
	})
	ctx := context.Background()

	m1, _ := f.newManager(nil)
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	start := blockingFSM(t, m1, "waitwake", entered, block)

	version, err := start(ctx, "waitwake-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	// A second node waits on the still-blocked run: its first manifest read is non-terminal, so
	// it parks on the 30s poll floor and only fsm.run.done can release it in time.
	m2, _ := f.newManager(nil)
	waitErr := make(chan error, 1)
	go func() { waitErr <- m2.Wait(ctx, version) }()

	// The run stays blocked, so once the waiter has subscribed and polled once it is parked on
	// the 30s floor for certain; a short pause guarantees that first non-terminal poll lands
	// before completion, so the run cannot read terminal without the bus. (Without it, close
	// could race ahead of poll #1 and the manifest would already be terminal — the bus untested.)
	eventually(t, 2*time.Second, func() bool {
		return bus.subscriberCount(doneSubject(version)) >= 1
	}, "waiter never subscribed to the done subject")
	time.Sleep(200 * time.Millisecond)

	close(block) // m1's run completes and publishes fsm.run.done
	started := time.Now()

	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("wait returned error: %v", err)
		}
		if elapsed := time.Since(started); elapsed > 5*time.Second {
			t.Fatalf("wait took %v; the poll floor delivered, not the bus fast path", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("wait never returned; the done event did not wake it")
	}
}

// TestClaimWakesOnPending verifies the claim wakeup: with the periodic scan an hour out, a
// fsm.run.pending event is the only thing that can pull a claim pass forward, and it drives an
// idle worker to take over and complete a claimable run.
func TestClaimWakesOnPending(t *testing.T) {
	bus := newTestBus()
	nodes := 0
	f := newObjectFactoryWithBus(t, bus, func(cfg *ObjectStorageConfig) {
		nodes++
		cfg.LeaseTimeout = 150 * time.Millisecond
		cfg.ClaimInterval = time.Hour // the periodic scan is disabled; only the wakeup claims
		cfg.HeartbeatPeriod = 75 * time.Millisecond
		if nodes == 1 {
			cfg.HeartbeatPeriod = time.Hour // node-1's lease lapses, leaving the run claimable
		}
	})
	ctx := context.Background()

	m1, _ := f.newManager(nil)
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	defer close(block)
	start := blockingFSM(t, m1, "claimwake", entered, block)

	version, err := start(ctx, "claimwake-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered

	time.Sleep(300 * time.Millisecond) // let node-1's undefended lease lapse

	m2, _ := f.newManager(nil)
	completingFSM(t, m2, "claimwake")

	// Both coordinate loops subscribe to pending; wait until node-2's is registered so its
	// wakeup cannot be missed, then broadcast that a run is claimable.
	eventually(t, 2*time.Second, func() bool {
		return bus.subscriberCount(subjectPending) >= 2
	}, "node-2 never subscribed to the pending subject")
	bus.Publish(subjectPending, &fsmv1.RunEvent{Kind: fsmv1.RunEventKind_RUN_EVENT_KIND_PENDING})

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("claimed run completed with error: %v", err)
	}
}
