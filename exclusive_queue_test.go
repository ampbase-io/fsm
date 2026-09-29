package fsm

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"connectrpc.com/connect"
	"github.com/oklog/ulid/v2"
	"go.etcd.io/bbolt"
)

// startExclusive starts an exclusive queued run and returns its version and error.
func startExclusive(start Start[orderReq, orderResp], id, queue string) (ulid.ULID, error) {
	return start(context.Background(), id, NewRequest(&orderReq{}, &orderResp{}), WithExclusiveQueue(queue))
}

// mustAlreadyRunning fails the test unless err is an *AlreadyRunningError naming version.
func mustAlreadyRunning(t *testing.T, err error, version ulid.ULID, what string) {
	t.Helper()
	are, ok := errors.AsType[*AlreadyRunningError](err)
	if !ok {
		t.Fatalf("%s: expected *AlreadyRunningError, got %v", what, err)
	}
	if are.Version != version {
		t.Fatalf("%s: expected the error to name %s, got %s", what, version, are.Version)
	}
}

// TestExclusiveQueueRefusesSecondStart verifies an exclusive queued run holds its resource: a
// second start of the same id is refused while the first is still pending admission, and again
// once it is admitted and running; a start on another id is admitted; and once the run finishes
// the id is free.
func TestExclusiveQueueRefusesSecondStart(t *testing.T) {
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		cfg.ClaimInterval = 50 * time.Millisecond
		cfg.HeartbeatPeriod = 100 * time.Millisecond
	})
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	// Capacity 0 holds every start pending: the lock must already refuse a second start.
	holder, _ := b.newManager(map[string]int{"pending": 0})
	pendingStart := blockingFSM(t, holder, "excl-pending", entered, release)

	first, err := startExclusive(pendingStart, "excl-1", "pending")
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	_, err = startExclusive(pendingStart, "excl-1", "pending")
	mustAlreadyRunning(t, err, first, "a second start while pending")

	// A different id is unaffected by the first run's lock.
	if _, err := startExclusive(pendingStart, "excl-2", "pending"); err != nil {
		t.Fatalf("a start on another id: %v", err)
	}

	// With capacity, the run is admitted and executes; the id stays held.
	m, _ := b.newManager(map[string]int{"deploys": 2})
	start := blockingFSM(t, m, "excl-running", entered, release)
	running, err := startExclusive(start, "excl-3", "deploys")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	within(t, entered, 10*time.Second, "the admitted run")
	_, err = startExclusive(start, "excl-3", "deploys")
	mustAlreadyRunning(t, err, running, "a second start while running")

	close(release)
	waitRun(t, m, running)

	// The finish deletes the lock the start took, which it builds from the manifest: the run's own
	// key is gone, not left for the next start to reap as a terminal one.
	store, ok := m.store.(*objectStore)
	if !ok {
		t.Fatalf("expected objectStore, got %T", m.store)
	}
	held := store.lockKey(Run{TypeName: "orderReq", ID: "excl-3", Action: "excl-running", Queue: "deploys", Exclusive: true})
	if !objectGone(t, store, held) {
		t.Fatalf("expected the finish to delete the lock at %s", held)
	}

	// A peer, which reads the lock from storage rather than from this manager's memory, can start.
	peer, _ := b.newManager(map[string]int{"deploys": 2})
	peerStart := blockingFSM(t, peer, "excl-running", entered, release)
	if _, err := startExclusive(peerStart, "excl-3", "deploys"); err != nil {
		t.Fatalf("a start after the run finished: %v", err)
	}
}

// TestExclusiveQueueAdmitsOneIDAtATime verifies exclusivity does not disturb admission: with
// capacity for one, an exclusive run on another id waits for the first to finish, then runs.
func TestExclusiveQueueAdmitsOneIDAtATime(t *testing.T) {
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		cfg.ClaimInterval = 50 * time.Millisecond
		cfg.HeartbeatPeriod = 100 * time.Millisecond
	})
	m, _ := b.newManager(map[string]int{"one": 1})

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	start := blockingFSM(t, m, "excl-cap", entered, release)

	a, err := startExclusive(start, "cap-a", "one")
	if err != nil {
		t.Fatalf("start a: %v", err)
	}
	within(t, entered, 10*time.Second, "the first run")

	bVersion, err := startExclusive(start, "cap-b", "one")
	if err != nil {
		t.Fatalf("start b: %v", err)
	}
	if !noSignal(entered) {
		t.Fatal("expected the second id to wait for capacity")
	}

	close(release)
	waitRun(t, m, a)
	within(t, entered, 10*time.Second, "the second run once capacity freed")
	waitRun(t, m, bVersion)
}

// TestExclusiveQueueHeldAcrossTakeover verifies the lock outlives its owner: when a peer takes the
// run over, a start on the id is still refused, and succeeds once the run finishes.
func TestExclusiveQueueHeldAcrossTakeover(t *testing.T) {
	b := newObjectBackendWith(t, crossManagerTimings)
	ctx := context.Background()

	entered := make(chan struct{}, 2)
	block := make(chan struct{})
	defer close(block)

	m1, stop1 := b.newManager(map[string]int{"takeover": 1})
	start := blockingFSM(t, m1, "excl-takeover", entered, block)
	version, err := startExclusive(start, "takeover-1", "takeover")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	within(t, entered, 10*time.Second, "the run on its first owner")
	stop1()

	// The peer registers the same action, so its start takes the same lock the live run holds.
	m2, _ := b.newManager(map[string]int{"takeover": 1})
	peerStart, resume := completingStart(t, m2, "excl-takeover")
	_, err = startExclusive(peerStart, "takeover-1", "takeover")
	mustAlreadyRunning(t, err, version, "a start on the peer before the takeover")

	if err := resume(ctx); err != nil {
		t.Fatalf("resume on the peer: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("the resumed run failed: %v", err)
	}

	if _, err := startExclusive(peerStart, "takeover-1", "takeover"); err != nil {
		t.Fatalf("a start after the resumed run finished: %v", err)
	}
}

// TestExclusiveQueueOverRPC verifies a start through the ingress with the exclusive option holds
// the id the same way, so an RPC caller gets what an embedded start does.
func TestExclusiveQueueOverRPC(t *testing.T) {
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		cfg.ClaimInterval = 50 * time.Millisecond
	})
	ctx := context.Background()
	m, _ := b.newManager(map[string]int{"rpcq": 0})
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	blockingFSM(t, m, "excl-rpc", entered, release)

	admin := &adminServer{m: m}
	startRPC := func() (*connect.Response[fsmv1.StartResponse], error) {
		return admin.Start(ctx, connect.NewRequest(&fsmv1.StartRequest{
			TypeName: "orderReq", Action: "excl-rpc", Id: "rpc-1", Resource: []byte(`{"Name":"x"}`),
			Options: &fsmv1.StartOptions{Queue: "rpcq", Exclusive: true},
		}))
	}

	first, err := startRPC()
	if err != nil {
		t.Fatalf("first start over RPC: %v", err)
	}
	_, err = startRPC()
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("expected AlreadyExists for a second start, got %v", err)
	}
	var connErr *connect.Error
	if !errors.As(err, &connErr) {
		t.Fatalf("expected a connect error, got %v", err)
	}
	version, ok := startDetailVersion(connErr)
	if !ok {
		t.Fatalf("expected the refusal to carry the live run's version, got %v", err)
	}
	if version != first.Msg.GetVersion() {
		t.Fatalf("expected the refusal to name %s, got %s", first.Msg.GetVersion(), version)
	}
}

// TestBoltExclusiveQueue verifies the BoltDB backend gives the option the same meaning: a second
// start of the id is refused, a resumed run keeps one ACTIVE key rather than writing a second
// beside it, and the id is still held after the resume.
func TestBoltExclusiveQueue(t *testing.T) {
	b := newBoltBackend(t)
	ctx := context.Background()

	entered := make(chan struct{}, 2)
	block := make(chan struct{})

	m1, stop1 := b.newManager(nil)
	start := blockingFSM(t, m1, "excl-bolt", entered, block)
	version, err := startExclusive(start, "bolt-1", "deploys")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	within(t, entered, 10*time.Second, "the run")
	_, err = startExclusive(start, "bolt-1", "deploys")
	mustAlreadyRunning(t, err, version, "a second start")
	stop1()
	close(block)

	// The resumed run rebuilds its key from the persisted options: a restored exclusive run must
	// append to the entry its start took, not write a second one beside it.
	m2, _ := b.newManager(nil)
	_, resume := completingStart(t, m2, "excl-bolt")
	if err := resume(ctx); err != nil {
		t.Fatalf("resume: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("the resumed run failed: %v", err)
	}
	if n := activeKeysFor(t, m2, "orderReq", "bolt-1", "excl-bolt"); n != 0 {
		t.Fatalf("expected the finished run to leave no ACTIVE key, got %d", n)
	}
}

// activeKeysFor counts the BoltDB ACTIVE entries for one resource and action.
func activeKeysFor(t *testing.T, m *Manager, typeName, id, action string) int {
	t.Helper()
	s, ok := m.store.(*boltStore)
	if !ok {
		t.Fatalf("expected boltStore, got %T", m.store)
	}
	prefix := bytes.Join([][]byte{[]byte(typeName), []byte(id), []byte(action)}, keySeparator)

	var n int
	if err := s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(activeBucket).Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			n++
		}
		return nil
	}); err != nil {
		t.Fatalf("count active keys: %v", err)
	}
	return n
}

// TestQueueOptionsLastWins verifies the queue options compose as any other start option: the last
// one given decides both the queue and whether the run holds its resource.
func TestQueueOptionsLastWins(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  []StartOptionsFn
		queue string
		want  bool
	}{
		{name: "exclusive", opts: []StartOptionsFn{WithExclusiveQueue("a")}, queue: "a", want: true},
		{name: "plain", opts: []StartOptionsFn{WithQueue("a")}, queue: "a"},
		{name: "plain after exclusive", opts: []StartOptionsFn{WithExclusiveQueue("a"), WithQueue("b")}, queue: "b"},
		{name: "exclusive after plain", opts: []StartOptionsFn{WithQueue("a"), WithExclusiveQueue("b")}, queue: "b", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts startOptions
			for _, o := range tc.opts {
				o(&opts)
			}
			if opts.queue != tc.queue || opts.exclusive != tc.want {
				t.Fatalf("expected queue %q exclusive %v, got %q and %v", tc.queue, tc.want, opts.queue, opts.exclusive)
			}
		})
	}
}

// TestBoltActiveReportsExclusive verifies BoltDB's in-memory snapshot of a resumed run carries its
// exclusivity, so ActiveChildren and ListActive describe it as it was started.
func TestBoltActiveReportsExclusive(t *testing.T) {
	b := newBoltBackend(t)
	ctx := context.Background()

	entered := make(chan struct{}, 2)
	block := make(chan struct{})
	m1, stop1 := b.newManager(nil)
	start := blockingFSM(t, m1, "excl-active", entered, block)
	version, err := startExclusive(start, "active-1", "deploys")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	within(t, entered, 10*time.Second, "the run")
	stop1()
	close(block)

	// A second manager reads the run back from the store, as a restarted process does.
	m2, _ := b.newManager(nil)
	completingStart(t, m2, "excl-active")
	active, err := m2.store.Active(ctx, fsmKey{typeName: "orderReq", action: "excl-active"})
	if err != nil {
		t.Fatalf("active: %v", err)
	}
	states, err := m2.store.ListActive(ctx)
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	i := slices.IndexFunc(states, func(rs runSnapshot) bool { return rs.StartVersion == version })
	if i < 0 {
		t.Fatalf("expected the run listed, got %+v", states)
	}
	if !states[i].Exclusive {
		t.Fatal("expected the listed run to report its exclusivity")
	}
	if len(active) != 1 {
		t.Fatalf("expected one active run, got %d", len(active))
	}
}

// TestPlainQueueStacksBesideExclusive pins the boundary of the guarantee: exclusivity is a
// property of the starts, not of the resource, so a plain WithQueue start of the same id takes a
// version-keyed lock and runs beside an exclusive one. Every start of a resource that must run
// alone has to pass WithExclusiveQueue.
func TestPlainQueueStacksBesideExclusive(t *testing.T) {
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		cfg.ClaimInterval = 50 * time.Millisecond
	})
	ctx := context.Background()
	m, _ := b.newManager(map[string]int{"mixed": 2})

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	start := blockingFSM(t, m, "excl-mixed", entered, release)

	if _, err := startExclusive(start, "mixed-1", "mixed"); err != nil {
		t.Fatalf("the exclusive start: %v", err)
	}
	within(t, entered, 10*time.Second, "the exclusive run")

	// Documented gap, not a guarantee: the plain start's lock carries its version, so it does not
	// collide with the resource lock the exclusive run holds.
	if _, err := start(ctx, "mixed-1", NewRequest(&orderReq{}, &orderResp{}), WithQueue("mixed")); err != nil {
		t.Fatalf("a plain queued start beside an exclusive run: %v", err)
	}
	within(t, entered, 10*time.Second, "the stacked run")
}

// declaredFSM registers an FSM that blocks in its first transition and declares itself exclusive
// on queue.
func declaredFSM(t *testing.T, m *Manager, action, queue string, entered chan<- struct{}, release <-chan struct{}) Start[orderReq, orderResp] {
	t.Helper()
	return blockingFSM(t, m, action, entered, release, RunsExclusively[orderReq, orderResp](queue))
}

// TestRunsExclusivelyHonoredByEveryStart verifies a declared FSM's runs hold their resource
// however they are started: a start passing nothing adopts the declaration, over the typed start
// and the admin RPC alike, and a second start of the id is refused by either path.
func TestRunsExclusivelyHonoredByEveryStart(t *testing.T) {
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		cfg.ClaimInterval = 50 * time.Millisecond
	})
	ctx := context.Background()
	m, _ := b.newManager(map[string]int{"declared": 2})

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	start := declaredFSM(t, m, "excl-declared", "declared", entered, release)

	// Silence adopts the declaration: the run is queued and holds its resource.
	version := startOrder(t, start, "declared-1")
	within(t, entered, 10*time.Second, "the declared run")
	run, err := m.store.liveRun(ctx, version)
	if err != nil {
		t.Fatalf("live run: %v", err)
	}
	if run.Queue != "declared" || !run.Exclusive {
		t.Fatalf("expected the run recorded queued and exclusive, got %+v", run)
	}

	// A matching option is accepted as an option; a second start is refused by the lock.
	_, err = startExclusive(start, "declared-1", "declared")
	mustAlreadyRunning(t, err, version, "a matching second start")

	admin := &adminServer{m: m}
	_, err = admin.Start(ctx, connect.NewRequest(&fsmv1.StartRequest{
		TypeName: "orderReq", Action: "excl-declared", Id: "declared-1", Resource: []byte(`{"Name":"x"}`),
	}))
	if connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("expected an RPC start with no options refused by the lock, got %v", err)
	}
}

// TestRunsExclusivelyRefusesConflictingOptions verifies a start whose queue options contradict the
// declaration is refused rather than silently overridden, on both start paths.
func TestRunsExclusivelyRefusesConflictingOptions(t *testing.T) {
	b := newObjectBackendWith(t, nil)
	ctx := context.Background()
	m, _ := b.newManager(map[string]int{"declared": 1, "other": 1})

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	start := declaredFSM(t, m, "excl-conflict", "declared", entered, release)

	for name, opt := range map[string]StartOptionsFn{
		"plain queue":     WithQueue("declared"),
		"another queue":   WithExclusiveQueue("other"),
		"no queue at all": WithQueue(""),
		"plain elsewhere": WithQueue("other"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := start(ctx, "conflict-"+name, NewRequest(&orderReq{}, &orderResp{}), opt)
			if !errors.Is(err, errQueueConflict) {
				t.Fatalf("expected errQueueConflict, got %v", err)
			}
		})
	}

	admin := &adminServer{m: m}
	_, err := admin.Start(ctx, connect.NewRequest(&fsmv1.StartRequest{
		TypeName: "orderReq", Action: "excl-conflict", Id: "conflict-rpc", Resource: []byte(`{"Name":"x"}`),
		Options: &fsmv1.StartOptions{Queue: "declared"},
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument for a plain queued RPC start, got %v", err)
	}
}

// TestRunsExclusivelyRequiresAConfiguredQueue verifies Build refuses a declaration naming a queue
// the Manager has no capacity for: under the object backend no node would admit such a run, and
// its resource lock would block the id until the queue was configured.
func TestRunsExclusivelyRequiresAConfiguredQueue(t *testing.T) {
	m := newTestManager(t)
	_, _, err := m.Register[orderReq, orderResp]("excl-unconfigured").
		Start("only", okTransition).
		End("done", RunsExclusively[orderReq, orderResp]("nowhere")).
		Build(context.Background())
	if !errors.Is(err, errQueueConflict) {
		t.Fatalf("expected the build refused for an unconfigured queue, got %v", err)
	}
}

// TestRunsExclusivelyPassesOtherOptions verifies the resolver touches only the queue: a start that
// says nothing about queueing keeps its other options and adopts the declaration.
func TestRunsExclusivelyPassesOtherOptions(t *testing.T) {
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		cfg.ClaimInterval = 50 * time.Millisecond
	})
	ctx := context.Background()
	m, _ := b.newManager(map[string]int{"declared": 2})

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	start := declaredFSM(t, m, "excl-parent", "declared", entered, release)

	parent := ulid.Make()
	version, err := start(ctx, "parent-child", NewRequest(&orderReq{}, &orderResp{}), WithParent(parent))
	if err != nil {
		t.Fatalf("start with a parent: %v", err)
	}
	within(t, entered, 10*time.Second, "the child run")

	run, err := m.store.liveRun(ctx, version)
	if err != nil {
		t.Fatalf("live run: %v", err)
	}
	if run.Parent != parent {
		t.Fatalf("expected the parent kept, got %s", run.Parent)
	}
	if run.Queue != "declared" || !run.Exclusive {
		t.Fatalf("expected the declaration adopted beside it, got %+v", run)
	}
}

// TestRunsExclusivelyLeavesPendingRunsAlone verifies a plain queued run still waiting for
// admission when its definition becomes exclusive is admitted under its stored options: it stacks,
// takes no resource lock, and runs to completion.
func TestRunsExclusivelyLeavesPendingRunsAlone(t *testing.T) {
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		cfg.ClaimInterval = 50 * time.Millisecond
		cfg.HeartbeatPeriod = 100 * time.Millisecond
	})
	ctx := context.Background()

	// Started before the declaration, and parked: the node configures no capacity for the queue.
	starter, _ := b.newManager(map[string]int{"drain": 0})
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	old := blockingFSM(t, starter, "excl-drain", entered, release)
	pending, err := old(ctx, "drain-1", NewRequest(&orderReq{}, &orderResp{}), WithQueue("drain"))
	if err != nil {
		t.Fatalf("the pre-existing start: %v", err)
	}

	// The upgraded node declares the FSM exclusive and has capacity, so its claim loop admits the
	// pending run under the options it was started with.
	upgraded, _ := b.newManager(map[string]int{"drain": 1})
	completing, _, err := upgraded.Register[orderReq, orderResp]("excl-drain").
		Start("created", okTransition).
		End("done", RunsExclusively[orderReq, orderResp]("drain")).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build the declared FSM: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := upgraded.Wait(waitCtx, pending); err != nil {
		t.Fatalf("the drained run failed: %v", err)
	}

	store, ok := upgraded.store.(*objectStore)
	if !ok {
		t.Fatalf("expected objectStore, got %T", upgraded.store)
	}
	resource := store.lockKey(Run{TypeName: "orderReq", ID: "drain-1", Action: "excl-drain"})
	if !objectGone(t, store, resource) {
		t.Fatal("expected the stacking run never to have taken the resource lock")
	}
	// The id is free for a declared start once the drained run is done.
	if _, err := completing(ctx, "drain-1", NewRequest(&orderReq{}, &orderResp{})); err != nil {
		t.Fatalf("a declared start after the drain: %v", err)
	}
}
