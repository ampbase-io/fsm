package fsm

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"connectrpc.com/connect"
	"github.com/oklog/ulid/v2"
)

// command is the test signals' payload; pause and advance share it.
type command struct {
	Stage int
	Note  string
}

var (
	testPause   = NewSignal[command]("pause")
	testAdvance = NewSignal[command]("advance")
)

func acceptTestSignals() EndOption[orderReq, orderResp] {
	return WithSignals[orderReq, orderResp](testPause, testAdvance)
}

// noSignal reports whether nothing arrives on ch within a short wait.
func noSignal[T any](ch <-chan Received[T]) bool {
	select {
	case <-ch:
		return false
	case <-time.After(200 * time.Millisecond):
		return true
	}
}

// waitRun waits for the run and fails the test unless it succeeded.
func waitRun(t *testing.T, m *Manager, version ulid.ULID) {
	t.Helper()
	if err := waitFor(m, version); err != nil {
		t.Fatalf("run failed: %v", err)
	}
}

// TestSignalRoundTrip verifies a signal sent to a running run reaches the transition reading its
// name, typed, with the ID Send returned.
func TestSignalRoundTrip(t *testing.T) { runBackends(t, testSignalRoundTrip) }

func testSignalRoundTrip(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	entered := make(chan struct{}, 1)
	got := make(chan Received[command], 1)
	start, _, err := m.Register[orderReq, orderResp]("signal-round-trip").
		Start("wait", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			select {
			case adv := <-testAdvance.Receive(req):
				got <- adv
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).
		End("done", acceptTestSignals()).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version := startOrder(t, start, "signal-1")
	within(t, entered, 10*time.Second, "the transition")
	id, err := testAdvance.Send(ctx, m, version, &command{Stage: 2, Note: "go"})
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	adv := within(t, got, 10*time.Second, "the signal")
	waitRun(t, m, version)

	if adv.ID != id {
		t.Fatalf("expected signal %s, got %s", id, adv.ID)
	}
	if *adv.Msg != (command{Stage: 2, Note: "go"}) {
		t.Fatalf("unexpected payload %+v", adv.Msg)
	}
	if time.Since(adv.SentAt) > time.Minute {
		t.Fatalf("unexpected send time %s", adv.SentAt)
	}
}

// TestSignalWaitsForItsReader verifies signals wait for a transition that reads their name: one
// that reads nothing consumes none by completing, and reading one name leaves another pending.
func TestSignalWaitsForItsReader(t *testing.T) { runBackends(t, testSignalWaitsForItsReader) }

func testSignalWaitsForItsReader(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var (
		mu    sync.Mutex
		notes []string
	)
	note := func(r Received[command]) {
		mu.Lock()
		defer mu.Unlock()
		notes = append(notes, r.Msg.Note)
	}
	start, _, err := m.Register[orderReq, orderResp]("signal-waits").
		Start("hold", func(ctx context.Context, _ *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			<-release
			return nil, nil
		}).
		To("advance", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			note(<-testAdvance.Receive(req))
			return nil, nil
		}).
		To("pause", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			note(<-testPause.Receive(req))
			if !noSignal(testAdvance.Receive(req)) {
				return nil, Abort(errors.New("advance delivered twice"))
			}
			return nil, nil
		}).
		End("done", acceptTestSignals()).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version := startOrder(t, start, "signal-2")
	within(t, entered, 10*time.Second, "the holding transition")
	if _, err := testPause.Send(ctx, m, version, &command{Note: "pause"}); err != nil {
		t.Fatalf("send pause: %v", err)
	}
	if _, err := testAdvance.Send(ctx, m, version, &command{Note: "advance"}); err != nil {
		t.Fatalf("send advance: %v", err)
	}
	close(release)
	waitRun(t, m, version)

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(notes, []string{"advance", "pause"}) {
		t.Fatalf("expected advance then pause, each by its own reader, got %v", notes)
	}
}

// TestSignalRedeliveredOnRetry verifies a signal received by an attempt that failed is received
// again by the retry, with the same ID.
func TestSignalRedeliveredOnRetry(t *testing.T) { runBackends(t, testSignalRedeliveredOnRetry) }

func testSignalRedeliveredOnRetry(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	entered := make(chan struct{}, 1)
	ids := make(chan ulid.ULID, 2)
	var attempts atomic.Int32
	start, _, err := m.Register[orderReq, orderResp]("signal-retry").
		Start("wait", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			if attempts.Add(1) == 1 {
				entered <- struct{}{}
			}
			adv := <-testAdvance.Receive(req)
			ids <- adv.ID
			if attempts.Load() == 1 {
				return nil, errors.New("failed after reading")
			}
			return nil, nil
		}).
		End("done", acceptTestSignals()).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version := startOrder(t, start, "signal-3")
	within(t, entered, 10*time.Second, "the transition")
	id, err := testAdvance.Send(ctx, m, version, &command{})
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	waitRun(t, m, version)

	first := within(t, ids, time.Second, "the first attempt's signal")
	second := within(t, ids, time.Second, "the retry's signal")
	if first != id || second != id {
		t.Fatalf("expected the retry to receive %s again, got %s then %s", id, first, second)
	}
}

// TestSignalAcrossRestart verifies a run resumed on another manager receives the signals its
// previous owner received but did not complete, and not those a completed transition consumed.
func TestSignalAcrossRestart(t *testing.T) { runBackends(t, testSignalAcrossRestart) }

func testSignalAcrossRestart(t *testing.T, b *backend) {
	ctx := context.Background()
	var block atomic.Bool
	block.Store(true)
	entered := make(chan struct{}, 2)
	ids := make(chan ulid.ULID, 2)
	pauseAgain := make(chan bool, 1)
	done := make(chan struct{}, 1)

	register := func(m *Manager) (Start[orderReq, orderResp], Resume) {
		start, resume, err := m.Register[orderReq, orderResp]("signal-restart").
			Start("paused", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				<-testPause.Receive(req)
				return nil, nil
			}).
			To("wait", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				entered <- struct{}{}
				adv := <-testAdvance.Receive(req)
				ids <- adv.ID
				if block.Load() {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				pauseAgain <- !noSignal(testPause.Receive(req))
				return nil, nil
			}).
			End("done", acceptTestSignals(), WithFinalizers(func(context.Context, *Request[orderReq, orderResp], RunErr) {
				done <- struct{}{}
			})).
			Build(ctx)
		if err != nil {
			t.Fatalf("failed to build FSM: %v", err)
		}
		return start, resume
	}

	m1, stop1 := b.newManager(nil)
	start, _ := register(m1)
	version := startOrder(t, start, "signal-4")
	if _, err := testPause.Send(ctx, m1, version, &command{}); err != nil {
		t.Fatalf("send pause: %v", err)
	}
	within(t, entered, 10*time.Second, "the waiting transition")
	id, err := testAdvance.Send(ctx, m1, version, &command{})
	if err != nil {
		t.Fatalf("send advance: %v", err)
	}
	if got := within(t, ids, 10*time.Second, "the first owner's advance"); got != id {
		t.Fatalf("expected %s, got %s", id, got)
	}

	stop1()
	block.Store(false)
	m2, _ := b.newManager(nil)
	_, resume := register(m2)
	if err := resume(ctx); err != nil {
		t.Fatalf("failed to resume: %v", err)
	}
	within(t, done, 10*time.Second, "the resumed run to finish")

	if got := within(t, ids, time.Second, "the resumed advance"); got != id {
		t.Fatalf("expected the resumed transition to receive %s again, got %s", id, got)
	}
	if within(t, pauseAgain, time.Second, "the pause check") {
		t.Fatal("expected the consumed pause not to be delivered again")
	}
}

// TestSignalRefused verifies what a send refuses: an unknown or finished run, an undeclared name,
// a payload that does not decode, and a run whose FSM accepts no signals.
func TestSignalRefused(t *testing.T) { runBackends(t, testSignalRefused) }

func testSignalRefused(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	start := blockingFSM(t, m, "signal-refused", entered, release, WithSignals[orderReq, orderResp](testAdvance))
	version := startOrder(t, start, "signal-5")
	within(t, entered, 10*time.Second, "the transition")

	if _, err := testAdvance.Send(ctx, m, ulid.Make(), &command{}); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("unknown run: expected ErrFsmNotFound, got %v", err)
	}
	if _, err := testPause.Send(ctx, m, version, &command{}); !errors.Is(err, errSignalNotDeclared) {
		t.Fatalf("undeclared name: expected errSignalNotDeclared, got %v", err)
	}

	admin := &adminServer{m: m}
	_, err := admin.Signal(ctx, connect.NewRequest(&fsmv1.SignalRequest{Version: version.String(), Name: "advance", Payload: []byte("not json")}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("bad payload: expected InvalidArgument, got %v", err)
	}
	resp, err := admin.Signal(ctx, connect.NewRequest(&fsmv1.SignalRequest{Version: version.String(), Name: "advance", Payload: []byte(`{"Stage":1}`)}))
	if err != nil {
		t.Fatalf("valid RPC signal refused: %v", err)
	}
	if _, err := ulid.Parse(resp.Msg.GetId()); err != nil {
		t.Fatalf("expected the signal's ID, got %q", resp.Msg.GetId())
	}

	close(release)
	waitRun(t, m, version)
	if _, err := testAdvance.Send(ctx, m, version, &command{}); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("finished run: expected ErrFsmNotFound, got %v", err)
	}
}

// TestSignalDeclarations verifies an FSM refuses a signal declared twice or with no name, and
// that Receive panics, with a message, for a run whose FSM does not accept the signal.
func TestSignalDeclarations(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	for name, sigs := range map[string][]AnySignal{
		"duplicate": {testAdvance, NewSignal[command]("advance")},
		"unnamed":   {NewSignal[command]("")},
	} {
		_, _, err := m.Register[orderReq, orderResp]("signal-decl-"+name).
			Start("only", okTransition).
			End("done", WithSignals[orderReq, orderResp](sigs...)).
			Build(ctx)
		if err == nil {
			t.Fatalf("%s: expected the build to fail", name)
		}
	}

	defer func() {
		if recover() == nil {
			t.Fatal("expected Receive to panic for a run that accepts no signals")
		}
	}()
	testAdvance.Receive(MockRequest(NewRequest(&orderReq{}, &orderResp{}), slog.Default(), Run{}))
}

// TestObjectSignalAcrossNodes verifies a signal sent on one node reaches the run executing on
// another through the owner's heartbeat sweep.
func TestObjectSignalAcrossNodes(t *testing.T) {
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		cfg.HeartbeatPeriod = 50 * time.Millisecond
	})
	ctx := context.Background()

	entered := make(chan struct{}, 1)
	got := make(chan ulid.ULID, 1)
	register := func(m *Manager) Start[orderReq, orderResp] {
		start, _, err := m.Register[orderReq, orderResp]("signal-nodes").
			Start("wait", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				entered <- struct{}{}
				select {
				case adv := <-testAdvance.Receive(req):
					got <- adv.ID
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}).
			End("done", acceptTestSignals()).
			Build(ctx)
		if err != nil {
			t.Fatalf("failed to build FSM: %v", err)
		}
		return start
	}

	owner, _ := b.newManager(nil)
	version := startOrder(t, register(owner), "signal-6")
	within(t, entered, 10*time.Second, "the transition")

	peer, _ := b.newManager(nil)
	register(peer)
	id, err := testAdvance.Send(ctx, peer, version, &command{})
	if err != nil {
		t.Fatalf("send from the peer failed: %v", err)
	}
	if delivered := within(t, got, 10*time.Second, "the signal on the owner"); delivered != id {
		t.Fatalf("expected %s, got %s", id, delivered)
	}
	waitRun(t, owner, version)
}

// TestSignalUnacceptedNameSkipped verifies a stored signal whose name the running definition no
// longer accepts, left by an earlier definition, is skipped, not offered: the run's other
// signals are still delivered.
func TestSignalUnacceptedNameSkipped(t *testing.T) { runBackends(t, testSignalUnacceptedNameSkipped) }

func testSignalUnacceptedNameSkipped(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	entered := make(chan struct{}, 1)
	got := make(chan ulid.ULID, 1)
	start, _, err := m.Register[orderReq, orderResp]("signal-retired").
		Start("wait", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			select {
			case adv := <-testAdvance.Receive(req):
				got <- adv.ID
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).
		End("done", acceptTestSignals()).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	version := startOrder(t, start, "signal-7")
	within(t, entered, 10*time.Second, "the transition")

	run, err := m.store.liveRun(ctx, version)
	if err != nil {
		t.Fatalf("live run: %v", err)
	}
	retired := &fsmv1.Signal{Id: ulid.Make().String(), Name: "retired", Payload: []byte("{}")}
	if err := m.store.recordSignal(ctx, run, retired); err != nil {
		t.Fatalf("record a signal of a retired name: %v", err)
	}
	m.sweepSignals(ctx)

	id, err := testAdvance.Send(ctx, m, version, &command{})
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if delivered := within(t, got, 10*time.Second, "the accepted signal"); delivered != id {
		t.Fatalf("expected %s, got %s", id, delivered)
	}
	waitRun(t, m, version)
}

// TestSignalRefusedOnceFinished verifies a send that found the run live but records after it
// finished is refused, and leaves no pending signal behind.
func TestSignalRefusedOnceFinished(t *testing.T) { runBackends(t, testSignalRefusedOnceFinished) }

func testSignalRefusedOnceFinished(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	start := blockingFSM(t, m, "signal-finished", entered, release, WithSignals[orderReq, orderResp](testAdvance))
	version := startOrder(t, start, "signal-8")
	within(t, entered, 10*time.Second, "the transition")

	// The run as a send reads it before recording, then the run finishes first.
	run, err := m.store.liveRun(ctx, version)
	if err != nil {
		t.Fatalf("live run: %v", err)
	}
	close(release)
	waitRun(t, m, version)

	late := &fsmv1.Signal{Id: ulid.Make().String(), Name: "advance", Payload: []byte("{}")}
	if err := m.store.recordSignal(ctx, run, late); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected ErrFsmNotFound for a run that finished, got %v", err)
	}
	ids, err := m.store.pendingSignalIDs(ctx, version)
	if err != nil {
		t.Fatalf("pending signals: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected no pending signal left behind, got %v", ids)
	}
}

// TestSignalReadByPredicate verifies a RepeatWhile predicate's signals belong to its attempt: one
// it receives before RepeatAgain is not offered again to the iteration's body, and one it
// receives before RepeatDone is consumed with the transition, not offered to the next one.
func TestSignalReadByPredicate(t *testing.T) { runBackends(t, testSignalReadByPredicate) }

func testSignalReadByPredicate(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var (
		mu    sync.Mutex
		notes []string
	)
	note := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		notes = append(notes, s)
	}

	predicate := func(ctx context.Context, req *Request[orderReq, orderResp]) (Repeat, error) {
		switch req.Run().Iteration {
		case 0:
			return RepeatAgain(), nil
		case 1:
			select {
			case <-testAdvance.Receive(req):
				note("predicate:advance")
				return RepeatAgain(), nil
			case <-ctx.Done():
				return Repeat{}, ctx.Err()
			}
		}
		select {
		case <-testPause.Receive(req):
			note("predicate:pause")
			return RepeatDone(), nil
		case <-ctx.Done():
			return Repeat{}, ctx.Err()
		}
	}
	start, _, err := m.Register[orderReq, orderResp]("signal-predicate").
		Start("first", okTransition).
		To("stage", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			if req.Run().Iteration == 0 {
				entered <- struct{}{}
				<-release
				return nil, nil
			}
			if !noSignal(testAdvance.Receive(req)) {
				note("body:advance")
			}
			return nil, nil
		}, RepeatWhile(predicate)).
		To("after", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			if !noSignal(testPause.Receive(req)) {
				note("after:pause")
			}
			return nil, nil
		}).
		End("done", acceptTestSignals()).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version := startOrder(t, start, "signal-9")
	within(t, entered, 10*time.Second, "the first iteration")
	// Both wait while the first iteration's body holds; only the predicate reads them.
	if _, err := testAdvance.Send(ctx, m, version, &command{}); err != nil {
		t.Fatalf("send advance: %v", err)
	}
	if _, err := testPause.Send(ctx, m, version, &command{}); err != nil {
		t.Fatalf("send pause: %v", err)
	}
	close(release)
	waitRun(t, m, version)

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"predicate:advance", "predicate:pause"}; !slices.Equal(notes, want) {
		t.Fatalf("expected %v, got %v", want, notes)
	}
}

// TestSignalBeforeFirstExecution verifies a signal sent to a run that is pending, not yet
// executing anywhere, reaches it once it starts.
func TestSignalBeforeFirstExecution(t *testing.T) { runBackends(t, testSignalBeforeFirstExecution) }

func testSignalBeforeFirstExecution(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	got := make(chan ulid.ULID, 1)
	start, _, err := m.Register[orderReq, orderResp]("signal-early").
		Start("wait", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			select {
			case adv := <-testAdvance.Receive(req):
				got <- adv.ID
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).
		End("done", acceptTestSignals()).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	version, err := start(ctx, "signal-10", NewRequest(&orderReq{}, &orderResp{}), WithDelayedStart(time.Now().Add(300*time.Millisecond)))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	id, err := testAdvance.Send(ctx, m, version, &command{})
	if err != nil {
		t.Fatalf("send to the pending run failed: %v", err)
	}
	if delivered := within(t, got, 10*time.Second, "the signal once the run starts"); delivered != id {
		t.Fatalf("expected %s, got %s", id, delivered)
	}
	waitRun(t, m, version)
}

// recordingSender is a SignalSender that records what a typed Send hands it.
type recordingSender struct {
	name    string
	payload []byte
}

func (s *recordingSender) SendSignal(_ context.Context, _ ulid.ULID, name string, payload []byte) (ulid.ULID, error) {
	s.name, s.payload = name, payload
	return ulid.Make(), nil
}

// TestSignalSendThroughSender verifies a typed Send encodes the message with the signal's codec
// and hands the name and bytes to any SignalSender, so a caller can put the Manager behind an
// interface.
func TestSignalSendThroughSender(t *testing.T) {
	var sender recordingSender
	if _, err := testAdvance.Send(context.Background(), &sender, ulid.Make(), &command{Stage: 4, Note: "x"}); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if sender.name != "advance" {
		t.Fatalf("expected the signal's name, got %q", sender.name)
	}
	if err := testAdvance.check(sender.payload); err != nil {
		t.Fatalf("expected a payload the declared type decodes: %v", err)
	}
}

// TestMockSignals verifies a body under test receives signals through MockSignals as it would in a
// run: typed, per name, and counted received only once read.
func TestMockSignals(t *testing.T) {
	req := MockRequest(NewRequest(&orderReq{}, &orderResp{}), slog.Default(), Run{})
	sigs := MockSignals(req, testPause, testAdvance)
	defer sigs.Close()

	id := sigs.Deliver(testAdvance, &command{Stage: 3})
	sigs.Deliver(testPause, &command{})

	body := func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
		select {
		case adv := <-testAdvance.Receive(req):
			return NewResponse(&orderResp{Status: adv.Msg.Note}), checkStage(adv.Msg.Stage)
		case <-time.After(time.Second):
			return nil, errors.New("no advance")
		}
	}
	if _, err := body(context.Background(), req); err != nil {
		t.Fatalf("body failed: %v", err)
	}
	if got := sigs.Received(); !slices.Equal(got, []ulid.ULID{id}) {
		t.Fatalf("expected only the advance received, got %v", got)
	}
}

// TestSignalDiscardedAtFinish verifies a run's FINISH, as History returns it, lists the signals it
// accepted and never read, and not those it read.
func TestSignalDiscardedAtFinish(t *testing.T) { runBackends(t, testSignalDiscardedAtFinish) }

func testSignalDiscardedAtFinish(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	entered := make(chan struct{}, 1)
	start, _, err := m.Register[orderReq, orderResp]("signal-discarded").
		Start("wait", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			entered <- struct{}{}
			select {
			case <-testAdvance.Receive(req):
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).
		End("done", acceptTestSignals()).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	version := startOrder(t, start, "signal-11")
	within(t, entered, 10*time.Second, "the transition")

	pause, err := testPause.Send(ctx, m, version, &command{})
	if err != nil {
		t.Fatalf("send pause: %v", err)
	}
	if _, err := testAdvance.Send(ctx, m, version, &command{}); err != nil {
		t.Fatalf("send advance: %v", err)
	}
	waitRun(t, m, version)

	he, err := m.History(ctx, version)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if got := he.GetLastEvent().GetDiscardedSignals(); !slices.Equal(got, []string{pause.String()}) {
		t.Fatalf("expected only the unread pause listed discarded, got %v", got)
	}
}

func checkStage(stage int) error {
	if stage != 3 {
		return errors.New("unexpected stage")
	}
	return nil
}
