package fsm

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// drift is how a registration of the drift FSM differs from the created → shipped → done that
// started the run: strict about resuming, and missing the shipped transition.
type drift struct {
	strict, missingShipped bool
}

// driftFSM registers the drift FSM as d describes it. created blocks until its context ends
// unless allowComplete is set, reporting each entry.
func driftFSM(t *testing.T, m *Manager, d drift, entered chan<- struct{}, allowComplete *atomic.Bool) (Start[orderReq, orderResp], Resume) {
	t.Helper()

	created := func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
		entered <- struct{}{}
		if allowComplete.Load() {
			return NewResponse(&orderResp{Status: "created"}), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	shipped := func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
		return NewResponse(&orderResp{Status: "shipped"}), nil
	}

	reg := m.Register[orderReq, orderResp]("drift")
	if d.strict {
		reg = reg.StrictResume()
	}
	next := reg.Start("created", created)
	if !d.missingShipped {
		next = next.To("shipped", shipped)
	}
	start, resume, err := next.End("done").Build(context.Background())
	if err != nil {
		t.Fatalf("failed to build drift FSM: %v", err)
	}
	return start, resume
}

// fastClaims gives every node a short lease and claim cadence, so a claim decision lands within
// a few hundred milliseconds and "never claimed" can be observed across several passes.
func fastClaims(cfg *ObjectStorageConfig) {
	cfg.LeaseTimeout = 150 * time.Millisecond
	cfg.HeartbeatPeriod = 75 * time.Millisecond
	cfg.ClaimInterval = 75 * time.Millisecond
}

// interruptedDriftRun starts a created → shipped → done run and shuts its manager down while
// created is blocked, leaving the run recorded with shipped still to come. It returns the run's
// version.
func interruptedDriftRun(t *testing.T, b *backend, entered chan struct{}, allowComplete *atomic.Bool) ulid.ULID {
	t.Helper()
	ctx := context.Background()

	m1, stop1 := b.newManager(nil)
	start, _ := driftFSM(t, m1, drift{}, entered, allowComplete)
	version, err := start(ctx, "drift-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start run: %v", err)
	}
	<-entered
	stop1()
	return version
}

// TestStrictResumeRefusesUnknownTransition verifies StrictResume: a definition missing a
// transition the run still has to make refuses the run rather than skipping the step. On BoltDB
// Resume reports ErrUnknownTransition; on the object backend the run is refused at the claim
// filter and stays unowned — no lease taken, epoch unchanged — across several claim passes. In
// both, shipped is never recorded complete, the refusing definition never runs the handler, and
// a definition with the transition then resumes and finishes the run.
func TestStrictResumeRefusesUnknownTransition(t *testing.T) {
	t.Run("bolt", func(t *testing.T) { testStrictResumeRefusesUnknownTransition(t, newBoltBackend(t)) })
	t.Run("object", func(t *testing.T) {
		testStrictResumeRefusesUnknownTransition(t, newObjectBackendWith(t, fastClaims))
	})
}

func testStrictResumeRefusesUnknownTransition(t *testing.T, b *backend) {
	ctx := context.Background()
	capture := &logCapture{}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	b.configureManager = func(cfg *Config) {
		cfg.Logger = slog.New(slog.NewTextHandler(capture, nil))
		cfg.MeterProvider = provider
	}
	entered := make(chan struct{}, 3)
	var allowComplete atomic.Bool
	version := interruptedDriftRun(t, b, entered, &allowComplete)

	m2, stop2 := b.newManager(nil)
	_, resume := driftFSM(t, m2, drift{strict: true, missingShipped: true}, entered, &allowComplete)
	err := resume(ctx)
	// Long enough for the object backend's claim loop to make several passes.
	time.Sleep(500 * time.Millisecond)

	switch store := m2.store.(type) {
	case *objectStore:
		if err != nil {
			t.Fatalf("expected a refused claim to leave Resume clean, got %v", err)
		}
		manifest := mustManifest(t, store, version)
		if manifest.GetOwnerNode() != "" || manifest.GetLeaseEpoch() != 1 {
			t.Fatalf("expected the run left unowned at epoch 1, got owner %q epoch %d", manifest.GetOwnerNode(), manifest.GetLeaseEpoch())
		}
	default:
		if !errors.Is(err, ErrUnknownTransition) {
			t.Fatalf("expected Resume to report ErrUnknownTransition, got %v", err)
		}
	}

	select {
	case <-entered:
		t.Fatal("the strict definition ran the handler of a run it refused")
	default:
	}
	if !slices.ContainsFunc(capture.lines(), func(line string) bool {
		return strings.Contains(line, "transition not defined") && strings.Contains(line, "state=shipped")
	}) {
		t.Fatal("expected a warning naming the undefined transition")
	}
	if counterValue(t, collect(t, reader), "fsm.resume.refused", attrState, "shipped") == 0 {
		t.Fatal("expected the refusal counted under the undefined transition")
	}
	active, err := m2.store.Active(ctx, fsmKey{typeName: "orderReq", action: "drift"})
	if err != nil {
		t.Fatalf("failed to list active runs: %v", err)
	}
	i := slices.IndexFunc(active, func(r *activeResource) bool { return r.version == version })
	if i < 0 {
		t.Fatal("expected the refused run still active")
	}
	if slices.Contains(active[i].completedTransitions, "shipped") {
		t.Fatal("expected shipped never recorded complete by the refusing definition")
	}

	// A definition with the transition finishes the run. (BoltDB holds a file lock, so the
	// refusing manager must be gone before the next one opens the same store.)
	stop2()
	allowComplete.Store(true)
	m3, _ := b.newManager(nil)
	_, resume = driftFSM(t, m3, drift{strict: true}, entered, &allowComplete)
	if err := resume(ctx); err != nil {
		t.Fatalf("failed to resume with the full definition: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m3.Wait(waitCtx, version); err != nil {
		t.Fatalf("run completed with error: %v", err)
	}
}

// TestLenientResumeSkipsUnknownTransition pins the default: a definition missing a transition
// the run still has to make resumes it anyway, and the run finishes with that step skipped.
func TestLenientResumeSkipsUnknownTransition(t *testing.T) {
	runBackends(t, testLenientResumeSkipsUnknownTransition)
}

func testLenientResumeSkipsUnknownTransition(t *testing.T, b *backend) {
	ctx := context.Background()
	capture := &logCapture{}
	b.configureManager = func(cfg *Config) {
		cfg.Logger = slog.New(slog.NewTextHandler(capture, nil))
	}
	entered := make(chan struct{}, 2)
	var allowComplete atomic.Bool
	version := interruptedDriftRun(t, b, entered, &allowComplete)

	allowComplete.Store(true)
	m2, _ := b.newManager(nil)
	_, resume := driftFSM(t, m2, drift{missingShipped: true}, entered, &allowComplete)
	if err := resume(ctx); err != nil {
		t.Fatalf("failed to resume: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("run completed with error: %v", err)
	}
	if len(linesWithMessage(capture.lines(), "transition did not exist")) == 0 {
		t.Fatal("expected the skipped transition logged")
	}
}
