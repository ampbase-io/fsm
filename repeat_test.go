package fsm

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

// iterations records what a repeated transition saw, from the handler and interceptor sides,
// and how often the transition after it ran.
type iterations struct {
	mu       sync.Mutex
	body     []int
	seen     []int
	versions []ulid.ULID

	last atomic.Int32
}

func (it *iterations) ran(run Run) {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.body = append(it.body, run.Iteration)
	it.versions = append(it.versions, run.TransitionVersion)
}

// intercept records every iteration that reaches the transition's own interceptors.
func (it *iterations) intercept(next TransitionFunc) TransitionFunc {
	return func(ctx context.Context, req AnyRequest) (AnyResponse, error) {
		it.mu.Lock()
		it.seen = append(it.seen, req.Run().Iteration)
		it.mu.Unlock()
		return next(ctx, req)
	}
}

func (it *iterations) snapshot() (body, seen []int, versions []ulid.ULID) {
	it.mu.Lock()
	defer it.mu.Unlock()
	return slices.Clone(it.body), slices.Clone(it.seen), slices.Clone(it.versions)
}

// fewerThan repeats a transition while its index is below n.
func fewerThan(n int) func(context.Context, *Request[orderReq, orderResp]) (Repeat, error) {
	return func(_ context.Context, req *Request[orderReq, orderResp]) (Repeat, error) {
		if req.Run().Iteration < n {
			return RepeatAgain(), nil
		}
		return RepeatDone(), nil
	}
}

// repeatingFSM registers first → stage (repeated under predicate) → last → done, recording into
// it.
func repeatingFSM(t *testing.T, m *Manager, action string, it *iterations, predicate func(context.Context, *Request[orderReq, orderResp]) (Repeat, error)) Start[orderReq, orderResp] {
	t.Helper()

	start, _, err := m.Register[orderReq, orderResp](action).
		Start("first", okTransition).
		To("stage", func(_ context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			it.ran(req.Run())
			return nil, nil
		}, RepeatWhile(predicate), WithInterceptors[orderReq, orderResp](it.intercept)).
		To("last", func(context.Context, *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			it.last.Add(1)
			return nil, nil
		}).
		End("done").
		Build(context.Background())
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	return start
}

func startAndWait(t *testing.T, m *Manager, start Start[orderReq, orderResp], id string) (ulid.ULID, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	version, err := start(ctx, id, NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	return version, m.Wait(ctx, version)
}

// TestRepeatWhileRunsEachIteration verifies a repeated transition runs once per index the
// predicate allows, each iteration through the transition's interceptors under a fresh
// transition version, and that the repeat does not carry over to the next transition.
func TestRepeatWhileRunsEachIteration(t *testing.T) { runBackends(t, testRepeatWhileRunsEachIteration) }

func testRepeatWhileRunsEachIteration(t *testing.T, b *backend) {
	m, _ := b.newManager(nil)
	var it iterations
	start := repeatingFSM(t, m, "repeat", &it, fewerThan(3))

	if _, err := startAndWait(t, m, start, "repeat-1"); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	body, seen, versions := it.snapshot()
	if want := []int{0, 1, 2}; !slices.Equal(body, want) || !slices.Equal(seen, want) {
		t.Fatalf("expected iterations %v in the body and the interceptor, got %v and %v", want, body, seen)
	}
	slices.SortFunc(versions, ulid.ULID.Compare)
	if len(slices.Compact(versions)) != 3 {
		t.Fatalf("expected a fresh transition version per iteration, got %v", versions)
	}
	if n := it.last.Load(); n != 1 {
		t.Fatalf("expected the next transition to run once, ran %d times", n)
	}
}

// TestRepeatWhileNoIterations verifies a predicate that answers RepeatDone at index zero runs
// neither the body nor the transition's interceptors, and the run moves on.
func TestRepeatWhileNoIterations(t *testing.T) { runBackends(t, testRepeatWhileNoIterations) }

func testRepeatWhileNoIterations(t *testing.T, b *backend) {
	m, _ := b.newManager(nil)
	var it iterations
	start := repeatingFSM(t, m, "repeat-none", &it, fewerThan(0))

	if _, err := startAndWait(t, m, start, "repeat-none-1"); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	if body, seen, _ := it.snapshot(); len(body) != 0 || len(seen) != 0 {
		t.Fatalf("expected no iteration, got body %v and interceptor %v", body, seen)
	}
	if n := it.last.Load(); n != 1 {
		t.Fatalf("expected the next transition to run once, ran %d times", n)
	}
}

// TestRepeatWhilePredicateErrorRetries verifies a plain predicate error is retried as a body's
// would be, and the predicate is asked again.
func TestRepeatWhilePredicateErrorRetries(t *testing.T) {
	runBackends(t, testRepeatWhilePredicateErrorRetries)
}

func testRepeatWhilePredicateErrorRetries(t *testing.T, b *backend) {
	m, _ := b.newManager(nil)
	var (
		it     iterations
		failed atomic.Bool
	)
	flaky := func(ctx context.Context, req *Request[orderReq, orderResp]) (Repeat, error) {
		if failed.CompareAndSwap(false, true) {
			return Repeat{}, errors.New("flaky")
		}
		return fewerThan(1)(ctx, req)
	}
	start := repeatingFSM(t, m, "repeat-flaky", &it, flaky)

	if _, err := startAndWait(t, m, start, "repeat-flaky-1"); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if body, _, _ := it.snapshot(); !slices.Equal(body, []int{0}) {
		t.Fatalf("expected one iteration after the retried predicate, got %v", body)
	}
}

// TestRepeatWhilePredicateHalts verifies a predicate halts the run as a body would, recorded in
// the repeated transition, and that a zero Repeat is an unrecoverable system error rather than
// a quiet RepeatDone.
func TestRepeatWhilePredicateHalts(t *testing.T) { runBackends(t, testRepeatWhilePredicateHalts) }

func testRepeatWhilePredicateHalts(t *testing.T, b *backend) {
	cases := []struct {
		name      string
		predicate func(context.Context, *Request[orderReq, orderResp]) (Repeat, error)
		check     func(error) bool
	}{
		{
			name: "abort",
			predicate: func(context.Context, *Request[orderReq, orderResp]) (Repeat, error) {
				return Repeat{}, Abort(errors.New("no stages"))
			},
			check: isA[*AbortError],
		},
		{
			name: "zero",
			predicate: func(context.Context, *Request[orderReq, orderResp]) (Repeat, error) {
				return Repeat{}, nil
			},
			check: func(err error) bool {
				ue, ok := errors.AsType[*UnrecoverableError](err)
				return ok && ue.Kind == ErrorKindSystem
			},
		},
	}

	m, _ := b.newManager(nil)
	for _, tc := range cases {
		var it iterations
		start := repeatingFSM(t, m, "repeat-halt-"+tc.name, &it, tc.predicate)

		version, err := startAndWait(t, m, start, "repeat-halt-"+tc.name)
		if !tc.check(err) {
			t.Fatalf("%s: unexpected outcome %v", tc.name, err)
		}
		if body, seen, _ := it.snapshot(); len(body) != 0 || len(seen) != 0 {
			t.Fatalf("%s: expected no iteration, got body %v and interceptor %v", tc.name, body, seen)
		}
		if n := it.last.Load(); n != 0 {
			t.Fatalf("%s: expected the run halted before the next transition, it ran %d times", tc.name, n)
		}
		he, err := m.History(context.Background(), version)
		if err != nil {
			t.Fatalf("%s: history: %v", tc.name, err)
		}
		if state := he.GetLastEvent().GetErrorState(); state != "stage" {
			t.Fatalf("%s: expected the halt recorded in stage, got %q", tc.name, state)
		}
	}
}

// TestRepeatWhileResumes verifies a run stopped mid-iteration resumes at that iteration, not
// from zero, and that ListActive reports the repeated transition while it iterates.
func TestRepeatWhileResumes(t *testing.T) { runBackends(t, testRepeatWhileResumes) }

func testRepeatWhileResumes(t *testing.T, b *backend) {
	ctx := context.Background()
	var (
		it    iterations
		block atomic.Bool
	)
	block.Store(true)
	entered := make(chan int, 4)
	done := make(chan struct{}, 1)

	register := func(m *Manager) (Start[orderReq, orderResp], Resume) {
		start, resume, err := m.Register[orderReq, orderResp]("repeat-resume").
			Start("first", okTransition).
			To("stage", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				it.ran(req.Run())
				entered <- req.Run().Iteration
				if req.Run().Iteration == 1 && block.Load() {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, nil
			}, RepeatWhile(fewerThan(3))).
			End("done", WithFinalizers(func(context.Context, *Request[orderReq, orderResp], RunErr) {
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
	version, err := start(ctx, "repeat-resume-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	for want := range 2 {
		if got := within(t, entered, 10*time.Second, "an iteration"); got != want {
			t.Fatalf("expected iteration %d, got %d", want, got)
		}
	}

	states, err := m1.store.ListActive(ctx)
	if err != nil {
		t.Fatalf("failed to list active runs: %v", err)
	}
	i := slices.IndexFunc(states, func(rs runSnapshot) bool { return rs.StartVersion == version })
	if i < 0 || states[i].CurrentState != "stage" {
		t.Fatalf("expected the run listed in stage while it iterates, got %+v", states)
	}

	stop1()
	block.Store(false)

	m2, _ := b.newManager(nil)
	_, resume := register(m2)
	if err := resume(ctx); err != nil {
		t.Fatalf("failed to resume: %v", err)
	}
	within(t, done, 10*time.Second, "the resumed run to finish")

	if body, _, _ := it.snapshot(); !slices.Equal(body, []int{0, 1, 1, 2}) {
		t.Fatalf("expected the resume to rerun the stopped iteration and go on, ran %v", body)
	}
}

// TestRepeatWhileFinishedMovesOn verifies a repeated transition's RepeatDone is recorded: while
// the next transition runs, ListActive reports that one, and a resume moves past the finished
// transition even on a definition where it no longer repeats, rather than running it again.
func TestRepeatWhileFinishedMovesOn(t *testing.T) { runBackends(t, testRepeatWhileFinishedMovesOn) }

func testRepeatWhileFinishedMovesOn(t *testing.T, b *backend) {
	ctx := context.Background()
	var block atomic.Bool
	block.Store(true)
	entered := make(chan struct{}, 2)
	done := make(chan struct{}, 1)

	register := func(m *Manager, it *iterations, opts ...Option[orderReq, orderResp]) (Start[orderReq, orderResp], Resume) {
		start, resume, err := m.Register[orderReq, orderResp]("repeat-finished").
			Start("first", okTransition).
			To("stage", func(_ context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				it.ran(req.Run())
				return nil, nil
			}, opts...).
			To("next", func(ctx context.Context, _ *Request[orderReq, orderResp]) (*Response[orderResp], error) {
				entered <- struct{}{}
				if block.Load() {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, nil
			}).
			End("done", WithFinalizers(func(context.Context, *Request[orderReq, orderResp], RunErr) {
				done <- struct{}{}
			})).
			Build(ctx)
		if err != nil {
			t.Fatalf("failed to build FSM: %v", err)
		}
		return start, resume
	}

	var repeated iterations
	m1, stop1 := b.newManager(nil)
	start, _ := register(m1, &repeated, RepeatWhile(fewerThan(2)))
	version, err := start(ctx, "repeat-finished-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	within(t, entered, 10*time.Second, "the transition after the repeat")

	states, err := m1.store.ListActive(ctx)
	if err != nil {
		t.Fatalf("failed to list active runs: %v", err)
	}
	i := slices.IndexFunc(states, func(rs runSnapshot) bool { return rs.StartVersion == version })
	if i < 0 || states[i].CurrentState != "next" {
		t.Fatalf("expected the run listed in next once stage finished, got %+v", states)
	}

	stop1()
	block.Store(false)

	var plain iterations
	m2, _ := b.newManager(nil)
	_, resume := register(m2, &plain)
	if err := resume(ctx); err != nil {
		t.Fatalf("failed to resume: %v", err)
	}
	within(t, done, 10*time.Second, "the resumed run to finish")

	if body, _, _ := plain.snapshot(); len(body) != 0 {
		t.Fatalf("expected the finished stage not to run again on resume, it ran %d times", len(body))
	}
}
