package fsm

import (
	"context"
	"errors"
	"fmt"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"
)

// Repeat is a RepeatWhile predicate's decision. Build it with RepeatAgain or RepeatDone; the
// zero value is no decision, and halts the run.
type Repeat struct {
	decision decision
}

type decision int

const (
	decisionAgain decision = iota + 1
	decisionDone
)

// RepeatAgain runs the transition's iteration at req.Run().Iteration.
func RepeatAgain() Repeat { return Repeat{decision: decisionAgain} }

// RepeatDone moves the run on to the next transition.
func RepeatDone() Repeat { return Repeat{decision: decisionDone} }

// RepeatWhile makes a transition repeatable. The predicate is asked before every iteration,
// including the first, with req.Run().Iteration set to the index about to run, and again on
// resume with the index after the last completed iteration: only that index is recorded, so the
// predicate's answer must follow from the request and the index alone. It is asked again before
// every retry of the transition, and its error is classified as the transition's own: Abort and
// the unrecoverable errors halt the run, anything else is retried.
//
// Each iteration is recorded as its own COMPLETE event carrying its index, runs with a fresh
// transition version, and passes through the transition's interceptors. A RepeatDone reaches
// none of those interceptors; it records the transition finished, as a COMPLETE with no index.
func RepeatWhile[R, W any](predicate func(context.Context, *Request[R, W]) (Repeat, error)) Option[R, W] {
	return repeatOption[R, W](predicate)
}

type repeatOption[R, W any] func(context.Context, *Request[R, W]) (Repeat, error)

func (o repeatOption[R, W]) apply(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	cfg.repeat = o
	return cfg
}

// errRepeatDone is how a repeated transition's gate tells the run loop the predicate answered
// RepeatDone. Retry passes it through, and the canceller records it as the transition's COMPLETE
// with no iteration.
var errRepeatDone = errors.New("fsm: repeat done")

// repeater asks the predicate before every attempt at an iteration. It sits inside retry, so a
// failing predicate is retried and classified as the transition is, and outside the caller's
// interceptors, so a RepeatDone never reaches them.
func repeater[R, W any](predicate func(context.Context, *Request[R, W]) (Repeat, error)) TransitionInterceptorFunc {
	return func(next TransitionFunc) TransitionFunc {
		return gate[R, W]{predicate: predicate, next: next}.run
	}
}

// gate is a repeated transition's predicate in front of the rest of its chain.
type gate[R, W any] struct {
	predicate func(context.Context, *Request[R, W]) (Repeat, error)
	next      TransitionFunc
}

func (g gate[R, W]) run(ctx context.Context, req AnyRequest) (AnyResponse, error) {
	typed, ok := req.(*Request[R, W])
	if !ok {
		return nil, fmt.Errorf("unexpected predicate request type %T", req)
	}
	r, err := g.predicate(ctx, typed)
	switch {
	case err != nil:
		return nil, err
	case r.decision == decisionAgain:
		return g.next(ctx, req)
	case r.decision == decisionDone:
		return nil, errRepeatDone
	}
	return nil, NewUnrecoverableSystemError(fmt.Errorf("transition %s: RepeatWhile predicate returned no decision", req.Run().CurrentState))
}

// recordIteration folds a COMPLETE into counts. One carrying an iteration counts it, so the next
// runs at the index after it; one without finishes the transition, repeated or not, and drops
// any count it had.
func recordIteration(counts map[string]uint32, event *fsmv1.StateEvent) map[string]uint32 {
	if event.Iteration == nil {
		delete(counts, event.GetState())
		return counts
	}
	if counts == nil {
		counts = map[string]uint32{}
	}
	counts[event.GetState()] = event.GetIteration() + 1
	return counts
}
