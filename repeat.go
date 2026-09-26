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
// none of those interceptors and records no event.
func RepeatWhile[R, W any](predicate func(context.Context, *Request[R, W]) (Repeat, error)) Option[R, W] {
	return repeatOption[R, W](predicate)
}

type repeatOption[R, W any] func(context.Context, *Request[R, W]) (Repeat, error)

func (o repeatOption[R, W]) apply(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	cfg.repeat = o
	return cfg
}

// errRepeatDone is how a repeated transition's gate tells the run loop the predicate answered
// RepeatDone. It passes through the canceller and retry unrecorded.
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

// recordIteration counts a completed iteration of a repeated transition into counts, so its next
// iteration runs at the index after the event's. An event with no iteration is not one.
func recordIteration(counts map[string]uint32, event *fsmv1.StateEvent) map[string]uint32 {
	if event.Iteration == nil {
		return counts
	}
	if counts == nil {
		counts = map[string]uint32{}
	}
	counts[event.GetState()] = event.GetIteration() + 1
	return counts
}
