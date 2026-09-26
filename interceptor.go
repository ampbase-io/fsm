package fsm

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/cenkalti/backoff/v4"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

func (m *Manager) finisher[R, W any](finalizers []FinalizerFunc) func(context.Context, *Request[R, W]) (*Response[W], error) {
	return func(ctx context.Context, req *Request[R, W]) (*Response[W], error) {
		logger := req.Log()
		run := req.Run()

		for idx, f := range finalizers {
			logger.DebugContext(ctx, "calling finalizer", "finalizer", idx)
			f(ctx, req, run.fsmErr)
		}

		// Carry the run's final response — the last transition's, marshaled once by the canceller —
		// into the terminal record so History and the RPC Wait reply return the W result without a
		// codec here or a re-read of transition events. Only a successful run has a meaningful
		// result; a halted run's is its error. Finalizers run for their side effects and do not
		// reshape the result.
		event := finishEvent(run, run.CurrentState)
		if run.fsmErr.Err == nil {
			event.Response = req.response
		}

		if _, err := m.store.Append(ctx, run, event); err != nil {
			logger.ErrorContext(ctx, "failed to append complete event", "error", err)
			return nil, err
		}
		return nil, nil
	}
}

// finishEvent builds a run's terminal FINISH event, recorded in the given state. It is the one
// shape completion is driven through: the run loop's finisher for a normal end, and a
// pre-execution cancel (cancelOwnedRun) for a run terminated before it ran.
//
// A halted run carries its error onto the FINISH event so the terminal record preserves it: once
// the archive loop reaps the manifest, the history record is the only outcome source, and
// historyOutcome reads the error off this event (the manifest's error is gone). Without it, a
// failed run would report success after archival.
func finishEvent(run Run, state string) *fsmv1.StateEvent {
	event := &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_FINISH,
		Id:           run.ID,
		ResourceType: run.TypeName,
		Action:       run.Action,
		State:        state,
	}
	run.fsmErr.stamp(event)
	return event
}

// appender records a run's transition events — everything after START — and is all a transition
// interceptor needs of a backend. START goes through Store.Start, which carries the start record.
type appender interface {
	Append(ctx context.Context, run Run, event *fsmv1.StateEvent) (ulid.ULID, error)
}

func canceller(store appender, codec Codec) TransitionInterceptorFunc {
	return TransitionInterceptorFunc(func(next TransitionFunc) TransitionFunc {
		return TransitionFunc(func(ctx context.Context, req AnyRequest) (AnyResponse, error) {
			var (
				logger = req.Log()
				run    = req.Run()
				event  = &fsmv1.StateEvent{
					Type:         fsmv1.EventType_EVENT_TYPE_COMPLETE,
					Id:           run.ID,
					ResourceType: run.TypeName,
					Action:       run.Action,
					State:        run.CurrentState,
				}
			)

			resp, err := next(ctx, req)
			switch haltErr, halted := errors.AsType[*haltError](err); {
			case halted:
				logger.InfoContext(ctx, "transition returned cancelable error, completing run", "error", haltErr.err)
				event.Type = fsmv1.EventType_EVENT_TYPE_CANCEL
				RunErr{Err: haltErr, State: run.CurrentState}.stamp(event)
			case errors.Is(err, errRepeatDone):
				// The predicate ended the repetition. The COMPLETE, with no iterations completed,
				// records the transition finished, so resume and ListActive move past it.
			case err != nil:
				return resp, err
			default:
				logger.DebugContext(ctx, "transition completed successfully")
				event.IterationsCompleted = run.iterationsCompleted()
				if resp != nil && resp.Any() != nil {
					b, err := codec.Marshal(resp.Any())
					if err != nil {
						logger.ErrorContext(ctx, "failed to marshal response", "error", err)
						return nil, err
					}
					event.Response = b
					// Stash the marshaled response so the finisher can record the run's final
					// result in the terminal record without holding a codec of its own.
					req.setResponse(b)
				}
			}

			switch _, appendErr := store.Append(ctx, run, event); {
			case errors.Is(appendErr, ErrLeaseLost):
				// A fenced append means the run must halt here even though the transition
				// itself succeeded; swallowing it would keep executing without a durable record.
				logger.WarnContext(ctx, "append fenced, halting run", "error", appendErr)
				return resp, appendErr
			case appendErr != nil:
				logger.ErrorContext(ctx, "failed to append complete event", "error", appendErr)
			}

			return resp, err
		})
	})
}

func retry(tracer trace.Tracer, instruments *instruments, store appender) TransitionInterceptorFunc {
	return TransitionInterceptorFunc(func(next TransitionFunc) TransitionFunc {
		return TransitionFunc(func(ctx context.Context, req AnyRequest) (AnyResponse, error) {
			logger := req.Log()
			run := req.Run()

			// The transition's duration counts from its first attempt across every retry.
			transitionStart := time.Now()
			observe := func(status string) {
				instruments.observeTransition(ctx, run, status, transitionStart)
			}

			boff := backoff.WithContext(&backoff.ExponentialBackOff{
				InitialInterval:     100 * time.Millisecond,
				RandomizationFactor: backoff.DefaultRandomizationFactor,
				Multiplier:          backoff.DefaultMultiplier,
				MaxInterval:         5 * time.Second,
				MaxElapsedTime:      0,
				Clock:               backoff.SystemClock,
			}, ctx)
			boff.Reset()

			transitionCtx, transitionSpan := newTransitionSpan(ctx, tracer, run)

			var (
				retryCount = RetryFromContext(ctx)
				lastErr    = errors.New("initial error")
				resp       AnyResponse
			)
			err := backoff.RetryNotify(
				func() (err error) {
					defer func() {
						if r := recover(); r != nil {
							observe("panic")
							transitionSpan.SetAttributes(semconv.ExceptionStacktrace(string(debug.Stack())))
							err = fmt.Errorf("FSM %s.%s transition %s panic", run.ResourceName, run.Action, run.CurrentState)
							logger.ErrorContext(transitionCtx, "recovered", "error", err, "stack", string(debug.Stack()))
						}
					}()
					resp, err = next(withRetry(transitionCtx, retryCount), req)
					if err == nil {
						observe("ok")
						return nil
					}
					if errors.Is(err, errRepeatDone) {
						return backoff.Permanent(err)
					}

					switch kind := outcomeKind(err); {
					case haltsRun(kind):
						transitionSpan.SetAttributes(outcomeAttrs(kind)...)
						status, _ := runStatus(kind)
						observe(status)
						logger.ErrorContext(transitionCtx, "transition halted the run", "error", err, "kind", kind)
						return backoff.Permanent(halt(err))
					case errors.Is(err, ErrLeaseLost):
						// Retrying a fenced write can never succeed; the run halts and the new
						// owner drives it to completion.
						observe("lease_lost")
						logger.WarnContext(transitionCtx, "run lease lost, halting", "error", err)
						return backoff.Permanent(err)
					case ctx.Err() != nil:
						// Classified below, once, together with a cancel that lands in the sleep
						// between attempts.
						return backoff.Permanent(err)
					default:
						observe("error")
						logger.WarnContext(transitionCtx, "transition failed, retrying", "error", err)
						return err
					}
				},
				boff,
				func(err error, _ time.Duration) {
					switch {
					case lastErr.Error() != err.Error(), retryCount%10 == 0:
						logger.DebugContext(transitionCtx, "recording transition error")
						if lastErr.Error() != err.Error() {
							store.Append(ctx,
								run,
								&fsmv1.StateEvent{
									Type:         fsmv1.EventType_EVENT_TYPE_ERROR,
									Id:           run.ID,
									ResourceType: run.TypeName,
									Action:       run.Action,
									State:        run.CurrentState,
									Error:        err.Error(),
									RetryCount:   retryCount,
								},
							)
						}

						transitionSpan.SetAttributes(attribute.Int("fsm.retry_count", int(retryCount)))
						transitionSpan.SetStatus(codes.Error, err.Error())
						transitionSpan.End()

						lastErr = err

						transitionCtx, transitionSpan = newTransitionSpan(ctx, tracer, run)
					default:
						logger.DebugContext(transitionCtx, "retrying without recording error")
					}
					retryCount++
					logger = req.Log().With("retry_count", retryCount)
				},
			)

			if endedByContext(ctx, err) {
				observe("canceled")
				logger.InfoContext(transitionCtx, "transition canceled", "error", context.Cause(ctx))
				err = haltOnCancel(ctx, err)
			}

			transitionSpan.SetAttributes(attribute.Int("fsm.retry_count", int(retryCount)))
			transitionSpan.End()

			return resp, err
		})
	})
}

// endedByContext reports whether the transition's context, not an attempt, ended the retries:
// err is then a bare error no attempt classified — RetryNotify's own ctx.Err() from its sleep,
// or the last attempt's — rather than a halt an attempt recorded or a lost lease, which passes
// through unrecorded. Such an end is the transition's outcome when an operator canceled it.
func endedByContext(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	return !isHalt(err) && !errors.Is(err, ErrLeaseLost)
}

// haltOnCancel turns err into a halt carrying the operator's reason when ctx was ended by
// Manager.Cancel, so the run records it and finishes. A shutdown or a lost lease passes through:
// the run stops without a record and its next owner resumes it.
func haltOnCancel(ctx context.Context, err error) error {
	cerr, ok := errors.AsType[*CancelError](context.Cause(ctx))
	if !ok {
		return err
	}
	return halt(cerr)
}

func newTransitionSpan(ctx context.Context, tracer trace.Tracer, run Run) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{
		attribute.String("fsm.action", run.Action),
		attribute.String("fsm.state", run.CurrentState),
		attribute.String("fsm.type", run.ResourceName),
		attribute.String(fmt.Sprintf("%s.id", run.ResourceName), run.ID),
		attribute.String(fmt.Sprintf("%s.version", run.ResourceName), run.StartVersion.String()),
	}
	if run.iterated {
		attrs = append(attrs, attribute.Int("fsm.iteration", run.Iteration))
	}
	return tracer.Start(ctx, fmt.Sprintf("%s.%s", run.ResourceName, run.CurrentState), trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
}
