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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	transitionCounterVec = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fsm_transition_count",
			Help: "A count of transition completions.",
		},
		[]string{"action", "state", "resource", "status"},
	)

	transitionDurationVec = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "fsm_transition_duration_seconds",
			Help:    "Time spent performing a transition.",
			Buckets: []float64{.5, 1, 2.5, 5, 10, 30, 60, 150, 300, 600, 1200},
		},
		[]string{"action", "state", "resource", "status"},
	)
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
	if run.fsmErr.Err != nil {
		event.Error = run.fsmErr.Err.Error()
	}
	return event
}

// skipper will skip executing the next transition if the FSM has already errored.
func skipper() TransitionInterceptorFunc {
	return TransitionInterceptorFunc(func(next TransitionFunc) TransitionFunc {
		return TransitionFunc(func(ctx context.Context, req AnyRequest) (AnyResponse, error) {
			if fsmErr := req.Run().fsmErr; fsmErr.Err != nil {
				req.Log().DebugContext(ctx, "skipping transition due to previous error", "error", fsmErr.Err)
				return nil, nil
			}
			return next(ctx, req)
		})
	})
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
			switch haltErr, isHalt := errors.AsType[*haltError](err); {
			case isHalt:
				logger.InfoContext(ctx, "transition returned cancelable error, completing run", "error", haltErr.err)
				event.Type = fsmv1.EventType_EVENT_TYPE_CANCEL
				event.Error = haltErr.Error()
			case err != nil:
				return resp, err
			default:
				logger.DebugContext(ctx, "transition completed successfully")
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

func retry(tracer trace.Tracer, store appender) TransitionInterceptorFunc {
	return TransitionInterceptorFunc(func(next TransitionFunc) TransitionFunc {
		return TransitionFunc(func(ctx context.Context, req AnyRequest) (AnyResponse, error) {
			logger := req.Log()
			run := req.Run()

			localTransitionCounterVec := transitionCounterVec.MustCurryWith(prometheus.Labels{
				"action":   run.Action,
				"state":    run.CurrentState,
				"resource": run.ResourceName,
			})

			transitionStartTime := time.Now()
			localTransitionDurationVec := transitionDurationVec.MustCurryWith(prometheus.Labels{
				"action":   run.Action,
				"state":    run.CurrentState,
				"resource": run.ResourceName,
			})

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
							localTransitionCounterVec.WithLabelValues("panic").Inc()
							localTransitionDurationVec.WithLabelValues("panic").Observe(time.Since(transitionStartTime).Seconds())
							transitionSpan.SetAttributes(semconv.ExceptionStacktrace(string(debug.Stack())))
							err = fmt.Errorf("FSM %s.%s transition %s panic", run.ResourceName, run.Action, run.CurrentState)
							logger.ErrorContext(transitionCtx, "recovered", "error", err, "stack", string(debug.Stack()))
						}
					}()
					resp, err = next(withRetry(transitionCtx, retryCount), req)
					if err == nil {
						localTransitionCounterVec.WithLabelValues("ok").Inc()
						localTransitionDurationVec.WithLabelValues("ok").Observe(time.Since(transitionStartTime).Seconds())
						return nil
					}

					var (
						_, isAbort          = errors.AsType[*AbortError](err)
						ue, isUnrecoverable = errors.AsType[*UnrecoverableError](err)
						_, isHandoff        = errors.AsType[*HandoffError](err)
					)
					switch {
					case isAbort:
						localTransitionCounterVec.WithLabelValues("abort").Inc()
						localTransitionDurationVec.WithLabelValues("abort").Observe(time.Since(transitionStartTime).Seconds())
						logger.ErrorContext(transitionCtx, "transition aborted", "error", err)
						return backoff.Permanent(halt(err))
					case isUnrecoverable:
						transitionSpan.SetAttributes(attribute.String("fsm.error_kind", ue.Kind.String()))
						localTransitionCounterVec.WithLabelValues("unrecoverable").Inc()
						localTransitionDurationVec.WithLabelValues("unrecoverable").Observe(time.Since(transitionStartTime).Seconds())
						logger.ErrorContext(transitionCtx, "reached unrecoverable error, canceling FSM", "error", err)
						return backoff.Permanent(halt(err))
					case isHandoff:
						transitionSpan.SetAttributes(attribute.String("fsm.error_kind", "fsmHandoffError"))
						localTransitionCounterVec.WithLabelValues("fsm_handoff_error").Inc()
						localTransitionDurationVec.WithLabelValues("fsm_handoff_error").Observe(time.Since(transitionStartTime).Seconds())
						logger.ErrorContext(transitionCtx, "reached fsm handoff error, canceling FSM", "error", err)
						return backoff.Permanent(halt(err))
					case errors.Is(err, ErrLeaseLost):
						// Retrying a fenced write can never succeed; the run halts and the new
						// owner drives it to completion.
						localTransitionCounterVec.WithLabelValues("lease_lost").Inc()
						localTransitionDurationVec.WithLabelValues("lease_lost").Observe(time.Since(transitionStartTime).Seconds())
						logger.WarnContext(transitionCtx, "run lease lost, halting", "error", err)
						return backoff.Permanent(err)
					case ctx.Err() != nil:
						localTransitionCounterVec.WithLabelValues("canceled").Inc()
						localTransitionDurationVec.WithLabelValues("canceled").Observe(time.Since(transitionStartTime).Seconds())
						logger.InfoContext(transitionCtx, "transition canceled", "error", context.Cause(ctx))
						return backoff.Permanent(haltOnCancel(ctx, err))
					default:
						localTransitionCounterVec.WithLabelValues("error").Inc()
						localTransitionDurationVec.WithLabelValues("error").Observe(time.Since(transitionStartTime).Seconds())
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

			transitionSpan.SetAttributes(attribute.Int("fsm.retry_count", int(retryCount)))
			transitionSpan.End()

			return resp, err
		})
	})
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
	return tracer.Start(ctx, fmt.Sprintf("%s.%s", run.ResourceName, run.CurrentState), trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("fsm.action", run.Action),
			attribute.String("fsm.state", run.CurrentState),
			attribute.String("fsm.type", run.ResourceName),
			attribute.String(fmt.Sprintf("%s.id", run.ResourceName), run.ID),
			attribute.String(fmt.Sprintf("%s.version", run.ResourceName), run.StartVersion.String()),
		),
	)
}
