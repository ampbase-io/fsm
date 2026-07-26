package fsm

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/cenkalti/backoff/v4"
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
			logger.WithField("finalizer", idx).Info("calling finalizer")
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

		if _, err := m.store.Append(ctx, run, event, run.Queue); err != nil {
			logger.WithError(err).Error("failed to append complete event")
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
				req.Log().WithError(fsmErr.Err).Info("skipping transition due to previous error")
				return nil, nil
			}
			return next(ctx, req)
		})
	})
}

func canceller(store Store, codec Codec) TransitionInterceptorFunc {
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
				logger.WithError(haltErr.err).Info("transition returned cancelable error, completing run")
				event.Type = fsmv1.EventType_EVENT_TYPE_CANCEL
				event.Error = haltErr.Error()
			case err != nil:
				return resp, err
			default:
				logger.Info("transition completed successfully")
				if resp != nil && resp.Any() != nil {
					b, err := codec.Marshal(resp.Any())
					if err != nil {
						logger.WithError(err).Error("failed to marshal response")
						return nil, err
					}
					event.Response = b
					// Stash the marshaled response so the finisher can record the run's final
					// result in the terminal record without holding a codec of its own.
					req.setResponse(b)
				}
			}

			switch _, appendErr := store.Append(ctx, run, event, run.Queue); {
			case errors.Is(appendErr, ErrLeaseLost):
				// A fenced append means the run must halt here even though the transition
				// itself succeeded; swallowing it would keep executing without a durable record.
				logger.WithError(appendErr).Warn("append fenced, halting run")
				return resp, appendErr
			case appendErr != nil:
				logger.WithError(appendErr).Error("failed to append complete event")
			}

			return resp, err
		})
	})
}

func retry(tracer trace.Tracer, store Store) TransitionInterceptorFunc {
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
							logger.WithError(err).Error("recovered")
							logger.Error(string(debug.Stack()))
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
						logger.WithError(err).Error("transition aborted")
						return backoff.Permanent(halt(err))
					case isUnrecoverable:
						transitionSpan.SetAttributes(attribute.String("fsm.error_kind", ue.Kind.String()))
						localTransitionCounterVec.WithLabelValues("unrecoverable").Inc()
						localTransitionDurationVec.WithLabelValues("unrecoverable").Observe(time.Since(transitionStartTime).Seconds())
						logger.WithError(err).Error("reached unrecoverable error, canceling FSM")
						return backoff.Permanent(halt(err))
					case isHandoff:
						transitionSpan.SetAttributes(attribute.String("fsm.error_kind", "fsmHandoffError"))
						localTransitionCounterVec.WithLabelValues("fsm_handoff_error").Inc()
						localTransitionDurationVec.WithLabelValues("fsm_handoff_error").Observe(time.Since(transitionStartTime).Seconds())
						logger.WithError(err).Error("reached fsm handoff error, canceling FSM")
						return backoff.Permanent(halt(err))
					case errors.Is(err, ErrLeaseLost):
						// Retrying a fenced write can never succeed; the run halts and the new
						// owner drives it to completion.
						localTransitionCounterVec.WithLabelValues("lease_lost").Inc()
						localTransitionDurationVec.WithLabelValues("lease_lost").Observe(time.Since(transitionStartTime).Seconds())
						logger.WithError(err).Warn("run lease lost, halting")
						return backoff.Permanent(err)
					case errors.Is(err, context.Canceled):
						localTransitionCounterVec.WithLabelValues("canceled").Inc()
						localTransitionDurationVec.WithLabelValues("canceled").Observe(time.Since(transitionStartTime).Seconds())
						logger.Debug("transition received signal to shutdown")
						if cerr := context.Cause(ctx); cerr != context.Canceled {
							logger.WithError(cerr).Error("FSM was intentionally canceled")
							err = halt(cerr)
						}
						return backoff.Permanent(err)
					default:
						localTransitionCounterVec.WithLabelValues("error").Inc()
						localTransitionDurationVec.WithLabelValues("error").Observe(time.Since(transitionStartTime).Seconds())
						logger.WithError(err).Error("transition failed, retrying")
						return err
					}
				},
				boff,
				func(err error, _ time.Duration) {
					switch {
					case lastErr.Error() != err.Error(), retryCount%10 == 0:
						logger.Info("recording transition error")
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
								run.Queue,
							)
						}

						transitionSpan.SetAttributes(attribute.Int("fsm.retry_count", int(retryCount)))
						transitionSpan.SetStatus(codes.Error, err.Error())
						transitionSpan.End()

						lastErr = err

						transitionCtx, transitionSpan = newTransitionSpan(ctx, tracer, run)
					default:
						logger.Info("retrying without recording error")
					}
					retryCount++
					logger = logger.WithField("retry_count", retryCount)
				},
			)

			transitionSpan.SetAttributes(attribute.Int("fsm.retry_count", int(retryCount)))
			transitionSpan.End()

			return resp, err
		})
	})
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
