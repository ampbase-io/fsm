package fsm

import (
	"context"
	"errors"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Attribute keys shared by the instruments; the run and transition keys match the span
// attributes so a metric and a trace of the same run correlate.
const (
	attrAction         = attribute.Key("fsm.action")
	attrState          = attribute.Key("fsm.state")
	attrResource       = attribute.Key("fsm.resource")
	attrStatus         = attribute.Key("fsm.status")
	attrErrorKind      = attribute.Key("fsm.error.kind")
	attrStorageOp      = attribute.Key("fsm.storage.op")
	attrStorageOutcome = attribute.Key("fsm.storage.outcome")
	attrCASKind        = attribute.Key("fsm.cas.kind")
	attrLeaseResult    = attribute.Key("fsm.lease.result")
	attrQueue          = attribute.Key("fsm.queue")
)

// Bucket advice for a consumer whose SDK has no View for these histograms. A transition can run
// for hours, so the run and transition buckets reach 4h; object operations are sub-second. The
// recommended configuration is a View selecting a base-2 exponential histogram (see README).
var (
	runBuckets     = []float64{.5, 1, 2.5, 5, 10, 30, 60, 150, 300, 600, 1200, 1800, 3600, 7200, 14400}
	storageBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
)

// instruments is every metric the library records, built once per Manager from its Meter and
// threaded to the run loop, the retry interceptor and the object store like the tracer.
type instruments struct {
	runs               metric.Int64Counter
	runDuration        metric.Float64Histogram
	transitions        metric.Int64Counter
	transitionDuration metric.Float64Histogram

	storageDuration metric.Float64Histogram
	casRetries      metric.Int64Counter
	leaseRenewals   metric.Int64Counter
	depth           metric.Int64Gauge
	queueCommits    metric.Int64Counter
}

func newInstruments(meter metric.Meter) (*instruments, error) {
	var errs []error
	counter := func(name, unit, description string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithUnit(unit), metric.WithDescription(description))
		errs = append(errs, err)
		return c
	}
	histogram := func(name, description string, buckets []float64) metric.Float64Histogram {
		h, err := meter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(description), metric.WithExplicitBucketBoundaries(buckets...))
		errs = append(errs, err)
		return h
	}
	depth, err := meter.Int64Gauge("fsm.queue.depth", metric.WithUnit("{run}"), metric.WithDescription("Admitted (in-flight) runs in a queue's roster."))
	errs = append(errs, err)

	i := &instruments{
		runs:               counter("fsm.run.completed", "{run}", "Runs that reached an outcome, by status."),
		runDuration:        histogram("fsm.run.duration", "Time from a run's start to its outcome.", runBuckets),
		transitions:        counter("fsm.transition.completed", "{transition}", "Transition attempts that reached an outcome, by status."),
		transitionDuration: histogram("fsm.transition.duration", "Time from a transition's first attempt to its outcome.", runBuckets),
		storageDuration:    histogram("fsm.object_storage.operation.duration", "Latency of object storage operations, by op and outcome.", storageBuckets),
		casRetries:         counter("fsm.object_storage.cas.retries", "{retry}", "Object storage compare-and-swap retries, by kind."),
		leaseRenewals:      counter("fsm.lease.renewals", "{renewal}", "Lease renewal outcomes, by result."),
		depth:              depth,
		queueCommits:       counter("fsm.queue.commits", "{commit}", "Successful queue-roster CAS writes, by queue."),
	}
	return i, errors.Join(errs...)
}

// observeRun records a run's outcome under its recorded kind (outcomeKind).
func (i *instruments) observeRun(ctx context.Context, run Run, kind fsmv1.HaltKind, start time.Time) {
	attrs := append([]attribute.KeyValue{attrAction.String(run.Action), attrResource.String(run.ResourceName)}, outcomeAttrs(kind)...)
	set := metric.WithAttributeSet(attribute.NewSet(attrs...))
	i.runs.Add(ctx, 1, set)
	i.runDuration.Record(ctx, time.Since(start).Seconds(), set)
}

// outcomeAttrs is an outcome kind as the fsm.status and fsm.error.kind attributes, shared by the
// run and transition metrics and their spans so a trace and a metric of the same run correlate.
func outcomeAttrs(kind fsmv1.HaltKind) []attribute.KeyValue {
	status, errorKind := runStatus(kind)
	attrs := []attribute.KeyValue{attrStatus.String(status)}
	if errorKind != "" {
		attrs = append(attrs, attrErrorKind.String(errorKind))
	}
	return attrs
}

// runStatus maps a halt kind onto the fsm.status and fsm.error.kind values, which predate the
// recorded kind and keep their spellings. A kind without a row reports its own name, so an
// addition shows up in the metric rather than hiding under a neighbour.
func runStatus(kind fsmv1.HaltKind) (status, errorKind string) {
	switch kind {
	case fsmv1.HaltKind_HALT_KIND_UNSPECIFIED:
		return "ok", ""
	case fsmv1.HaltKind_HALT_KIND_CANCELED:
		return "canceled", ""
	case fsmv1.HaltKind_HALT_KIND_ABORT:
		return "abort", ""
	case fsmv1.HaltKind_HALT_KIND_UNRECOVERABLE_SYSTEM:
		return "unrecoverable", ErrorKindSystem
	case fsmv1.HaltKind_HALT_KIND_UNRECOVERABLE_USER:
		return "unrecoverable", ErrorKindUser
	case fsmv1.HaltKind_HALT_KIND_HANDOFF:
		return "fsm_handoff_error", ""
	case fsmv1.HaltKind_HALT_KIND_ERROR:
		return "error", ""
	default:
		return kind.String(), ""
	}
}

// observeTransition records a transition attempt's outcome.
func (i *instruments) observeTransition(ctx context.Context, run Run, status string, start time.Time) {
	set := metric.WithAttributeSet(attribute.NewSet(
		attrAction.String(run.Action),
		attrState.String(run.CurrentState),
		attrResource.String(run.ResourceName),
		attrStatus.String(status),
	))
	i.transitions.Add(ctx, 1, set)
	i.transitionDuration.Record(ctx, time.Since(start).Seconds(), set)
}

// observeStorage records one object operation's latency under its classified outcome. Called at
// each low-level op site with the raw error, so retried ops observe every attempt.
func (i *instruments) observeStorage(ctx context.Context, op string, start time.Time, err error) {
	i.storageDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrStorageOp.String(op), attrStorageOutcome.String(storageOutcome(err))))
}

func (i *instruments) casRetry(ctx context.Context, kind string) {
	i.casRetries.Add(ctx, 1, metric.WithAttributes(attrCASKind.String(kind)))
}

func (i *instruments) leaseRenewal(ctx context.Context, result string) {
	i.leaseRenewals.Add(ctx, 1, metric.WithAttributes(attrLeaseResult.String(result)))
}

func (i *instruments) queueDepth(ctx context.Context, queue string, n int) {
	i.depth.Record(ctx, int64(n), metric.WithAttributes(attrQueue.String(queue)))
}

func (i *instruments) queueCommit(ctx context.Context, queue string) {
	i.queueCommits.Add(ctx, 1, metric.WithAttributes(attrQueue.String(queue)))
}

// storageOutcome classifies a raw object storage error into the operation histogram's outcome
// attribute. It must observe the untouched smithy error — before getObject wraps a 404 into
// ErrFsmNotFound — or the HTTP status classification is lost.
func storageOutcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case isPreconditionFailed(err):
		return "precondition"
	case isConditionalConflict(err):
		return "conflict"
	case isNotFound(err):
		return "not_found"
	default:
		return "error"
	}
}
