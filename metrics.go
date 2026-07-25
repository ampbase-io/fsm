package fsm

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Object-storage-specific instrumentation, mirroring the run/transition vecs in fsm.go and
// interceptor.go. These cover the backend the RFC's Observability section names: the latency and
// classification of every object operation, the compare-and-swap retries the conditional-write
// and manifest CAS loops absorb, and the lease renewals the coordinate loop drives.
var (
	storageLatencyVec = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "fsm_object_storage_latency_seconds",
			Help:    "Latency of object storage operations, by op and outcome.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"op", "outcome"},
	)

	casRetriesVec = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fsm_object_storage_cas_retries_total",
			Help: "A count of object storage compare-and-swap retries, by kind.",
		},
		[]string{"kind"},
	)

	leaseRenewalsVec = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fsm_lease_renewals_total",
			Help: "A count of lease renewal outcomes, by result.",
		},
		[]string{"result"},
	)
)

// storageOutcome classifies a raw object storage error into the latency histogram's outcome
// label. It must observe the untouched smithy error — before getObject wraps a 404 into
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

// observeStorage records one object operation's latency under its classified outcome. Called at
// each low-level op site with the raw error, so retried ops observe every attempt.
func observeStorage(op string, start time.Time, err error) {
	storageLatencyVec.WithLabelValues(op, storageOutcome(err)).Observe(time.Since(start).Seconds())
}

// storageBackend names the backend a run executes under, for the fsm.storage_backend span
// attribute: a lease-coordinated store is the object backend, otherwise bolt.
func storageBackend(lc leaseCoordinator) string {
	if lc == nil {
		return "bolt"
	}
	return "object"
}
