package fsm

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// statusErr builds a smithy response error carrying the given HTTP status, the shape the AWS SDK
// surfaces conditional-write and not-found failures as. storageOutcome classifies by that status.
func statusErr(code int) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: code}},
		Err:      errors.New("boom"),
	}
}

func TestStorageOutcome(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "ok"},
		{"not found", statusErr(http.StatusNotFound), "not_found"},
		{"precondition failed", statusErr(http.StatusPreconditionFailed), "precondition"},
		{"conflict", statusErr(http.StatusConflict), "conflict"},
		{"other status", statusErr(http.StatusInternalServerError), "error"},
		{"non-http error", errors.New("plain"), "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := storageOutcome(tc.err); got != tc.want {
				t.Fatalf("storageOutcome(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// latencyObservations sums the number of samples recorded in the storage latency histogram
// across every op/outcome series, so a test can assert a run added observations rather than
// inspect an absolute value the rest of the suite also moves.
func latencyObservations(t *testing.T) uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	var total uint64
	for _, fam := range families {
		if fam.GetName() != "fsm_object_storage_latency_seconds" {
			continue
		}
		for _, m := range fam.GetMetric() {
			total += m.GetHistogram().GetSampleCount()
		}
	}
	return total
}

// TestObjectStorageMetrics is the smoke test for the object-backend instrumentation: driving one
// run past a heartbeat must record storage-latency observations and move the lease-renewal
// counter. It asserts deltas, not absolutes, so it holds regardless of what the rest of the suite
// has already recorded into these package-global vecs.
func TestObjectStorageMetrics(t *testing.T) {
	f := newObjectFactoryWith(t, func(cfg *ObjectStorageConfig) {
		cfg.LeaseTimeout = 200 * time.Millisecond
		cfg.HeartbeatPeriod = 20 * time.Millisecond
		cfg.ClaimInterval = time.Hour // suppress the periodic claim so only the heartbeat extends
	})
	ctx := context.Background()

	m, _ := f.newManager(nil)
	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	start := blockingFSM(t, m, "metrics", entered, block)

	beforeLatency := latencyObservations(t)
	beforeExtend := testutil.ToFloat64(leaseRenewalsVec.WithLabelValues("extended"))

	version, err := start(ctx, "metrics-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered // the run is executing and holds its lease

	// The heartbeat extends the held lease every HeartbeatPeriod while the transition blocks.
	eventually(t, 2*time.Second, func() bool {
		return testutil.ToFloat64(leaseRenewalsVec.WithLabelValues("extended")) > beforeExtend
	}, "heartbeat never recorded a lease extension")

	close(block)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m.Wait(waitCtx, version); err != nil {
		t.Fatalf("run completed with error: %v", err)
	}

	if got := latencyObservations(t); got <= beforeLatency {
		t.Fatalf("expected storage latency observations to grow, before=%d after=%d", beforeLatency, got)
	}
}
