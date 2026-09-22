package fsm

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

// testInstruments builds instruments that record nowhere, for store-level tests that construct
// an objectStore directly.
func testInstruments(t *testing.T) *instruments {
	t.Helper()
	i, err := newInstruments(noop.NewMeterProvider().Meter("fsm"))
	if err != nil {
		t.Fatalf("failed to create instruments: %v", err)
	}
	return i
}

// durationView is the histogram configuration the README recommends: base-2 exponential buckets
// for every fsm duration, so a transition of minutes or hours lands in a real bucket. The tests
// run under it so the example is exercised, not just written.
var durationView = sdkmetric.NewView(
	sdkmetric.Instrument{Name: "fsm.*.duration"},
	sdkmetric.Stream{Aggregation: sdkmetric.AggregationBase2ExponentialHistogram{MaxSize: 160, MaxScale: 20}},
)

// metricsReader injects a manual-read SDK provider into the backend's managers and returns the
// reader to collect from.
func metricsReader(b *backend) *sdkmetric.ManualReader {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(durationView))
	b.configureManager = func(cfg *Config) { cfg.MeterProvider = provider }
	return reader
}

// collect reads every metric the provider has, keyed by instrument name.
func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}
	metrics := map[string]metricdata.Metrics{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			metrics[m.Name] = m
		}
	}
	return metrics
}

// hasAttr reports whether the set carries key=value.
func hasAttr(set attribute.Set, key attribute.Key, value string) bool {
	v, ok := set.Value(key)
	return ok && v.AsString() == value
}

// counterValue sums an Int64 counter's data points whose attributes carry key=value.
func counterValue(t *testing.T, metrics map[string]metricdata.Metrics, name string, key attribute.Key, value string) int64 {
	t.Helper()
	m, ok := metrics[name]
	if !ok {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s: expected an int64 sum, got %T", name, m.Data)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		if hasAttr(dp.Attributes, key, value) {
			total += dp.Value
		}
	}
	return total
}

// histogramCount sums an exponential histogram's counts across its data points.
func histogramCount(t *testing.T, metrics map[string]metricdata.Metrics, name string) uint64 {
	t.Helper()
	m, ok := metrics[name]
	if !ok {
		return 0
	}
	h, ok := m.Data.(metricdata.ExponentialHistogram[float64])
	if !ok {
		t.Fatalf("%s: expected an exponential histogram under the duration view, got %T", name, m.Data)
	}
	var total uint64
	for _, dp := range h.DataPoints {
		total += dp.Count
	}
	return total
}

// TestRunMetrics verifies the run and transition instruments on both backends: a run that
// completes records itself and each of its transitions as ok, and one that aborts records the
// abort — with the duration histograms counting alongside under the exponential view.
func TestRunMetrics(t *testing.T) { runBackends(t, testRunMetrics) }

func testRunMetrics(t *testing.T, b *backend) {
	ctx := context.Background()
	reader := metricsReader(b)
	m, _ := b.newManager(nil)

	pass := func(context.Context, *Request[orderReq, orderResp]) (*Response[orderResp], error) {
		return nil, nil
	}
	start, _, err := m.Register[orderReq, orderResp]("measured").
		Start("first", pass).
		To("second", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			if req.Msg.Name == "abort" {
				return nil, Abort(errors.New("boom"))
			}
			return nil, nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	waitFor := func(id, name string) error {
		version, err := start(ctx, id, NewRequest(&orderReq{Name: name}, &orderResp{}))
		if err != nil {
			t.Fatalf("failed to start FSM: %v", err)
		}
		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return m.Wait(waitCtx, version)
	}
	if err := waitFor("measured-ok", "ok"); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if err := waitFor("measured-abort", "abort"); err == nil {
		t.Fatal("expected the aborting run to fail")
	}

	metrics := collect(t, reader)
	if got := counterValue(t, metrics, "fsm.run.completed", attrStatus, "ok"); got != 1 {
		t.Fatalf("expected one run completed ok, got %d", got)
	}
	if got := counterValue(t, metrics, "fsm.run.completed", attrStatus, "abort"); got != 1 {
		t.Fatalf("expected one run aborted, got %d", got)
	}
	// first, second, done for the ok run; first and done for the aborting one — the finisher
	// records the halt, so it runs and completes ok — and its second is recorded as abort.
	if got := counterValue(t, metrics, "fsm.transition.completed", attrStatus, "ok"); got != 5 {
		t.Fatalf("expected five transitions completed ok, got %d", got)
	}
	if got := counterValue(t, metrics, "fsm.transition.completed", attrStatus, "abort"); got != 1 {
		t.Fatalf("expected one transition aborted, got %d", got)
	}
	if got := counterValue(t, metrics, "fsm.transition.completed", attrState, "second"); got != 2 {
		t.Fatalf("expected the state attribute on transition points, got %d for second", got)
	}
	if got := histogramCount(t, metrics, "fsm.run.duration"); got != 2 {
		t.Fatalf("expected two run duration samples, got %d", got)
	}
	if got := histogramCount(t, metrics, "fsm.transition.duration"); got != 6 {
		t.Fatalf("expected six transition duration samples, got %d", got)
	}
}

// TestObjectStorageMetrics is the smoke test for the object-backend instrumentation: driving one
// run past a heartbeat must record storage operations and lease extensions.
func TestObjectStorageMetrics(t *testing.T) {
	b := newObjectBackendWith(t, func(cfg *ObjectStorageConfig) {
		cfg.LeaseTimeout = 200 * time.Millisecond
		cfg.HeartbeatPeriod = 20 * time.Millisecond
		cfg.ClaimInterval = time.Hour // suppress the periodic claim so only the heartbeat extends
	})
	ctx := context.Background()
	reader := metricsReader(b)

	m, _ := b.newManager(nil)
	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	start := blockingFSM(t, m, "metrics", entered, block)

	version, err := start(ctx, "metrics-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	<-entered // the run is executing and holds its lease

	// The heartbeat extends the held lease every HeartbeatPeriod while the transition blocks.
	eventually(t, 2*time.Second, func() bool {
		return counterValue(t, collect(t, reader), "fsm.lease.renewals", attrLeaseResult, "extended") > 0
	}, "heartbeat never recorded a lease extension")

	close(block)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m.Wait(waitCtx, version); err != nil {
		t.Fatalf("run completed with error: %v", err)
	}

	metrics := collect(t, reader)
	if got := histogramCount(t, metrics, "fsm.object_storage.operation.duration"); got == 0 {
		t.Fatal("expected storage operation durations recorded")
	}
	m2, ok := metrics["fsm.object_storage.operation.duration"]
	if !ok {
		t.Fatal("storage duration instrument missing")
	}
	for _, dp := range m2.Data.(metricdata.ExponentialHistogram[float64]).DataPoints {
		if _, ok := dp.Attributes.Value(attrStorageOp); !ok {
			t.Fatalf("expected the op attribute on every storage data point, got %v", dp.Attributes)
		}
	}
}
