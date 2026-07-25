package fsm

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"connectrpc.com/connect"
	"github.com/oklog/ulid/v2"
)

// altReq is a second request type sharing an action with orderReq, so the action-only lookup's
// ambiguity check has something to trip on.
type altReq struct{ Name string }

// echoFSM registers a single-transition FSM that echoes the request Name into the response
// Status, so control-API tests can start it opaquely and assert the returned result.
func echoFSM(t *testing.T, m *Manager, action string) {
	t.Helper()

	_, _, err := m.Register[orderReq, orderResp](action).
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return NewResponse(&orderResp{Status: "ok:" + req.Msg.Name}), nil
		}).
		End("done").
		Build(context.Background())
	if err != nil {
		t.Fatalf("failed to build %s FSM: %v", action, err)
	}
}

// startReq builds an opaque StartRequest for the orderReq/orderResp FSMs, marshaling the request
// with the JSON codec those types resolve to.
func startReq(t *testing.T, action, id, name string) *connect.Request[fsmv1.StartRequest] {
	t.Helper()

	resource, err := json.Marshal(orderReq{Name: name})
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}
	return connect.NewRequest(&fsmv1.StartRequest{
		TypeName: "orderReq",
		Action:   action,
		Id:       id,
		Resource: resource,
	})
}

// TestControlStartWait covers the opaque Start → Wait round trip against both backends: a client
// with only serialized bytes submits a run, the worker persists then executes it (via the claim
// loop under the object backend, locally under BoltDB), and Wait returns the W result inline.
func TestControlStartWait(t *testing.T) { runBackends(t, testControlStartWait) }

func testControlStartWait(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	echoFSM(t, m, "control-start")
	admin := &adminServer{m: m}

	startResp, err := admin.Start(ctx, startReq(t, "control-start", "start-1", "hello"))
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	version, err := ulid.Parse(startResp.Msg.GetVersion())
	if err != nil {
		t.Fatalf("Start returned an invalid version %q: %v", startResp.Msg.GetVersion(), err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	waitResp, err := admin.Wait(waitCtx, connect.NewRequest(&fsmv1.WaitRequest{Version: version.String()}))
	if err != nil {
		t.Fatalf("Wait failed: %v", err)
	}
	if waitResp.Msg.GetError() != "" {
		t.Fatalf("run completed with error: %s", waitResp.Msg.GetError())
	}

	var got orderResp
	if err := json.Unmarshal(waitResp.Msg.GetResult(), &got); err != nil {
		t.Fatalf("failed to decode result %q: %v", waitResp.Msg.GetResult(), err)
	}
	if got.Status != "ok:hello" {
		t.Fatalf("expected result status ok:hello, got %q", got.Status)
	}
}

// TestControlStartAlreadyRunning verifies persist-then-ack idempotency on the run id: a second
// Start for an active id is rejected with CodeAlreadyExists and carries the existing version as a
// StartResponse error detail, from the resource lock alone — no execution need have begun.
func TestControlStartAlreadyRunning(t *testing.T) { runBackends(t, testControlStartAlreadyRunning) }

func testControlStartAlreadyRunning(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	blockingFSM(t, m, "control-dup", entered, block)
	defer close(block)
	admin := &adminServer{m: m}

	first, err := admin.Start(ctx, startReq(t, "control-dup", "dup-1", "a"))
	if err != nil {
		t.Fatalf("first Start failed: %v", err)
	}

	_, err = admin.Start(ctx, startReq(t, "control-dup", "dup-1", "b"))
	connErr, ok := errors.AsType[*connect.Error](err)
	if !ok || connErr.Code() != connect.CodeAlreadyExists {
		t.Fatalf("expected CodeAlreadyExists, got %v", err)
	}

	existing, ok := startDetailVersion(connErr)
	if !ok {
		t.Fatal("expected the existing version in the error detail")
	}
	if existing != first.Msg.GetVersion() {
		t.Fatalf("expected existing version %s, got %s", first.Msg.GetVersion(), existing)
	}
}

// TestControlCancel covers cancellation through the RPC ingress: a durable cancel drives the run
// to a terminal state whose Wait reports the cause.
func TestControlCancel(t *testing.T) { runBackends(t, testControlCancel) }

func testControlCancel(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	var (
		entered = make(chan struct{}, 1)
		block   = make(chan struct{})
	)
	blockingFSM(t, m, "control-cancel", entered, block)
	defer close(block)
	admin := &adminServer{m: m}

	startResp, err := admin.Start(ctx, startReq(t, "control-cancel", "cancel-1", "x"))
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	version := startResp.Msg.GetVersion()

	// Wait until the run is actually executing so the cancel reaches a live context.
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("run never started executing")
	}

	const cause = "operator requested"
	if _, err := admin.Cancel(ctx, connect.NewRequest(&fsmv1.CancelRequest{Version: version, Cause: cause})); err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	waitResp, err := admin.Wait(waitCtx, connect.NewRequest(&fsmv1.WaitRequest{Version: version}))
	if err != nil {
		t.Fatalf("Wait failed: %v", err)
	}
	if waitResp.Msg.GetError() != cause {
		t.Fatalf("expected cancel cause %q, got %q", cause, waitResp.Msg.GetError())
	}
}

// TestControlRunsHistory covers the Runs and History reads: a completed run is enumerable by
// resource id and its terminal record — including the W result — is retrievable by version.
func TestControlRunsHistory(t *testing.T) { runBackends(t, testControlRunsHistory) }

func testControlRunsHistory(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	echoFSM(t, m, "control-runs")
	admin := &adminServer{m: m}

	startResp, err := admin.Start(ctx, startReq(t, "control-runs", "runs-1", "z"))
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	version := startResp.Msg.GetVersion()

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := admin.Wait(waitCtx, connect.NewRequest(&fsmv1.WaitRequest{Version: version})); err != nil {
		t.Fatalf("Wait failed: %v", err)
	}

	runsResp, err := admin.Runs(ctx, connect.NewRequest(&fsmv1.RunsRequest{Id: "runs-1"}))
	if err != nil {
		t.Fatalf("Runs failed: %v", err)
	}
	if !contains(runsResp.Msg.GetVersions(), version) {
		t.Fatalf("expected run %s in %v", version, runsResp.Msg.GetVersions())
	}

	histResp, err := admin.History(ctx, connect.NewRequest(&fsmv1.HistoryRequest{RunVersion: version}))
	if err != nil {
		t.Fatalf("History failed: %v", err)
	}
	var got orderResp
	if err := json.Unmarshal(histResp.Msg.GetLastEvent().GetResponse(), &got); err != nil {
		t.Fatalf("failed to decode history result: %v", err)
	}
	if got.Status != "ok:z" {
		t.Fatalf("expected history result ok:z, got %q", got.Status)
	}
}

// TestControlResultReflectsFinalizer verifies the W result a Wait caller receives reflects a
// mutation an End finalizer made — the finisher marshals the final response after finalizers run,
// and both backends surface that same post-finalizer value (the object backend from the terminal
// manifest, BoltDB from the FINISH record).
func TestControlResultReflectsFinalizer(t *testing.T) {
	runBackends(t, testControlResultReflectsFinalizer)
}

func testControlResultReflectsFinalizer(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	_, _, err := m.Register[orderReq, orderResp]("control-final").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			return NewResponse(&orderResp{Status: "from-transition"}), nil
		}).
		End("done", WithFinalizers(func(ctx context.Context, req *Request[orderReq, orderResp], _ RunErr) {
			req.W.Msg.Status = "from-finalizer"
		})).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	admin := &adminServer{m: m}

	startResp, err := admin.Start(ctx, startReq(t, "control-final", "final-1", "x"))
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	waitResp, err := admin.Wait(waitCtx, connect.NewRequest(&fsmv1.WaitRequest{Version: startResp.Msg.GetVersion()}))
	if err != nil {
		t.Fatalf("Wait failed: %v", err)
	}

	var got orderResp
	if err := json.Unmarshal(waitResp.Msg.GetResult(), &got); err != nil {
		t.Fatalf("failed to decode result %q: %v", waitResp.Msg.GetResult(), err)
	}
	if got.Status != "from-finalizer" {
		t.Fatalf("expected the post-finalizer result, got %q", got.Status)
	}
}

// TestControlStartUnregistered verifies an unknown (type, action) is a CodeNotFound client error,
// not a persisted run.
func TestControlStartUnregistered(t *testing.T) { runBackends(t, testControlStartUnregistered) }

func testControlStartUnregistered(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	admin := &adminServer{m: m}

	_, err := admin.Start(context.Background(), startReq(t, "control-missing", "missing-1", "x"))
	connErr, ok := errors.AsType[*connect.Error](err)
	if !ok || connErr.Code() != connect.CodeNotFound {
		t.Fatalf("expected CodeNotFound for an unregistered action, got %v", err)
	}
}

// TestControlStartInvalidPayload verifies a resource that does not decode against the FSM's
// request codec is rejected at ingress as a client error and persists nothing — the object
// backend validates before persisting, the single-process backend before running, so neither
// leaves a poison run for the claim loop.
func TestControlStartInvalidPayload(t *testing.T) { runBackends(t, testControlStartInvalidPayload) }

func testControlStartInvalidPayload(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	echoFSM(t, m, "control-bad")
	admin := &adminServer{m: m}

	_, err := admin.Start(ctx, connect.NewRequest(&fsmv1.StartRequest{
		TypeName: "orderReq",
		Action:   "control-bad",
		Id:       "bad-1",
		Resource: []byte("}{ not valid json"),
	}))
	connErr, ok := errors.AsType[*connect.Error](err)
	if !ok || connErr.Code() != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument for a malformed payload, got %v", err)
	}

	runs, err := admin.Runs(ctx, connect.NewRequest(&fsmv1.RunsRequest{Id: "bad-1"}))
	if err != nil {
		t.Fatalf("Runs failed: %v", err)
	}
	if len(runs.Msg.GetVersions()) != 0 {
		t.Fatalf("a rejected Start must persist nothing, got %v", runs.Msg.GetVersions())
	}
}

// TestControlStartActionOnly covers action-only routing: a unique action resolves without a type,
// but an action shared by more than one registered FSM is ambiguous and rejected as a client
// error.
func TestControlStartActionOnly(t *testing.T) { runBackends(t, testControlStartActionOnly) }

func testControlStartActionOnly(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	echoFSM(t, m, "solo")
	echoFSM(t, m, "ambig") // orderReq/ambig
	_, _, err := m.Register[altReq, orderResp]("ambig").
		Start("created", func(ctx context.Context, req *Request[altReq, orderResp]) (*Response[orderResp], error) {
			return NewResponse(&orderResp{Status: "ok"}), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build altReq/ambig FSM: %v", err)
	}
	admin := &adminServer{m: m}

	resource, err := json.Marshal(orderReq{Name: "x"})
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	resp, err := admin.Start(ctx, connect.NewRequest(&fsmv1.StartRequest{
		Action:   "solo",
		Id:       "solo-1",
		Resource: resource,
	}))
	if err != nil {
		t.Fatalf("action-only Start of a unique action failed: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := admin.Wait(waitCtx, connect.NewRequest(&fsmv1.WaitRequest{Version: resp.Msg.GetVersion()})); err != nil {
		t.Fatalf("Wait failed: %v", err)
	}

	_, err = admin.Start(ctx, connect.NewRequest(&fsmv1.StartRequest{
		Action:   "ambig",
		Id:       "ambig-1",
		Resource: resource,
	}))
	connErr, ok := errors.AsType[*connect.Error](err)
	if !ok || connErr.Code() != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument for an ambiguous action, got %v", err)
	}
}

// TestControlStartOptions covers start-option translation: a parent version is threaded into the
// persisted run's parent-child link, and a malformed option version is a client error.
func TestControlStartOptions(t *testing.T) { runBackends(t, testControlStartOptions) }

func testControlStartOptions(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	echoFSM(t, m, "control-opt")
	admin := &adminServer{m: m}

	resource, err := json.Marshal(orderReq{Name: "x"})
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	_, err = admin.Start(ctx, connect.NewRequest(&fsmv1.StartRequest{
		TypeName: "orderReq",
		Action:   "control-opt",
		Id:       "opt-bad",
		Resource: resource,
		Options:  &fsmv1.StartOptions{RunAfter: "not-a-ulid"},
	}))
	connErr, ok := errors.AsType[*connect.Error](err)
	if !ok || connErr.Code() != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument for a malformed option, got %v", err)
	}

	parent, err := admin.Start(ctx, startReq(t, "control-opt", "parent-1", "p"))
	if err != nil {
		t.Fatalf("parent Start failed: %v", err)
	}
	child, err := admin.Start(ctx, connect.NewRequest(&fsmv1.StartRequest{
		TypeName: "orderReq",
		Action:   "control-opt",
		Id:       "child-1",
		Resource: resource,
		Options:  &fsmv1.StartOptions{Parent: parent.Msg.GetVersion()},
	}))
	if err != nil {
		t.Fatalf("child Start failed: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, v := range []string{parent.Msg.GetVersion(), child.Msg.GetVersion()} {
		if _, err := admin.Wait(waitCtx, connect.NewRequest(&fsmv1.WaitRequest{Version: v})); err != nil {
			t.Fatalf("Wait failed: %v", err)
		}
	}

	parentVersion, err := ulid.Parse(parent.Msg.GetVersion())
	if err != nil {
		t.Fatalf("invalid parent version: %v", err)
	}
	childVersion, err := ulid.Parse(child.Msg.GetVersion())
	if err != nil {
		t.Fatalf("invalid child version: %v", err)
	}
	children, err := m.Children(ctx, parentVersion)
	if err != nil {
		t.Fatalf("Children failed: %v", err)
	}
	if !slices.Contains(children, childVersion) {
		t.Fatalf("expected child %s linked to parent %s, got %v", childVersion, parentVersion, children)
	}
}

// TestFencingToken covers the sequencer: a handler reads its run version and lease epoch through
// FencingToken. The object backend leases each run — a fresh claim bumps the epoch from zero to
// one — while BoltDB has no leases, so its token epoch is zero.
func TestFencingToken(t *testing.T) { runBackends(t, testFencingToken) }

func testFencingToken(t *testing.T, f *managerFactory) {
	m, _ := f.newManager(nil)
	ctx := context.Background()

	tokens := make(chan FencingToken, 1)
	_, _, err := m.Register[orderReq, orderResp]("control-fence").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			tokens <- req.FencingToken()
			return NewResponse(&orderResp{Status: "ok"}), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build fence FSM: %v", err)
	}
	admin := &adminServer{m: m}

	startResp, err := admin.Start(ctx, startReq(t, "control-fence", "fence-1", "x"))
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	version, err := ulid.Parse(startResp.Msg.GetVersion())
	if err != nil {
		t.Fatalf("invalid version: %v", err)
	}

	wantEpoch := int64(0)
	if m.lc != nil {
		wantEpoch = 1
	}

	select {
	case token := <-tokens:
		if token.RunVersion != version {
			t.Fatalf("expected token version %s, got %s", version, token.RunVersion)
		}
		if token.LeaseEpoch != wantEpoch {
			t.Fatalf("expected lease epoch %d, got %d", wantEpoch, token.LeaseEpoch)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never ran")
	}
}

// TestFencingTokenIncrementsOnTakeover verifies the fencing token advances with the lease: when a
// peer takes over a run whose owner's lease lapsed, its handler observes a higher epoch than the
// original owner did — the property a Chubby-style token exists to provide.
func TestFencingTokenIncrementsOnTakeover(t *testing.T) {
	f := newObjectFactoryWith(t, asymmetricTimings(400*time.Millisecond))
	ctx := context.Background()

	m1, _ := f.newManager(nil)
	first := make(chan FencingToken, 1)
	start, _, err := m1.Register[orderReq, orderResp]("fence-takeover").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			first <- req.FencingToken()
			<-ctx.Done()
			return nil, ctx.Err()
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	version, err := start(ctx, "fence-takeover-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}

	var ownerToken FencingToken
	select {
	case ownerToken = <-first:
	case <-time.After(10 * time.Second):
		t.Fatal("owner handler never ran")
	}

	m2, _ := f.newManager(nil)
	second := make(chan FencingToken, 1)
	_, _, err = m2.Register[orderReq, orderResp]("fence-takeover").
		Start("created", func(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
			second <- req.FencingToken()
			return NewResponse(&orderResp{Status: "taken-over"}), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build takeover FSM: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m2.Wait(waitCtx, version); err != nil {
		t.Fatalf("taken-over run completed with error: %v", err)
	}

	select {
	case takeoverToken := <-second:
		if takeoverToken.RunVersion != ownerToken.RunVersion {
			t.Fatalf("expected the same run version across owners, got %s then %s", ownerToken.RunVersion, takeoverToken.RunVersion)
		}
		if takeoverToken.LeaseEpoch <= ownerToken.LeaseEpoch {
			t.Fatalf("expected the epoch to advance on takeover, got %d then %d", ownerToken.LeaseEpoch, takeoverToken.LeaseEpoch)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("takeover handler never ran")
	}
}

func contains(vs []string, want string) bool {
	for _, v := range vs {
		if v == want {
			return true
		}
	}
	return false
}

// startDetailVersion extracts the existing run version an AlreadyExists Start error carries as a
// StartResponse detail.
func startDetailVersion(connErr *connect.Error) (string, bool) {
	for _, d := range connErr.Details() {
		msg, err := d.Value()
		if err != nil {
			continue
		}
		if sr, ok := msg.(*fsmv1.StartResponse); ok {
			return sr.GetVersion(), true
		}
	}
	return "", false
}
