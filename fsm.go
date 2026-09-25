package fsm

//go:generate rm -rf gen
//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.28.1 generate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/benbjohnson/immutable"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Request[R, W any] struct {
	Msg *R
	W   Response[W]

	// base is the Manager's logger; logger is base with the run's attributes, rebuilt from
	// base on every transition rather than derived from the previous transition's logger (slog's
	// With appends, so the attributes would accumulate).
	base, logger *slog.Logger
	run          Run

	// leaseEpoch is the epoch this node holds the run's lease at, surfaced to handlers through
	// FencingToken. Zero under a single-process backend, which has no leases.
	leaseEpoch int64

	// response is the last transition's marshaled W, stashed by the canceller (which already
	// holds the codec) so the finisher can record it in the terminal record without a codec of
	// its own.
	response []byte
}

func (r *Request[_, _]) Any() any {
	if r == nil {
		return nil
	}
	return r.Msg
}

// Log returns the run's logger, carrying the run's and the current transition's attributes.
func (r *Request[_, _]) Log() *slog.Logger {
	return r.logger
}

func (r *Request[_, _]) Run() Run {
	return r.run
}

// FencingToken is a run identity paired with the lease epoch its executing node holds. The epoch
// increments on every lease takeover, so successive owners of the same run are strictly ordered.
type FencingToken struct {
	RunVersion ulid.ULID

	LeaseEpoch int64
}

// FencingToken returns the run's version and current lease epoch as a Chubby-style fencing
// token: a handler hands it to an external system so writes from a superseded owner are rejected
// after a takeover. The epoch is zero under a single-process backend — there is no lease to fence.
func (r *Request[_, _]) FencingToken() FencingToken {
	return FencingToken{RunVersion: r.run.StartVersion, LeaseEpoch: r.leaseEpoch}
}

func (r *Request[_, _]) withLogger(base *slog.Logger) {
	r.base = base
	r.logger = base.With(runAttr(r.run))
}

func (r *Request[_, _]) withTransition(name string, version ulid.ULID) {
	r.run.TransitionVersion = version
	r.run.CurrentState = name
	r.logger = r.base.With(runAttr(r.run))
}

// runAttr groups a run's identity under fsm, keyed like the run's span and metric attributes, so
// the three signals share one vocabulary. The transition keys are present once the run is in one.
func runAttr(run Run) slog.Attr {
	attrs := []any{
		"action", run.Action,
		"type", run.TypeName,
		"alias", run.ResourceName,
		"id", run.ID,
		"version", run.StartVersion.String(),
	}
	if run.CurrentState != "" {
		attrs = append(attrs, "state", run.CurrentState, "transition_version", run.TransitionVersion.String())
	}
	return slog.Group("fsm", attrs...)
}

// versionAttr is runAttr for a site that knows a run only by its version.
func versionAttr(version ulid.ULID) slog.Attr {
	return slog.Group("fsm", "version", version.String())
}

func (r *Request[_, _]) withError(err RunErr) {
	r.run.fsmErr = err
}

func (r *Request[_, _]) withLeaseEpoch(epoch int64) {
	r.leaseEpoch = epoch
}

func (r *Request[_, _]) setResponse(b []byte) {
	r.response = b
}

// NewRequest creates a new request to be used for starting a FSM.
func NewRequest[R, W any](msg *R, w *W) *Request[R, W] {
	return &Request[R, W]{
		Msg: msg,
		W:   *NewResponse[W](w),
	}
}

type AnyRequest interface {
	Any() any

	Log() *slog.Logger

	Run() Run

	withLogger(*slog.Logger)

	withTransition(string, ulid.ULID)

	withError(RunErr)

	withLeaseEpoch(int64)

	setResponse([]byte)
}

// MockRequest takes an fsm request and customizes it with logger and run
// objects provided by the caller.
// Note: this should probably be deprecated once better test helpers for
// executing a transition are introduced
func MockRequest[R, W any](req *Request[R, W], logger *slog.Logger, run Run) *Request[R, W] {
	return &Request[R, W]{
		Msg:    req.Msg,
		W:      req.W,
		base:   logger,
		logger: logger.With(runAttr(run)),
		run:    run,
	}
}

type Response[W any] struct {
	Msg *W
}

func (r *Response[_]) Any() any {
	if r == nil {
		return nil
	}
	return r.Msg
}

func (r *Response[_]) internalOnly() {}

func NewResponse[W any](msg *W) *Response[W] {
	return &Response[W]{
		Msg: msg,
	}
}

type AnyResponse interface {
	Any() any

	internalOnly()
}

type RunErr struct {
	Err error

	State string
}

// Run contains the information associated with an active FSM.
type Run struct {
	StartVersion ulid.ULID

	TransitionVersion ulid.ULID

	ID string

	Action string

	CurrentState string

	ResourceName string

	TypeName string

	Queue string

	Parent ulid.ULID

	// fsmErr is the error and originating state that caused the FSM to stop executing transitions.
	fsmErr RunErr
}

type fsm struct {
	action string

	typeName, alias string

	rCodec, wCodec Codec

	startState, endState string

	initializers []InitializerFunc

	transitions *immutable.List[string]

	// transitions is used to lookup a transition by key in order to execute it.
	registeredTransitions map[transitionKey]*transition

	// resumeOne dispatches a single persisted resource through the typed resume path. It is
	// registered at End() so the claim loop can resume runs of this FSM without knowing its
	// R/W types.
	resumeOne func(context.Context, *activeResource) error

	// decodeResource validates opaque request bytes against the R codec. The RPC ingress runs it
	// so a malformed submission fails at Start instead of persisting a run the claim loop can
	// never resume. Registered at End().
	decodeResource func([]byte) error

	// startFromBytes decodes opaque request bytes and runs the typed embedded start. The
	// single-process backend uses it, where no claim loop exists to execute a persisted run.
	// Registered at End().
	startFromBytes func(context.Context, string, []byte, ...StartOptionsFn) (ulid.ULID, error)
}

// startEvent builds the START event for a run of f with the given id.
func (f *fsm) startEvent(id string) *fsmv1.StateEvent {
	return &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_START,
		Id:           id,
		ResourceType: f.typeName,
		Action:       f.action,
		State:        f.startState,
	}
}

func (f *fsm) transitionSlice() []string {
	names := make([]string, 0, f.transitions.Len())
	itr := f.transitions.Iterator()
	for !itr.Done() {
		_, value := itr.Next()
		names = append(names, value)
	}
	return names
}

type transitionKey struct {
	action string

	typeName string

	name string
}

type transition struct {
	name string

	impl TransitionFunc
}

type TransitionFunc func(context.Context, AnyRequest) (AnyResponse, error)

// Attributable is an interface that can be implemented by a request to include additional Span
// attributes.
type Attributable interface {
	Attributes() []attribute.KeyValue
}

func newTransition[R, W any](name string, transitionFn func(context.Context, *Request[R, W]) (*Response[W], error), cfg TransitionConfig[R, W]) *transition {
	// Wrap the strongly-typed implementation so we can apply interceptors.
	untyped := TransitionFunc(func(ctx context.Context, request AnyRequest) (AnyResponse, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		typed, ok := request.(*Request[R, W])
		if !ok {
			return nil, fmt.Errorf("unexpected handler request type %T", request)
		}
		res, err := transitionFn(ctx, typed)
		if res != nil {
			typed.W = *res
		}
		return res, err
	})

	if interceptors := cfg.interceptors; interceptors != nil {
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			untyped = interceptor(untyped)
		}
	}

	return &transition{
		name: name,
		impl: untyped,
	}
}

type InitializerFunc func(context.Context, AnyRequest) context.Context

func newInitializer[R, W any](initFn func(context.Context, *Request[R, W]) context.Context) InitializerFunc {
	return InitializerFunc(func(ctx context.Context, request AnyRequest) context.Context {
		typed, ok := request.(*Request[R, W])
		if !ok {
			return ctx
		}
		return initFn(ctx, typed)
	})
}

type FinalizerFunc func(context.Context, AnyRequest, RunErr)

func newFinalizer[R, W any](finalFn func(context.Context, *Request[R, W], RunErr)) FinalizerFunc {
	return FinalizerFunc(func(ctx context.Context, request AnyRequest, err RunErr) {
		typedReq, ok := request.(*Request[R, W])
		if !ok {
			return
		}
		finalFn(ctx, typedReq, err)
	})
}

// runSnapshot is a run with the state and error a backend has recorded for it.
type runSnapshot struct {
	Run

	State fsmv1.RunState

	Error RunErr
}

// nodeIdentified is a backend with a node identity; one without is single-process and has no
// owner to record on a run span.
type nodeIdentified interface {
	nodeID() string
}

// key identifies this FSM by the resource type and action its runs are recorded under.
func (f *fsm) key() fsmKey {
	return fsmKey{typeName: f.typeName, action: f.action}
}

// resume drives every resumable run of f through resumeOne. Which runs are resumable is
// backend-defined: a lease-coordinated backend hands out only runs this node claimed. Every
// run is dispatched even when another fails — the claims are already this node's, and an
// undispatched claimed run would stay heartbeat-extended but never execute.
func (m *Manager) resume(ctx context.Context, f *fsm) error {
	resources, err := m.resumable(ctx, f)
	if err != nil {
		return err
	}

	var errs error
	for _, resource := range resources {
		errs = errors.Join(errs, f.resumeOne(ctx, resource))
	}
	return errs
}

// resumeOne rebuilds the typed request from a persisted resource and dispatches run() with
// restart semantics, honoring the run's recorded queue, delay, and dependency options.
func (m *Manager) resumeOne[R, W any](f *fsm) func(ctx context.Context, resource *activeResource) error {
	return func(ctx context.Context, resource *activeResource) error {
		clearRun := func(run Run) {
			if err := m.store.ForgetRun(run); err != nil {
				m.logger.ErrorContext(ctx, "failed to update fsm state store", "error", err)
			}
		}

		// Restore the run options from the persisted event; the queue and parent must reflect
		// this resource, not whatever the most recent Start call left on f.
		var startOpt startOptions
		if delayUntil := resource.active.GetOptions().GetDelayUntil(); delayUntil > 0 {
			startOpt.until = time.UnixMilli(delayUntil)
		}

		if runAfter := resource.active.GetOptions().GetRunAfter(); runAfter != nil {
			if err := startOpt.runAfter.UnmarshalText(runAfter); err != nil {
				m.logger.ErrorContext(ctx, "failed to unmarshal run_after", "error", err)
			}
		}

		startOpt.queue = resource.active.GetOptions().GetQueue()

		if parentBytes := resource.active.GetOptions().GetParent(); parentBytes != nil {
			if err := startOpt.parent.UnmarshalText(parentBytes); err != nil {
				m.logger.ErrorContext(ctx, "failed to unmarshal parent", "error", err)
			}
		}

		r := Run{
			ID:           resource.active.GetResourceId(),
			StartVersion: resource.version,
			Action:       f.action,
			ResourceName: f.alias,
			TypeName:     f.typeName,
			Queue:        startOpt.queue,
			Parent:       startOpt.parent,
			fsmErr:       resource.fsmError,
		}

		var req R
		if err := f.rCodec.Unmarshal(resource.active.Resource, &req); err != nil {
			m.logger.ErrorContext(ctx, "failed to unmarshal resource, unable to resume", "error", err)
			clearRun(r)
			return err
		}

		var w W
		if resource.response != nil {
			if err := f.wCodec.Unmarshal(resource.response, &w); err != nil {
				m.logger.ErrorContext(ctx, "failed to unmarshal response, unable to resume", "error", err, "response_bytes", string(resource.response))
				clearRun(r)
				return err
			}
		}
		m.logger.DebugContext(ctx, "pruning completed transitions", "completed", resource.completedTransitions)

		remainingTransitions := immutable.NewList[*transition]()
		for _, name := range resource.active.Transitions {
			if !slices.Contains(resource.completedTransitions, name) {
				transition, ok := f.registeredTransitions[transitionKey{
					action:   f.action,
					typeName: f.typeName,
					name:     name,
				}]
				if !ok {
					m.logger.WarnContext(ctx, "transition did not exist", slog.Group("fsm", "version", resource.version.String(), "state", name))
					transition = newTransition(name, noOp, TransitionConfig[R, W]{
						interceptors: []TransitionInterceptorFunc{
							skipper(),
							canceller(m.store, f.wCodec),
						},
					})
				}
				remainingTransitions = remainingTransitions.Append(transition)
			}
		}

		ctx = withRetry(ctx, resource.retryCount)
		ctx = withRestart(ctx, true)

		ctx = (propagation.TraceContext{}).Extract(ctx, propagation.MapCarrier(resource.active.TraceContext))

		runner := runnerFromOpts(&startOpt, m)

		request := NewRequest(&req, &w)
		request.run = r

		run(ctx, request, m, runner, &runInstance{initializers: f.initializers, transitions: remainingTransitions})
		return nil
	}
}

type StartOptionsFn func(*startOptions)

type startOptions struct {
	until time.Time

	runAfter ulid.ULID

	queue string

	parent ulid.ULID
}

// WithDelayedStart will delay the start of the FSM until the provided time.
func WithDelayedStart(until time.Time) StartOptionsFn {
	return func(opts *startOptions) {
		opts.until = until
	}
}

// WithRunAfter will delay the start of the FSM until the FSM with the version has completed.
func WithRunAfter(version ulid.ULID) StartOptionsFn {
	return func(opts *startOptions) {
		opts.runAfter = version
	}
}

// WithQueue will attempt to run the FSM if there is capacity in the queue, otherwise it will queue
// the FSM to be run until capacity is available.
func WithQueue(queue string) StartOptionsFn {
	return func(opts *startOptions) {
		opts.queue = queue
	}
}

// WithParent records the run as a child of parent, so Children and ActiveChildren list it. It is
// an index entry and nothing more. The child's lifetime is its own: canceling or finishing the
// parent does not reach it. And its id is not scoped to the parent: two parents that start a
// child under one id contend for the same resource, and the second gets an AlreadyRunningError
// naming the first's run. A parent that adopts the run such an error names should give its
// children ids no other parent can produce — its own run version in the id does.
func WithParent(parent ulid.ULID) StartOptionsFn {
	return func(opts *startOptions) {
		opts.parent = parent
	}
}

// start attempts to start the FSM using the provided id and request. The id is used to uniquely
// identify the FSM associated with the req type along with the action used to register it.
func (m *Manager) start[R, W any](f *fsm) func(ctx context.Context, id string, request *Request[R, W], opts ...StartOptionsFn) (ulid.ULID, error) {
	return func(ctx context.Context, id string, request *Request[R, W], opts ...StartOptionsFn) (ulid.ULID, error) {
		var startOpt startOptions
		for _, opt := range opts {
			opt(&startOpt)
		}

		logger := m.logger.With(slog.Group("fsm", "action", f.action, "type", f.typeName, "alias", f.alias, "id", id))

		resource, err := f.rCodec.Marshal(request.Msg)
		if err != nil {
			logger.ErrorContext(ctx, "failed to marshal request", "error", err)
			return ulid.ULID{}, fmt.Errorf("failed to marshal request: %w", err)
		}

		runVersion := ulid.Make()

		// On the object backend a queued run is admission-controlled cluster-wide, so it is
		// persisted unowned and the claim loop admits and executes it — exactly the RPC ingress
		// path. Executing it locally here would bypass the shared capacity limit (this node would
		// admit up to `size` of its own). Delayed/run-after runs are not admission-controlled and
		// keep executing locally (matching runnerFromOpts precedence).
		if _, ok := m.store.(runClaimer); ok && admissionControlled(&startOpt) {
			if _, err := m.persistStart(ctx, f, id, runVersion, resource, &startOpt, true); err != nil {
				logger.ErrorContext(ctx, "failed to append start event", "error", err)
				return ulid.ULID{}, err
			}
			m.nudgeClaim()
			return runVersion, nil
		}

		ctx = withRestart(ctx, false)

		r := runnerFromOpts(&startOpt, m)

		transitions := immutable.NewList[*transition]()
		iter := f.transitions.Iterator()
		for !iter.Done() {
			_, value := iter.Next()
			transitions = transitions.Append(f.registeredTransitions[transitionKey{
				action:   f.action,
				typeName: f.typeName,
				name:     value,
			}])
		}

		startedRun, err := m.persistStart(ctx, f, id, runVersion, resource, &startOpt, false)
		if err != nil {
			logger.ErrorContext(ctx, "failed to append start event", "error", err)
			return ulid.ULID{}, err
		}
		request.run = startedRun

		run(ctx, request, m, r, &runInstance{initializers: f.initializers, transitions: transitions})

		return runVersion, nil
	}
}

// persistStart writes a run's START event through the store and returns the Run it recorded. It
// is the single home for the START persistence contract: the embedded start (leased to this node)
// and the opaque ingress start (unowned, for the claim loop) share it, differing only in whether
// the START is persisted unowned. Non-generic — persistence never touches R/W.
func (m *Manager) persistStart(ctx context.Context, f *fsm, id string, runVersion ulid.ULID, resource []byte, startOpt *startOptions, unowned bool) (Run, error) {
	run := Run{
		ID:           id,
		StartVersion: runVersion,
		Action:       f.action,
		ResourceName: f.alias,
		TypeName:     f.typeName,
		Queue:        startOpt.queue,
		Parent:       startOpt.parent,
	}
	start := &startRecord{
		Resource:    resource,
		Transitions: f.transitionSlice(),
		DelayUntil:  startOpt.until,
		RunAfter:    startOpt.runAfter,
		Unowned:     unowned,
	}
	if _, err := m.store.Start(ctx, run, f.startEvent(id), start); err != nil {
		return Run{}, err
	}
	return run, nil
}

// startFromBytes decodes an opaque request payload into the FSM's R type and runs the typed
// embedded start. It is the single-process backend's opaque entry point: with no claim loop to
// execute a persisted run, the accepting node runs it directly. Registered at End() so the RPC
// ingress can start a run without knowing R/W.
func (m *Manager) startFromBytes[R, W any](f *fsm) func(context.Context, string, []byte, ...StartOptionsFn) (ulid.ULID, error) {
	return func(ctx context.Context, id string, resource []byte, opts ...StartOptionsFn) (ulid.ULID, error) {
		var r R
		if err := f.rCodec.Unmarshal(resource, &r); err != nil {
			return ulid.ULID{}, invalidResource(f, err)
		}
		var w W
		return m.start[R, W](f)(ctx, id, NewRequest(&r, &w), opts...)
	}
}

// decodeResourceFn returns a validator that decodes opaque request bytes into R, reporting a
// codec error as errInvalidResource, without executing the run.
func decodeResourceFn[R any](f *fsm) func([]byte) error {
	return func(b []byte) error {
		var r R
		if err := f.rCodec.Unmarshal(b, &r); err != nil {
			return invalidResource(f, err)
		}
		return nil
	}
}

// invalidResource wraps a request-codec decode failure as errInvalidResource, naming the FSM the
// payload was rejected for.
func invalidResource(f *fsm, err error) error {
	return fmt.Errorf("%w for %s/%s: %w", errInvalidResource, f.typeName, f.action, err)
}

type runInstance struct {
	initializers []InitializerFunc

	transitions *immutable.List[*transition]
}

func run(ctx context.Context, request AnyRequest, m *Manager, r runner, ri *runInstance) {
	// We create a new context that is not cancelable so that we can control the lifecycle of the FSM
	// separately from the context that is passed in.
	ctx = context.WithoutCancel(ctx)

	var (
		run        = request.Run()
		runVersion = run.StartVersion
		action     = run.Action
		alias      = run.ResourceName
		typeName   = run.TypeName
		parent     = run.Parent
	)

	// Only a backend with a node identity attributes the run to an owner node; bolt has neither a
	// node identity nor leases, so it is labeled the single-process backend with no owner.
	backend := "bolt"
	startAttrs := []attribute.KeyValue{
		attribute.String("fsm.action", action),
		attribute.String("fsm.alias", alias),
		attribute.String("fsm.type", typeName),
		attribute.String("fsm.version", runVersion.String()),
		attribute.Int("fsm.sdk_version", 2),
	}
	if node, ok := m.store.(nodeIdentified); ok {
		backend = "object"
		startAttrs = append(startAttrs, attribute.String("fsm.owner_node", node.nodeID()))
	}
	startAttrs = append(startAttrs, attribute.String("fsm.storage_backend", backend))
	if attr, ok := request.Any().(Attributable); ok {
		startAttrs = append(startAttrs, attr.Attributes()...)
	}

	startOpts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(startAttrs...),
	}

	// If the FSM does not have a parent, we create a new root span and connect to the caller's span
	// with the link.
	if parent.Compare(ulid.ULID{}) == 0 {
		startOpts = append(startOpts,
			trace.WithNewRoot(),
			trace.WithLinks(trace.LinkFromContext(ctx)),
		)
	}

	ctx, span := m.tracer.Start(ctx, fmt.Sprintf("%s.%s", alias, action), startOpts...)

	logger := m.logger.With(runAttr(run))

	runFn := func() {
		// What stops the run and what halts its transitions are different events: runCtx ends on
		// a shutdown or a lost lease, ctx additionally on an operator's cancel.
		runCtx, stop := context.WithCancelCause(ctx)
		ctx, cancel := context.WithCancelCause(runCtx)

		m.mu.Lock()
		m.running[runVersion] = runHandle{cancel: cancel, stop: stop}
		m.mu.Unlock()

		// A lease-coordinated run may have lost ownership before reaching execution — a
		// delayed or queued dispatch can trail its claim by arbitrarily long. Stop before
		// any side effects run rather than waiting to be fenced on the first write. The same
		// lookup yields the lease epoch handlers read as their fencing token.
		if f, ok := m.store.(fencer); ok {
			epoch, owned := f.ownedEpoch(runVersion)
			if !owned {
				stop(ErrLeaseLost)
			}
			request.withLeaseEpoch(epoch)
			span.SetAttributes(attribute.Int64("fsm.lease_epoch", epoch))
		}

		defer func() {
			span.End()
			m.mu.Lock()
			delete(m.running, runVersion)
			m.mu.Unlock()
			stop(nil)
		}()

		logger.InfoContext(ctx, "starting fsm")

		// The run's duration counts from its submission, so a queued or delayed wait is included.
		runStart := ulid.Time(runVersion.Time())

		request.withLogger(m.logger)
		for _, init := range ri.initializers {
			ctx = init(ctx, request)
		}

		finalizerCtx, release := finalizerContext(ctx, runCtx)
		defer release()

		// The finisher is always the last transition, on a fresh start and on a resume alike.
		finisher := ri.transitions.Len() - 1

		iter := ri.transitions.Iterator()
		for !iter.Done() {
			idx, transition := iter.Next()
			transitionName := transition.name
			transitionVersion := ulid.Make()
			request.withTransition(transitionName, transitionVersion)
			transitionLogger := request.Log()

			if stopped(runCtx, transitionLogger) {
				return
			}

			transitionLogger.DebugContext(ctx, "running transition")

			transitionCtx := ctx
			if idx == finisher {
				transitionCtx = finalizerCtx
			}
			_, err := transition.impl(transitionCtx, request)

			if stopped(runCtx, transitionLogger) {
				return
			}

			if errors.Is(err, ErrLeaseLost) {
				// Halt without finalizers or FINISH: this node may no longer write to the run,
				// and the new owner runs them at its own finish.
				transitionLogger.WarnContext(ctx, "run lease lost, halting")
				return
			}

			switch cancel, canceled := errors.AsType[*CancelError](context.Cause(transitionCtx)); {
			case canceled && !isHalt(err):
				// The cancel is the run's outcome whether the handler finished its work after
				// it landed or a retry's sleep returned the bare context error: Cancel has
				// already answered its caller.
				err = halt(cancel)
			case err == nil:
				continue
			}

			// The first halt is the run's outcome. Every transition after it is skipped, yet
			// still sees the canceled context, and must not claim the halt as its own.
			if request.Run().fsmErr.Err != nil {
				continue
			}

			kind := outcomeKind(err)
			m.instruments.observeRun(ctx, run, kind, runStart)
			span.SetAttributes(attribute.String("fsm.error_kind", kind))
			if _, unrecoverable := errors.AsType[*UnrecoverableError](err); unrecoverable {
				transitionLogger.ErrorContext(ctx, "reached unrecoverable error, canceling FSM", "error", err)
			}
			request.withError(RunErr{
				Err:   err,
				State: transitionName,
			})
		}
		if request.Run().fsmErr.Err == nil {
			m.instruments.observeRun(ctx, run, kindOK, runStart)
		}
	}

	m.wg.Add(1)
	ack := make(chan struct{})
	go func() {
		defer m.wg.Done()
		r.Run(ctx, logger, ack, runFn)
		// A finished queued run on the object backend frees a roster slot; wake the local claim
		// loop so the next pending run is admitted at once instead of on the periodic pass. This
		// is the no-bus path — a live bus additionally gets the subjectPending publish from the
		// store's finish cleanup.
		if _, ok := m.store.(runClaimer); ok && run.Queue != "" {
			m.nudgeClaim()
		}
	}()

	<-ack
	return
}

// stopped reports whether the run was ended short of an outcome by a shutdown or a lost lease.
// Nothing more may be recorded; the run resumes where it left off.
func stopped(runCtx context.Context, logger *slog.Logger) bool {
	cause := context.Cause(runCtx)
	if cause == nil {
		return false
	}
	logger.InfoContext(runCtx, "run stopped", "error", cause)
	return true
}

// finalizerContext returns a context carrying ctx's values that ends only when the run is
// stopped, so finalizers outlive the operator's cancel that halted the transitions.
func finalizerContext(ctx, runCtx context.Context) (context.Context, func()) {
	finalizerCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	unlink := context.AfterFunc(runCtx, func() { cancel(context.Cause(runCtx)) })
	return finalizerCtx, func() {
		unlink()
		cancel(nil)
	}
}

func noOp[R, W any](ctx context.Context, req *Request[R, W]) (*Response[W], error) {
	return nil, nil
}
