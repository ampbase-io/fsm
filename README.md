# fsm

A persistent finite state machine library. FSMs are registered against a `Manager`, execute a
series of named transitions, and persist their progress so interrupted runs can be resumed after a
restart.

Requires Go 1.27+ (the builder API uses generic methods).

## Usage

```go
m, err := fsm.New(fsm.Config{DBPath: "/var/lib/myapp/fsm"})
if err != nil {
    // ...
}

type CreateReq struct{ Name string }
type CreateResp struct{ ID string }

start, resume, err := m.Register[CreateReq, CreateResp]("create").
    Start("created", func(ctx context.Context, req *fsm.Request[CreateReq, CreateResp]) (*fsm.Response[CreateResp], error) {
        return fsm.NewResponse(&CreateResp{ID: req.Msg.Name}), nil
    }).
    To("verified", func(ctx context.Context, req *fsm.Request[CreateReq, CreateResp]) (*fsm.Response[CreateResp], error) {
        // req.W.Msg holds the previous transition's response.
        return nil, nil
    }).
    End("done").
    Build(ctx)
if err != nil {
    // ...
}

// Resume any runs that were interrupted before completing.
if err := resume(ctx); err != nil {
    // ...
}

version, err := start(ctx, "resource-id", fsm.NewRequest(&CreateReq{Name: "widget"}, &CreateResp{}))
if err != nil {
    // ...
}

// Block until the run completes.
if err := m.Wait(ctx, version); err != nil {
    // ...
}
```

`Wait` returns the run's recorded error, nil on success, or `fsm.ErrFsmNotFound` for a version
the backend does not know.

Request/response types are persisted with a protobuf codec when they implement `proto.Message`,
with a custom codec when they implement `fsm.Codec`, and with JSON otherwise.

## Interceptors

An interceptor wraps a transition's handler: it sees the request before and the response and
error after. `fsm.WithInterceptors` attaches interceptors to one transition, and
`fsm.InterceptAll`, passed to `End`, attaches them to every transition declared with `Start` or
`To`:

```go
Start("reserve", reserve).
To("create_flag", createFlag, fsm.WithInterceptors[Req, Resp](timing)).
End("done", fsm.InterceptAll[Req, Resp](audit)).
```

- **They run once per attempt.** Both kinds run inside the retry, so a retried transition calls
  them again, and a retryable error reaches them before it is retried. An error an interceptor
  returns is retried like the handler's.
- **FSM-wide interceptors run outside a transition's own.** In the example, `audit` sees
  `create_flag`'s request before `timing` does.
- **The finisher runs none of them,** and neither does a `RepeatDone`.

## Repeating a transition

`fsm.RepeatWhile` makes a transition run once per iteration its predicate allows, so a run whose
length depends on its request keeps one fixed definition:

```go
To("stage", stage, fsm.RepeatWhile(func(ctx context.Context, req *fsm.Request[RolloutReq, Verdict]) (fsm.Repeat, error) {
    if req.Run().Iteration < len(req.Msg.Stages) {
        return fsm.RepeatAgain(), nil
    }
    return fsm.RepeatDone(), nil
}))
```

- **The predicate decides before every iteration, including the first.** It sees the index about
  to run in `req.Run().Iteration`; answering `RepeatDone` at zero runs no iteration. The zero
  `fsm.Repeat` is no decision and halts the run as an unrecoverable system error.
- **Only the count is recorded.** Each iteration is its own COMPLETE event carrying
  `iterations_completed`, its index plus one. A
  resumed run re-asks the predicate at the index after the last completed iteration, so the
  answer must follow from the request and the index alone.
- **Its errors are the transition's.** `fsm.Abort` and the unrecoverable errors halt the run in
  the repeated transition; any other error is retried under the transition's backoff, and the
  predicate is asked again before every retry.
- **Each iteration is a transition to everything around it.** It runs under a fresh transition
  version, through the transition's interceptors, with its own span. A `RepeatDone` reaches
  none of those interceptors; it records the transition finished, as a COMPLETE with zero
  iterations completed, so a resumed run does not re-enter it.

## Cancellation

A transition's context ends for one of three reasons, and `context.Cause(ctx)` names which:

| Cause | Meaning | What the handler should do |
|---|---|---|
| `*fsm.CancelError` | `Manager.Cancel` was called; `Reason` carries its cause. | Stop. The run halts, skips its remaining transitions, and runs its finalizers. |
| `fsm.ErrShutdown` | This `Manager` is shutting down. | Return promptly and record nothing; the run resumes on the next start or claim. |
| `fsm.ErrLeaseLost` | Another node now owns the run (object storage backend only). | Return promptly and record nothing; the new owner is already running it. |

```go
<-ctx.Done()
if cancel, ok := errors.AsType[*fsm.CancelError](context.Cause(ctx)); ok {
    // An operator stopped the run: cancel.Reason says why.
}
return nil, ctx.Err()
```

Those three are every cause the library sets. A later version may add one — always an exported
value, never an anonymous error — so treat a cause you do not recognize as a reason to stop, not
as an operator's intent. A context an initializer derived can also end for reasons of its own.

A run's outcome is typed on every node. `Wait` returns the same `*fsm.CancelError`,
`*fsm.AbortError` or `*fsm.UnrecoverableError` whether the run finished in this process, on
another node, or before its record was archived, so `errors.AsType[*fsm.CancelError](err)` is
the way to ask "was that a cancel?" — never the message text. The FINISH event `History`
returns carries the same classification as `halt_kind`, with `error_state` naming the
transition the run halted in.

A run that has not started anywhere — waiting on a queue with no free slot, or on a delay — is
settled by `Cancel` where it stands: its cause is recorded, its resource is freed, and its waiters
get the same `*fsm.CancelError` an admitted run's would, so `Wait` and `History` cannot tell the two
apart. It never executes a transition, and its finalizers do not run: nothing has happened that
needs finalizing. A cancel that races the run starting resolves to exactly one of the two — the run
either starts and is canceled through its context, or is settled and never starts.

Finalizers run on a context that an operator's cancel does not end, so they can do the work the
cancel calls for — start a compensating run and wait on it, say. It carries the values
initializers put on the transitions' context but none of their cancellation, and no deadline:
the run's lease is held until its finalizers return, so that wait may take minutes. The context
still ends on `ErrShutdown` and `ErrLeaseLost`, and a finalizer runs again if the run is resumed
before it finished.

## Signals

A signal is a named, typed message sent to a running run, read by whichever transition asks for
that name. Declare each signal once, accept it on the FSM, send it from any node, and receive it
in a transition:

```go
var Advance = fsm.NewSignal[Command]("advance")

End("done", fsm.WithSignals[Req, Resp](Advance))

id, err := Advance.Send(ctx, m, version, &Command{Reason: "looks good"})

// in a transition
select {
case adv := <-Advance.Receive(req): // adv.ID, adv.SentAt, adv.Msg (*Command)
case <-ctx.Done():
}
```

- **Delivered at least once.** A signal is consumed when the transition that received it records
  its COMPLETE. A retry, a restart or a takeover before then receives it again with the same ID,
  so a handler that must act once dedupes on the ID.
- **Only a receive consumes.** A transition that never reads a name consumes none of its signals
  by completing; they wait for a transition that reads them, and are discarded when the run
  finishes. The FINISH event, as `History` returns it, lists their IDs (`discarded_signals`),
  best effort, so a consumer can record the drop.
- **Ordered by ID, best effort.** Signals accepted close together on different nodes can arrive
  out of ID order, so dedupe on the set of applied IDs, never on the highest one.
- **Refused at the door.** A finished or unknown run refuses with `ErrFsmNotFound`. A name the
  FSM did not accept, or a payload that does not decode, refuses with an error (`InvalidArgument`
  over the `Signal` RPC). The sending `Manager` must have the run's FSM registered to check them.
  A signal never ends a run; that is what `Cancel` is for.

`Send` takes a `SignalSender`, which the `Manager` is through its `SendSignal`, so a handler that
sends can be tested with a fake. A transition body that receives can be tested with `MockSignals`:

```go
req := fsm.MockRequest(fsm.NewRequest(&Req{}, &Resp{}), logger, fsm.Run{})
sigs := fsm.MockSignals(req, Advance)
defer sigs.Close()
sigs.Deliver(Advance, &Command{Reason: "looks good"})
resp, err := observe(ctx, req)
consumed := sigs.Received() // what the transition's COMPLETE would consume
```

On the object storage backend a signal sent on one node reaches the run's owner within one
heartbeat, sooner with an event bus. [docs/rfc-addendum-signals.md](docs/rfc-addendum-signals.md)
has the design.

## Serving the control API

A process that does not embed a `Manager` starts, waits on, cancels and signals runs through the
Connect-RPC service in `gen/fsm/v1/fsmv1connect`. The manager serves it on its admin unix socket
(`Config.AdminSocketPath`). To serve it on a listener of your own, mount `ServiceHandler`:

```go
path, handler := m.ServiceHandler(connect.WithInterceptors(auth))
mux := http.NewServeMux()
mux.Handle(path, handler)
```

- **fsm does no authentication.** Whoever reaches the handler can start, cancel and signal runs,
  so guard a network listener, with an interceptor or in front of it.
- **`Wait` holds its request open until the run finishes.** Give the server write and idle
  timeouts long enough for your longest run, or have clients retry `Wait`.

## Storage backends

State is persisted through a `Store` interface with two implementations, selected by `Config`
(exactly one must be set):

- **BoltDB** (`DBPath`): local on-disk persistence, the default choice for single-node
  deployments.
- **Object storage** (`ObjectStorage`): S3-compatible persistence (e.g.
  [Tigris](https://www.tigrisdata.com)) with no local disk dependency, using conditional writes
  for coordination. See `docs/rfc-object-storage-backend.md` for the design; cluster
  coordination (leases, distributed cancel, brokered queues) arrives in later phases.

```go
m, err := fsm.New(fsm.Config{
    ObjectStorage: &fsm.ObjectStorageConfig{
        Bucket:   "my-fsm-state",
        Endpoint: "https://fly.storage.tigris.dev",
        Region:   "auto",
    },
})
```

A consumer that already operates an S3 client — with its own credentials, retryer, timeouts or
middleware — passes it as `ObjectStorageConfig.Client` and the library uses it as is; `Endpoint`
and `Region` are then the client's. The client must address the bucket path-style.

A run whose persisted request no longer decodes with the registered definition cannot be
resumed. The node that fails it releases the run and leaves it for ten lease timeouts (five
minutes at the default) before trying again, while peers try on their own schedules; a restart
with a fixed definition retries at once.

### Event bus on a shared NATS hub

The library names its subjects `fsm.run.*`. On a hub whose grants are subject-scoped, give the
NATS adapter the tenant's prefix and every subject travels under it — `org.acme.fsm.run.pending`
— in both directions, so one tenant's managers never hear another's signals:

```go
bus := natsbus.New(nc, logger, natsbus.WithSubjectPrefix("org.acme"))
```

## Parent and child runs

`fsm.WithParent(version)` records a run as a child of another, so `Children` and
`ActiveChildren` list it. That is all it does:

- **Lifetimes are independent.** Canceling or finishing the parent does not reach the child. A
  parent that must not finish over a live child cancels it and waits for it.
- **Ids are not scoped to the parent.** A run's lock is its type, id and action. Two parents that
  start a child under one id contend for the same resource, and the second gets an
  `AlreadyRunningError` naming the first parent's run. A parent that adopts the run that error
  names should put its own run version in its children's ids.
- **"Start this child unless I already did" needs more than that error.** Transitions and
  finalizers run at least once, so a parent re-enters the code that starts its children.
  `AlreadyRunningError` covers only a child still running: a finished child's id can be started
  again, and a plain queued start stacks. Check `Runs(id)` before starting.

## Queues and exclusivity

`Config.Queues` gives a queue a cluster-wide capacity, and `fsm.WithQueue(name)` starts a run
under it: the run is persisted immediately and the claim loop admits it when the queue has room.

A run started with no queue holds its resource — its type, id and action — so a second start
while it is live gets an `*AlreadyRunningError` naming it. Queued runs stack behind one another
instead, so several runs of one id can be live at once.

`fsm.WithExclusiveQueue(name)` is both: the run waits for the queue's capacity and holds its
resource for its whole life.

```go
version, err := start(ctx, orgID, req, fsm.WithExclusiveQueue("tofu"))
```

- **Held from Start, not from admission.** A run still waiting for capacity already refuses a
  second start of its id.
- **Refused the same way as an unqueued run.** A second start that also takes the resource's lock
  — an unqueued start, or another exclusive one — gets an `*AlreadyRunningError` naming the live
  run, and over the RPC an `AlreadyExists` carrying its version.
- **Every start of the resource must use it,** or declare it on the FSM instead (below). A plain
  `WithQueue` start of the same id stacks beside an exclusive run, because its lock is keyed by
  run version, so a call site left on `WithQueue` silently runs a second time.
- **A run that is never admitted holds the resource** until the queue has capacity — or until
  `Cancel` settles it, which frees the id at once without the run ever executing.
- **Released when the run finishes,** so the id is free again.
- **Other ids are unaffected.** Exclusivity is per resource; the queue's capacity still governs
  how many run at once across the fleet.
- **The last option wins.** `WithQueue` after `WithExclusiveQueue` leaves the run queued and not
  exclusive, as passing the plain option alone would. An empty queue name starts the run on the
  default runner, where it holds its resource as any unqueued run does.

To hold the resource however a run is started, declare it on the FSM instead of at each call site:

```go
End("done", fsm.RunsExclusively[Req, Resp]("tofu"))
```

- **Every start path honours it,** including the `Start` RPC, whatever options the caller sends.
- **A start that says nothing about queueing adopts it.** One passing the matching
  `WithExclusiveQueue` is accepted too, so existing call sites keep working.
- **A contradicting start is refused,** not silently overridden: a plain `WithQueue`, a different
  queue, or `WithQueue("")`. Over the RPC that is `InvalidArgument`.
- **`Build` fails unless the queue is configured** in `Config.Queues`. On the object backend no
  node would admit a run on an unknown queue, and its lock, taken at Start, would hold the id
  until the queue was configured.
- **Runs started earlier keep the options they were started with.** A plain queued run still
  waiting for admission when its definition becomes exclusive is admitted as it was started:
  stacking, with no resource lock.

## Observability

### Logging

The library logs through `log/slog`. Pass a `*slog.Logger` as `Config.Logger`; the default is
`slog.Default()`. Handlers get a logger through `req.Log()`, carrying the run's attributes. Every
log call that has a context passes it to the handler, so a handler that reads trace context —
an OpenTelemetry slog bridge, say — attaches each line to the run's span.

```go
m, err := fsm.New(fsm.Config{
    DBPath: "/var/lib/myapp/fsm",
    Logger: slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
})
```

Levels: a run's start and stop, a claim, and shutdown are Info; each transition, each wait, and
each queued dispatch are Debug; a retry is Warn; a halt or a storage failure is Error.

A run's identity is one `fsm` group, keyed like its span and metric attributes so the three
signals share a vocabulary: `fsm.action`, `fsm.type`, `fsm.alias`, `fsm.id`, `fsm.version`, and
on a transition's lines `fsm.state` and `fsm.transition_version`, and `fsm.iteration` in a
repeated one. The JSON handler nests them
under `"fsm"`; the text handler dots them. Outside the group: `sys` (`fsm` or `fsm-store`),
`retry_count` on a retry's lines, and per-line keys such as `error`, `queue` and `key`.

### Metrics

Metrics are OpenTelemetry instruments. Pass a `metric.MeterProvider` as `Config.MeterProvider`;
the default is the OTel global provider, which records nothing until an SDK is installed. Traces
come from the global tracer provider.

| Instrument | Kind | Attributes |
|---|---|---|
| `fsm.run.completed` | counter | `fsm.action`, `fsm.resource`, `fsm.status` (`ok`, `canceled`, `abort`, `unrecoverable`, `fsm_handoff_error`, `error`), `fsm.error.kind` |
| `fsm.run.duration` (s) | histogram | same |
| `fsm.transition.completed` | counter | `fsm.action`, `fsm.state`, `fsm.resource`, `fsm.status` |
| `fsm.transition.duration` (s) | histogram | same |
| `fsm.object_storage.operation.duration` (s) | histogram | `fsm.storage.op`, `fsm.storage.outcome` |
| `fsm.object_storage.cas.retries` | counter | `fsm.cas.kind` |
| `fsm.lease.renewals` | counter | `fsm.lease.result` |
| `fsm.queue.depth` | gauge | `fsm.queue` |
| `fsm.queue.commits` | counter | `fsm.queue` |

A run is recorded once, when its finisher returns, by the node that finishes it. Its duration
runs from submission through its finalizers, whether it succeeded or halted. A run resumed after a
crash is counted by the node that resumes it, not the one that stopped.

A transition can run for hours, so the duration histograms carry explicit bucket advice up to
4 h for an SDK with no View. The recommended configuration is a base-2 exponential histogram,
which fits any range at a fixed cost; the library's own tests run under this View:

```go
provider := sdkmetric.NewMeterProvider(
    sdkmetric.WithReader(exporter),
    sdkmetric.WithView(sdkmetric.NewView(
        sdkmetric.Instrument{Name: "fsm.*.duration"},
        sdkmetric.Stream{Aggregation: sdkmetric.AggregationBase2ExponentialHistogram{MaxSize: 160, MaxScale: 20}},
    )),
)
m, err := fsm.New(fsm.Config{DBPath: "/var/lib/myapp/fsm", MeterProvider: provider})
```

## Testing

The `fsmtest` package runs a scenario against both backends without external services: a temp
BoltDB and an in-memory S3. Managers from one `Backend` share storage, so a restart is a stopped
manager followed by a new one over the same state:

```go
func TestDeployResumes(t *testing.T) {
    fsmtest.RunBackends(t, func(t *testing.T, b *fsmtest.Backend) {
        m1, stop1 := b.NewManager(nil)
        // register, start a run, let it block in its first transition ...
        stop1()
        m2, _ := b.NewManager(nil)
        // register again, Resume, Wait ...
    })
}
```

A takeover needs the object backend, a lease the owner cannot defend, and optionally a bus so
the peer claims on the event rather than the next scan:

```go
b := fsmtest.NewObjectBackend(t,
    fsmtest.WithBus(fake.NewBus()),
    fsmtest.WithObjectConfig(fsmtest.AsymmetricTimings(400*time.Millisecond)),
)
```

The fakes live in `fsmtest/fake` and do not import `fsm`: a test that builds its own `Config`
takes `fake.NewS3(t).Client()` for `ObjectStorageConfig.Client` and `fake.NewBus()` for
`EventBus`. The S3 fake injects faults — `Conflicts`, `LostPuts`, `SetPrePut`, `SetFailDelete`
— and counts `Puts()` and reads, for tests of the conditional-write paths.
