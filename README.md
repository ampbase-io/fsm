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

Request/response types are persisted with a protobuf codec when they implement `proto.Message`,
with a custom codec when they implement `fsm.Codec`, and with JSON otherwise.

### Definition drift

A run records its transition list at start. If the definition changes while the run is in
flight and a resuming node's definition lacks a transition the run still has to make, the
default is lenient: that transition is recorded complete and skipped, with a warning. A
definition registered with `StrictResume()` refuses such a run instead:

```go
start, resume, err := m.Register[CreateReq, CreateResp]("create").StrictResume().
    Start("created", ...).
    End("done").
    Build(ctx)
```

On the object storage backend the refusal happens before the lease is taken, so the run stays
unowned for a node whose definition has the transition; the refusing node logs a warning and
counts `fsm.resume.refused`. On BoltDB `resume` returns `ErrUnknownTransition`. Only transitions
still to run are checked, so a step every in-flight run has already completed can be dropped
under either mode.

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

Finalizers run on a context that an operator's cancel does not end, so they can do the work the
cancel calls for — start a compensating run and wait on it, say. It carries the values
initializers put on the transitions' context but none of their cancellation, and no deadline:
the run's lease is held until its finalizers return, so that wait may take minutes. The context
still ends on `ErrShutdown` and `ErrLeaseLost`, and a finalizer runs again if the run is resumed
before it finished.

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
  again, and queued starts stack. Check `Runs(id)` before starting.

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
on a transition's lines `fsm.state` and `fsm.transition_version`. The JSON handler nests them
under `"fsm"`; the text handler dots them. Outside the group: `sys` (`fsm` or `fsm-store`),
`retry_count` on a retry's lines, and per-line keys such as `error`, `queue` and `key`.

### Metrics

Metrics are OpenTelemetry instruments. Pass a `metric.MeterProvider` as `Config.MeterProvider`;
the default is the OTel global provider, which records nothing until an SDK is installed. Traces
come from the global tracer provider.

| Instrument | Kind | Attributes |
|---|---|---|
| `fsm.run.completed` | counter | `fsm.action`, `fsm.resource`, `fsm.status`, `fsm.error.kind` |
| `fsm.run.duration` (s) | histogram | same |
| `fsm.transition.completed` | counter | `fsm.action`, `fsm.state`, `fsm.resource`, `fsm.status` |
| `fsm.transition.duration` (s) | histogram | same |
| `fsm.object_storage.operation.duration` (s) | histogram | `fsm.storage.op`, `fsm.storage.outcome` |
| `fsm.object_storage.cas.retries` | counter | `fsm.cas.kind` |
| `fsm.lease.renewals` | counter | `fsm.lease.result` |
| `fsm.queue.depth` | gauge | `fsm.queue` |
| `fsm.queue.commits` | counter | `fsm.queue` |
| `fsm.resume.refused` | counter | `fsm.action`, `fsm.state` |

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
