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

## Cancellation

A transition's context ends for one of three reasons, and `context.Cause(ctx)` names which:

| Cause | Meaning | What the handler should do |
|---|---|---|
| `*fsm.CancelError` | `Manager.Cancel` was called; `Reason` carries its cause. | Stop. The run halts, skips its remaining transitions, and runs its finalizers. |
| `fsm.ErrShutdown` | This `Manager` is shutting down. | Return promptly and record nothing; the run resumes on the next start or claim. |
| `fsm.ErrLeaseLost` | Another node now owns the run. | Return promptly and record nothing; the new owner is already running it. |

```go
<-ctx.Done()
if cancel, ok := errors.AsType[*fsm.CancelError](context.Cause(ctx)); ok {
    // An operator stopped the run: cancel.Reason says why.
}
return nil, ctx.Err()
```

Finalizers run on a context that an operator's cancel does not end, so they can do the work the
cancel calls for — start a compensating run and wait on it, say. It carries no deadline: the
run's lease is held until its finalizers return, so that wait may take minutes. The context still
ends on `ErrShutdown` and `ErrLeaseLost`, and a finalizer runs again if the run is resumed before
it finished.

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
