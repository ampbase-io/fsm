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
