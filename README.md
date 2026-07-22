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
