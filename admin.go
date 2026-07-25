package fsm

import (
	"context"
	"errors"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"
	"github.com/superfly/fsm/gen/fsm/v1/fsmv1connect"

	"connectrpc.com/connect"
	"github.com/oklog/ulid/v2"
)

var _ fsmv1connect.FSMServiceHandler = (*adminServer)(nil)

type adminServer struct {
	m *Manager
}

func (s *adminServer) ListRegistered(context.Context, *connect.Request[fsmv1.ListRegisteredRequest]) (*connect.Response[fsmv1.ListRegisteredResponse], error) {
	registered := s.m.registeredFSMs()
	fsms := make([]*fsmv1.FSM, 0, len(registered))
	for _, fsm := range registered {
		f := &fsmv1.FSM{
			Action:      fsm.action,
			TypeName:    fsm.typeName,
			Alias:       fsm.alias,
			StartState:  fsm.startState,
			EndState:    fsm.endState,
			Transitions: fsm.transitionSlice(),
		}
		fsms = append(fsms, f)
	}

	return connect.NewResponse(&fsmv1.ListRegisteredResponse{
		Fsms: fsms,
	}), nil
}

func (s *adminServer) ListActive(ctx context.Context, _ *connect.Request[fsmv1.ListActiveRequest]) (*connect.Response[fsmv1.ListActiveResponse], error) {
	states, err := s.m.store.ListActive(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	active := make([]*fsmv1.ActiveFSM, 0, len(states))
	for _, rs := range states {
		af := &fsmv1.ActiveFSM{
			Id:           rs.ID,
			Action:       rs.Action,
			Version:      rs.StartVersion.String(),
			RunState:     rs.State,
			CurrentState: rs.CurrentState,
			Queue:        rs.Queue,
		}
		if rs.TransitionVersion.Compare(ulid.ULID{}) != 0 {
			af.TransitionVersion = rs.TransitionVersion.String()
		}
		// TODO - What should we do about Error.State here?
		if rs.Error.Err != nil {
			af.Error = rs.Error.Err.Error()
		}
		active = append(active, af)
	}
	return connect.NewResponse(&fsmv1.ListActiveResponse{
		Active: active,
	}), nil
}

// Start durably submits a run from an opaque request payload and returns its version once
// persisted (persist-then-ack). A second Start for an already-active id returns CodeAlreadyExists
// carrying the existing version as a StartResponse error detail.
func (s *adminServer) Start(ctx context.Context, req *connect.Request[fsmv1.StartRequest]) (*connect.Response[fsmv1.StartResponse], error) {
	msg := req.Msg
	opts, err := startOptionsFromProto(msg.GetOptions())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	version, err := s.m.startOpaque(ctx, msg.GetTypeName(), msg.GetAction(), msg.GetId(), msg.GetResource(), opts...)
	if err != nil {
		return nil, startError(err)
	}
	return connect.NewResponse(&fsmv1.StartResponse{Version: version.String()}), nil
}

// Wait blocks until the run reaches a terminal state and returns its outcome and W result. A
// client disconnect or deadline surfaces as the transport error, not a run outcome.
func (s *adminServer) Wait(ctx context.Context, req *connect.Request[fsmv1.WaitRequest]) (*connect.Response[fsmv1.WaitResponse], error) {
	version, err := ulid.Parse(req.Msg.GetVersion())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	waitErr := s.m.Wait(ctx, version)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, connect.NewError(connect.CodeCanceled, ctxErr)
	}

	result, err := s.m.RunResult(ctx, version)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &fsmv1.WaitResponse{Result: result}
	if waitErr != nil {
		resp.Error = waitErr.Error()
	}
	return connect.NewResponse(resp), nil
}

// Cancel records a durable cancel for the run; the owning worker reacts. Canceling a terminal or
// unknown run reports CodeNotFound.
func (s *adminServer) Cancel(ctx context.Context, req *connect.Request[fsmv1.CancelRequest]) (*connect.Response[fsmv1.CancelResponse], error) {
	version, err := ulid.Parse(req.Msg.GetVersion())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	switch err := s.m.Cancel(ctx, version, req.Msg.GetCause()); {
	case errors.Is(err, ErrFsmNotFound):
		return nil, connect.NewError(connect.CodeNotFound, err)
	case err != nil:
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&fsmv1.CancelResponse{}), nil
}

// Runs lists the versions of every run recorded for a resource id, oldest first.
func (s *adminServer) Runs(ctx context.Context, req *connect.Request[fsmv1.RunsRequest]) (*connect.Response[fsmv1.RunsResponse], error) {
	runs, err := s.m.Runs(ctx, req.Msg.GetId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	versions := make([]string, len(runs))
	for i, v := range runs {
		versions[i] = v.String()
	}
	return connect.NewResponse(&fsmv1.RunsResponse{Versions: versions}), nil
}

// History returns the archived terminal record for a completed run.
func (s *adminServer) History(ctx context.Context, req *connect.Request[fsmv1.HistoryRequest]) (*connect.Response[fsmv1.HistoryEvent], error) {
	version, err := ulid.Parse(req.Msg.GetRunVersion())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	he, err := s.m.History(ctx, version)
	switch {
	case errors.Is(err, ErrFsmNotFound):
		return nil, connect.NewError(connect.CodeNotFound, err)
	case err != nil:
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&fsmv1.HistoryEvent{
		ActiveEvent: he.GetActiveEvent(),
		LastEvent:   he.GetLastEvent(),
	}), nil
}

// startError maps an opaque Start failure to a Connect code. An already-running id is not a
// failure the client must fix — it carries the existing version so the caller can wait on it.
func startError(err error) error {
	if are, ok := errors.AsType[*AlreadyRunningError](err); ok {
		connErr := connect.NewError(connect.CodeAlreadyExists, err)
		if detail, derr := connect.NewErrorDetail(&fsmv1.StartResponse{Version: are.Version.String()}); derr == nil {
			connErr.AddDetail(detail)
		}
		return connErr
	}
	switch {
	case errors.Is(err, errFSMNotRegistered):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, errAmbiguousAction), errors.Is(err, errInvalidResource):
		return connect.NewError(connect.CodeInvalidArgument, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

// startOptionsFromProto translates the wire start options into StartOptionsFn. An unset field is
// omitted; a malformed run_after or parent version is a client error.
func startOptionsFromProto(o *fsmv1.StartOptions) ([]StartOptionsFn, error) {
	if o == nil {
		return nil, nil
	}

	var opts []StartOptionsFn
	if o.GetDelayUntil() > 0 {
		opts = append(opts, WithDelayedStart(time.UnixMilli(o.GetDelayUntil())))
	}
	if v := o.GetRunAfter(); v != "" {
		version, err := ulid.Parse(v)
		if err != nil {
			return nil, errors.New("invalid run_after version")
		}
		opts = append(opts, WithRunAfter(version))
	}
	if q := o.GetQueue(); q != "" {
		opts = append(opts, WithQueue(q))
	}
	if p := o.GetParent(); p != "" {
		version, err := ulid.Parse(p)
		if err != nil {
			return nil, errors.New("invalid parent version")
		}
		opts = append(opts, WithParent(version))
	}
	return opts, nil
}
