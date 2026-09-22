package fsm

import (
	"context"
)

func setStarted[R, W any](s Store) func(context.Context, *Request[R, W]) context.Context {
	return func(ctx context.Context, req *Request[R, W]) context.Context {
		if err := s.SetRunning(ctx, req.Run()); err != nil {
			req.Log().ErrorContext(ctx, "failed to update fsm state store", "error", err)
		}
		return ctx
	}
}
