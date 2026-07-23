package fsm

import (
	"context"
)

func setStarted[R, W any](s Store) func(context.Context, *Request[R, W]) context.Context {
	return func(ctx context.Context, req *Request[R, W]) context.Context {
		if err := s.SetRunning(req.Run()); err != nil {
			req.Log().WithError(err).Error("failed to update fsm state store")
		}
		return ctx
	}
}
