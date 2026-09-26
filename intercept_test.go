package fsm

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// calls records which interceptor saw which transition, in order.
type calls struct {
	mu   sync.Mutex
	seen []string
}

// tag is an interceptor recording tag:state for every call it sees.
func (c *calls) tag(tag string) TransitionInterceptorFunc {
	return func(next TransitionFunc) TransitionFunc {
		return func(ctx context.Context, req AnyRequest) (AnyResponse, error) {
			c.mu.Lock()
			c.seen = append(c.seen, tag+":"+req.Run().CurrentState)
			c.mu.Unlock()
			return next(ctx, req)
		}
	}
}

func (c *calls) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.seen)
}

// TestInterceptAll verifies interceptors passed to End wrap every Start and To transition, once
// per attempt and outside the transition's own interceptors, in the order given; that the
// finisher and a RepeatDone reach none of them; and that End's other options still apply.
func TestInterceptAll(t *testing.T) { runBackends(t, testInterceptAll) }

func testInterceptAll(t *testing.T, b *backend) {
	ctx := context.Background()
	m, _ := b.newManager(nil)

	var (
		c         calls
		failed    atomic.Bool
		finalized atomic.Bool
	)
	flaky := func(context.Context, *Request[orderReq, orderResp]) (*Response[orderResp], error) {
		if failed.CompareAndSwap(false, true) {
			return nil, errors.New("flaky")
		}
		return nil, nil
	}
	start, _, err := m.Register[orderReq, orderResp]("intercept-all").
		Start("first", okTransition).
		To("stage", okTransition, RepeatWhile(fewerThan(1))).
		To("flaky", flaky).
		To("last", okTransition, WithInterceptors[orderReq, orderResp](c.tag("own"))).
		End("done",
			InterceptAll[orderReq, orderResp](c.tag("a")),
			InterceptAll[orderReq, orderResp](c.tag("b")),
			WithFinalizers(func(context.Context, *Request[orderReq, orderResp], RunErr) {
				finalized.Store(true)
			}),
		).
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	version, err := start(waitCtx, "intercept-all-1", NewRequest(&orderReq{}, &orderResp{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	if err := m.Wait(waitCtx, version); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	want := []string{
		"a:first", "b:first",
		"a:stage", "b:stage",
		"a:flaky", "b:flaky",
		"a:flaky", "b:flaky",
		"a:last", "b:last", "own:last",
	}
	if got := c.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("unexpected interceptor calls:\n got %v\nwant %v", got, want)
	}
	if !finalized.Load() {
		t.Fatal("expected End's finalizer to run beside InterceptAll")
	}
}
