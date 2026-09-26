package fsm

import (
	"context"
	"log/slog"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"google.golang.org/protobuf/proto"
)

func okTransition(ctx context.Context, req *Request[orderReq, orderResp]) (*Response[orderResp], error) {
	return nil, nil
}

func TestBuilderDuplicateTransition(t *testing.T) {
	m := newTestManager(t)

	_, _, err := m.Register[orderReq, orderResp]("dup-transition").
		Start("created", okTransition).
		To("created", okTransition).
		End("done").
		Build(context.Background())
	if err == nil {
		t.Fatal("expected build error for duplicate transition name")
	}
}

func TestBuilderDuplicateEndState(t *testing.T) {
	m := newTestManager(t)

	_, _, err := m.Register[orderReq, orderResp]("dup-end").
		Start("created", okTransition).
		End("created").
		Build(context.Background())
	if err == nil {
		t.Fatal("expected build error for end state clashing with a transition name")
	}
}

func TestBuilderDuplicateFSM(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	_, _, err := m.Register[orderReq, orderResp]("dup-fsm").
		Start("created", okTransition).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("first registration failed: %v", err)
	}

	_, _, err = m.Register[orderReq, orderResp]("dup-fsm").
		Start("created", okTransition).
		End("done").
		Build(ctx)
	if err == nil {
		t.Fatal("expected build error for duplicate FSM registration")
	}
}

type unencodableReq struct {
	Ch chan int
}

func TestBuilderUnencodableRequest(t *testing.T) {
	m := newTestManager(t)

	_, _, err := m.Register[unencodableReq, orderResp]("bad-codec").
		Start("created", func(ctx context.Context, req *Request[unencodableReq, orderResp]) (*Response[orderResp], error) {
			return nil, nil
		}).
		End("done").
		Build(context.Background())
	if err == nil {
		t.Fatal("expected build error for request type without a usable codec")
	}
}

type staticCodec struct{}

func (staticCodec) Marshal(any) ([]byte, error) { return []byte("static"), nil }

func (staticCodec) Unmarshal([]byte, any) error { return nil }

func TestDetermineCodec(t *testing.T) {
	logger := slog.Default()

	c, err := determineCodec(logger, struct{ A string }{})
	if err != nil {
		t.Fatalf("expected json codec for plain struct, got error: %v", err)
	}
	if _, ok := c.(*jsonCodec); !ok {
		t.Fatalf("expected *jsonCodec, got %T", c)
	}

	c, err = determineCodec(logger, staticCodec{})
	if err != nil {
		t.Fatalf("expected provided codec to be used, got error: %v", err)
	}
	if _, ok := c.(staticCodec); !ok {
		t.Fatalf("expected staticCodec, got %T", c)
	}

	if _, err := determineCodec(logger, make(chan int)); err == nil {
		t.Fatal("expected error for unencodable type")
	}
}

// ptrCodecReq carries its Codec on the pointer, as a type wrapping generated code would.
type ptrCodecReq struct{ Name string }

func (*ptrCodecReq) Marshal(any) ([]byte, error) { return []byte("ptr"), nil }

func (*ptrCodecReq) Unmarshal([]byte, any) error { return nil }

// TestRegisterProbesCodecsThroughThePointer verifies a generated protobuf message registered as
// the value — so Request.Msg is the generated pointer, with no wrapper — gets the proto codec
// and round-trips on the proto wire, and that a Codec with pointer receivers is found the same
// way. proto.Message and such codecs live on *T; probing the value found neither.
func TestRegisterProbesCodecsThroughThePointer(t *testing.T) {
	runBackends(t, testRegisterProbesCodecsThroughThePointer)
}

func testRegisterProbesCodecsThroughThePointer(t *testing.T, b *backend) {
	m, _ := b.newManager(nil)
	ctx := context.Background()

	reg := m.Register[fsmv1.StateEvent, fsmv1.StateEvent]("proto-value")
	if _, ok := reg.f.rCodec.(*protoBinaryCodec); !ok {
		t.Fatalf("expected the proto codec for a message registered by value, got %T", reg.f.rCodec)
	}
	if _, ok := reg.f.wCodec.(*protoBinaryCodec); !ok {
		t.Fatalf("expected the proto codec for a response registered by value, got %T", reg.f.wCodec)
	}
	start, _, err := reg.
		Start("created", func(ctx context.Context, req *Request[fsmv1.StateEvent, fsmv1.StateEvent]) (*Response[fsmv1.StateEvent], error) {
			return NewResponse(&fsmv1.StateEvent{Id: "echo:" + req.Msg.GetId()}), nil
		}).
		End("done").
		Build(ctx)
	if err != nil {
		t.Fatalf("failed to build FSM: %v", err)
	}
	version, err := start(ctx, "proto-1", NewRequest(&fsmv1.StateEvent{Id: "in"}, &fsmv1.StateEvent{}))
	if err != nil {
		t.Fatalf("failed to start FSM: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m.Wait(waitCtx, version); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	result, err := m.RunResult(ctx, version)
	if err != nil {
		t.Fatalf("RunResult failed: %v", err)
	}
	var got fsmv1.StateEvent
	if err := proto.Unmarshal(result, &got); err != nil || got.GetId() != "echo:in" {
		t.Fatalf("expected the result on the proto wire, got %q (err=%v)", result, err)
	}

	if c := m.Register[ptrCodecReq, orderResp]("ptr-codec").f.rCodec; !isPtrCodec(c) {
		t.Fatalf("expected a pointer-receiver Codec found, got %T", c)
	}
}

func isPtrCodec(c Codec) bool {
	_, ok := c.(*ptrCodecReq)
	return ok
}
