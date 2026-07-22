package fsm

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
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
	logger := logrus.New()

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
