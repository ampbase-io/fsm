package natsbus

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"
)

// newTestBus boots an in-process NATS server and returns a connection to it alongside a Bus
// wrapping that same connection, so a test can Flush the connection to fence subscription setup
// against the publishes it drives.
func newTestBus(t *testing.T) (*nats.Conn, *Bus) {
	t.Helper()

	opts := natsserver.DefaultTestOptions
	opts.Port = -1 // an ephemeral port, so parallel tests never collide
	srv := natsserver.RunServer(&opts)
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("failed to connect to test NATS: %v", err)
	}
	t.Cleanup(nc.Close)

	return nc, New(nc, nil)
}

// TestPublishSubscribeRoundTrip proves an event survives the marshal → NATS → unmarshal path
// intact, exercising the exact subjects the core publishes on.
func TestPublishSubscribeRoundTrip(t *testing.T) {
	nc, bus := newTestBus(t)
	const subject = "fsm.run.event.01ARZ3NDEKTSV4RRFFQ69G5FAV"

	got := make(chan *fsmv1.RunEvent, 1)
	unsub, err := bus.Subscribe(subject, func(e *fsmv1.RunEvent) { got <- e })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer unsub()
	// Fence the subscription registration against the publish below.
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	sent := &fsmv1.RunEvent{
		Kind:       fsmv1.RunEventKind_RUN_EVENT_KIND_TRANSITION,
		RunVersion: []byte("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
		Type:       fsmv1.EventType_EVENT_TYPE_COMPLETE,
		State:      "created",
	}
	bus.Publish(subject, sent)

	select {
	case e := <-got:
		if !proto.Equal(e, sent) {
			t.Fatalf("round-tripped event mismatch:\n got %v\nwant %v", e, sent)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event delivered within 2s")
	}
}

// TestSubscribeSignalCoalescing mirrors the core's subscribeSignal wrapper — a size-1 buffer fed
// by a non-blocking send — end to end against a real NATS: a publish burst must not block the bus
// and must coalesce to an available wakeup, and a later publish must re-arm it (delivery is
// ongoing, not a one-shot). This is the pattern WaitRun, claim wakeup, and the cancel sweep all
// consume.
func TestSubscribeSignalCoalescing(t *testing.T) {
	nc, bus := newTestBus(t)
	const subject = "fsm.run.pending"

	signal := make(chan struct{}, 1)
	unsub, err := bus.Subscribe(subject, func(*fsmv1.RunEvent) {
		select {
		case signal <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer unsub()
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	pending := &fsmv1.RunEvent{Kind: fsmv1.RunEventKind_RUN_EVENT_KIND_PENDING}
	for range 50 {
		bus.Publish(subject, pending)
	}
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("a publish burst produced no wakeup")
	}

	// Draining the signal and publishing again must wake it once more.
	bus.Publish(subject, pending)
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("a publish after draining the signal never re-armed the wakeup")
	}
}

// TestUnsubscribeStopsDelivery proves unsubscribe halts delivery and is safe to call twice.
func TestUnsubscribeStopsDelivery(t *testing.T) {
	nc, bus := newTestBus(t)
	const subject = "fsm.run.cancel"

	fired := make(chan struct{}, 1)
	unsub, err := bus.Subscribe(subject, func(*fsmv1.RunEvent) { fired <- struct{}{} })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	unsub()
	unsub() // idempotent: a second call must not panic
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	bus.Publish(subject, &fsmv1.RunEvent{Kind: fsmv1.RunEventKind_RUN_EVENT_KIND_CANCEL})
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	select {
	case <-fired:
		t.Fatal("an event was delivered after unsubscribe")
	case <-time.After(200 * time.Millisecond):
		// no delivery, as expected
	}
}
