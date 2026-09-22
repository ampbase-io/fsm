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
func newTestBus(t *testing.T, opts ...Option) (*nats.Conn, *Bus) {
	t.Helper()

	serverOpts := natsserver.DefaultTestOptions
	serverOpts.Port = -1 // an ephemeral port, so parallel tests never collide
	srv := natsserver.RunServer(&serverOpts)
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("failed to connect to test NATS: %v", err)
	}
	t.Cleanup(nc.Close)

	return nc, New(nc, nil, opts...)
}

// TestSubjectPrefix proves a prefixed bus moves the core's subjects under the prefix on the wire
// in both directions — what a tenant's grant on a shared hub sees — while the core keeps naming
// its subjects as before.
func TestSubjectPrefix(t *testing.T) {
	nc, bus := newTestBus(t, WithSubjectPrefix("org.acme"))
	const core = "fsm.run.pending"
	const wire = "org.acme.fsm.run.pending"

	// Outbound: the core publishes on its subject; a raw subscriber sees it under the prefix.
	raw, err := nc.SubscribeSync(wire)
	if err != nil {
		t.Fatalf("raw subscribe: %v", err)
	}
	defer raw.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	bus.Publish(core, &fsmv1.RunEvent{Kind: fsmv1.RunEventKind_RUN_EVENT_KIND_PENDING})
	if _, err := raw.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("expected the publish under the prefix on the wire, got %v", err)
	}

	// Inbound: the core subscribes on its subject; a raw publish under the prefix reaches it.
	got := make(chan *fsmv1.RunEvent, 1)
	unsub, err := bus.Subscribe(core, func(e *fsmv1.RunEvent) { got <- e })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer unsub()
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	data, err := proto.Marshal(&fsmv1.RunEvent{Kind: fsmv1.RunEventKind_RUN_EVENT_KIND_CANCEL})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := nc.Publish(wire, data); err != nil {
		t.Fatalf("raw publish: %v", err)
	}
	select {
	case e := <-got:
		if e.GetKind() != fsmv1.RunEventKind_RUN_EVENT_KIND_CANCEL {
			t.Fatalf("expected the raw publish delivered under the core subject, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event delivered within 2s")
	}

	// And nothing leaks across: the unprefixed subject on the wire is not this bus's.
	if err := nc.Publish(core, data); err != nil {
		t.Fatalf("raw publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	select {
	case e := <-got:
		t.Fatalf("an unprefixed publish reached the prefixed bus: %v", e)
	case <-time.After(200 * time.Millisecond):
	}
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
