// Package natsbus provides a NATS-backed fsm.EventBus adapter. It wraps a *nats.Conn the consumer
// already operates — the core fsm module never imports NATS, so this adapter lives in its own
// nested module and object-only/Bolt deployments never pull a broker into their dependency graph.
//
// The package is deliberately named natsbus, not nats: a consumer must also import
// github.com/nats-io/nats.go (package nats) to build the connection it passes to New, so a
// matching package name here would force an alias at every callsite.
//
// It is the "live stream" tier of the fsm event transport: a best-effort accelerator over the
// durable object-storage event log. A down or partitioned NATS costs latency only — the polling
// floors carry every correctness property.
package natsbus

import (
	"log/slog"
	"sync"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	fsm "github.com/ampbase-io/fsm"
	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"
)

// Bus adapts a NATS connection to fsm.EventBus: RunEvents are protobuf-marshaled onto NATS
// subjects, and each delivery is decoded and dispatched to the subscriber off the NATS delivery
// goroutine.
type Bus struct {
	nc     *nats.Conn
	logger *slog.Logger

	// prefix, when set, is prepended to every subject on the wire: the core's fsm.run.pending
	// travels as <prefix>.fsm.run.pending. It scopes one Manager's traffic to a tenant on a shared
	// hub whose grants are subject-based, and keeps tenants from hearing each other's signals.
	prefix string
}

// Option configures a Bus.
type Option func(*Bus)

// WithSubjectPrefix scopes every subject the bus publishes and subscribes to under prefix, so a
// connection whose grant is "<prefix>.>" can carry the core's fixed fsm.run.* subjects. The
// prefix is given without a trailing dot; an empty prefix leaves the subjects as the core names
// them.
func WithSubjectPrefix(prefix string) Option {
	return func(b *Bus) {
		b.prefix = prefix
	}
}

// subject returns the wire subject for one the core names.
func (b *Bus) subject(core string) string {
	if b.prefix == "" {
		return core
	}
	return b.prefix + "." + core
}

// The adapter must stay assignable to the interface the core injects.
var _ fsm.EventBus = (*Bus)(nil)

// deliveryBuffer bounds the per-subscription hand-off queue between the NATS delivery goroutine
// and the goroutine that runs the subscriber callback. A subscriber slower than the event rate
// overflows it and drops events — best-effort per the EventBus contract, never blocking the bus.
const deliveryBuffer = 128

// New wraps an established NATS connection as an fsm.EventBus. A nil logger discards.
func New(nc *nats.Conn, logger *slog.Logger, opts ...Option) *Bus {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	b := &Bus{nc: nc, logger: logger}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Publish marshals the event and hands it to NATS, which buffers it in the client — the call does
// not block on the network, satisfying the "MUST NOT block the caller" contract. A marshal or
// publish failure is logged and dropped; the durable log remains the source of truth.
func (b *Bus) Publish(core string, event *fsmv1.RunEvent) {
	subject := b.subject(core)
	data, err := proto.Marshal(event)
	if err != nil {
		b.logger.Warn("failed to marshal run event, dropping", "error", err, "subject", subject)
		return
	}
	if err := b.nc.Publish(subject, data); err != nil {
		b.logger.Warn("failed to publish run event", "error", err, "subject", subject)
	}
}

// Subscribe delivers each event published on subject to fn on a dedicated per-subscription
// goroutine, so a slow fn never blocks NATS's delivery path — an overwhelmed subscriber drops
// events rather than stalling the bus. The NATS callback only decodes and enqueues.
//
// The returned unsubscribe stops new deliveries and tears the goroutine down; it is idempotent.
// It does not join an in-flight fn, so fn may run once more for an already-buffered event after
// unsubscribe returns — best-effort delivery makes that harmless, but a caller must not assume fn
// has stopped the instant unsubscribe returns.
func (b *Bus) Subscribe(core string, fn func(*fsmv1.RunEvent)) (func(), error) {
	subject := b.subject(core)
	events := make(chan *fsmv1.RunEvent, deliveryBuffer)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			case event := <-events:
				fn(event)
			}
		}
	}()

	sub, err := b.nc.Subscribe(subject, func(msg *nats.Msg) {
		var event fsmv1.RunEvent
		if err := proto.Unmarshal(msg.Data, &event); err != nil {
			b.logger.Warn("failed to unmarshal run event, dropping", "error", err, "subject", subject)
			return
		}
		select {
		case events <- &event:
		default:
			b.logger.Warn("subscriber too slow, dropping run event", "subject", subject)
		}
	})
	if err != nil {
		close(done)
		return nil, err
	}

	return sync.OnceFunc(func() {
		if err := sub.Unsubscribe(); err != nil {
			b.logger.Warn("failed to unsubscribe", "error", err, "subject", subject)
		}
		close(done)
	}), nil
}
