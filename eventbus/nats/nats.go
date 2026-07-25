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
	"io"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	fsm "github.com/superfly/fsm"
	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"
)

// Bus adapts a NATS connection to fsm.EventBus: RunEvents are protobuf-marshaled onto NATS
// subjects, and each delivery is decoded and dispatched to the subscriber off the NATS delivery
// goroutine.
type Bus struct {
	nc     *nats.Conn
	logger logrus.FieldLogger
}

// The adapter must stay assignable to the interface the core injects.
var _ fsm.EventBus = (*Bus)(nil)

// deliveryBuffer bounds the per-subscription hand-off queue between the NATS delivery goroutine
// and the goroutine that runs the subscriber callback. A subscriber slower than the event rate
// overflows it and drops events — best-effort per the EventBus contract, never blocking the bus.
const deliveryBuffer = 128

// New wraps an established NATS connection as an fsm.EventBus. A nil logger discards.
func New(nc *nats.Conn, logger logrus.FieldLogger) *Bus {
	if logger == nil {
		discard := logrus.New()
		discard.SetOutput(io.Discard)
		logger = discard
	}
	return &Bus{nc: nc, logger: logger}
}

// Publish marshals the event and hands it to NATS, which buffers it in the client — the call does
// not block on the network, satisfying the "MUST NOT block the caller" contract. A marshal or
// publish failure is logged and dropped; the durable log remains the source of truth.
func (b *Bus) Publish(subject string, event *fsmv1.RunEvent) {
	data, err := proto.Marshal(event)
	if err != nil {
		b.logger.WithError(err).WithField("subject", subject).Warn("failed to marshal run event, dropping")
		return
	}
	if err := b.nc.Publish(subject, data); err != nil {
		b.logger.WithError(err).WithField("subject", subject).Warn("failed to publish run event")
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
func (b *Bus) Subscribe(subject string, fn func(*fsmv1.RunEvent)) (func(), error) {
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
			b.logger.WithError(err).WithField("subject", subject).Warn("failed to unmarshal run event, dropping")
			return
		}
		select {
		case events <- &event:
		default:
			b.logger.WithField("subject", subject).Warn("subscriber too slow, dropping run event")
		}
	})
	if err != nil {
		close(done)
		return nil, err
	}

	return sync.OnceFunc(func() {
		if err := sub.Unsubscribe(); err != nil {
			b.logger.WithError(err).WithField("subject", subject).Warn("failed to unsubscribe")
		}
		close(done)
	}), nil
}
