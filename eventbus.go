package fsm

import (
	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

// The bus payload is the protobuf fsmv1.RunEvent, so the live stream and the durable events/ log
// speak one schema and transition events reuse the durable EventType. See its definition in
// proto/fsm/v1/event.proto.

// EventPublisher delivers a RunEvent on a subject AFTER it is durably persisted. Best-effort:
// implementations MUST NOT block the caller, and no correctness property depends on delivery —
// the object-storage log and sentinel objects remain the source of truth.
type EventPublisher interface {
	Publish(subject string, event *fsmv1.RunEvent)
}

// EventSubscriber registers interest in a subject, invoking fn for each matching event until the
// returned unsubscribe function is called. Best-effort: a subscription that never fires costs
// latency (the polling floors carry everything), never correctness. Implementations MUST
// dispatch fn without blocking their own delivery path.
type EventSubscriber interface {
	Subscribe(subject string, fn func(*fsmv1.RunEvent)) (unsubscribe func(), err error)
}

// EventBus is the injection point: a transport carrying both directions. Internal components
// depend on the narrow half they use. The default is a no-op bus; a live bus (e.g. a pub/sub
// broker) is supplied via Config, and the library never imports one.
type EventBus interface {
	EventPublisher
	EventSubscriber
}

// Subject addressing is by run, never by node, so no component ever needs another node's
// network address.
const subjectPending = "fsm.run.pending" // broadcast wakeup: a run became claimable

// doneSubject addresses the completion of a single run; a waiter subscribes to it to be
// released the moment the run's finish cleanup lands.
func doneSubject(version ulid.ULID) string { return "fsm.run.done." + version.String() }

// eventSubject addresses the per-transition stream of a single run, for live progress and
// audit consumers. Nothing internal depends on it; it is an accelerator over the event log.
func eventSubject(version ulid.ULID) string { return "fsm.run.event." + version.String() }

// noopBus is the default when no EventBus is injected: publishes drop and subscriptions never
// fire, so an object-storage-only deployment behaves exactly as the base RFC describes.
type noopBus struct{}

func (noopBus) Publish(string, *fsmv1.RunEvent) {}

func (noopBus) Subscribe(string, func(*fsmv1.RunEvent)) (func(), error) { return func() {}, nil }

// busOrNoop substitutes the no-op bus when none was injected, so publishers and subscribers
// never nil-check.
func busOrNoop(b EventBus) EventBus {
	if b == nil {
		return noopBus{}
	}
	return b
}

// busIsLive reports whether a real bus was injected, letting the poll floors relax when the
// stream will carry the fast path.
func busIsLive(b EventBus) bool {
	_, noop := b.(noopBus)
	return !noop
}

// subscribeSignal turns a subscription into a coalescing wakeup channel: the callback does a
// non-blocking send into a size-1 buffer, so a burst of events collapses to a single pending
// wakeup and never blocks the bus. A failed subscription logs and returns a no-op unsubscribe,
// leaving the caller on its polling floor. The internal fast paths — WaitRun's done wait, the
// claim wakeup, the cancel sweep — consume it and ignore the payload, routing on the subject.
func subscribeSignal(bus EventSubscriber, subject string, logger logrus.FieldLogger) (<-chan struct{}, func()) {
	signal := make(chan struct{}, 1)
	unsubscribe, err := bus.Subscribe(subject, func(*fsmv1.RunEvent) {
		select {
		case signal <- struct{}{}:
		default:
		}
	})
	if err != nil {
		logger.WithError(err).WithField("subject", subject).Warn("event subscribe failed, relying on the polling floor")
		return signal, func() {}
	}
	return signal, unsubscribe
}
