package fake

import (
	"sync"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"
)

// Bus is an in-process event bus satisfying fsm.EventBus. A publish fans out to the subject's
// subscribers asynchronously — a slow subscriber never blocks the publisher, per the contract —
// and every publish is recorded for assertions. Injecting one makes the library's fast paths
// live: cross-node Wait wakeups, claim wakeups, and cancel broadcasts.
type Bus struct {
	mu        sync.Mutex
	subs      map[string]map[int]func(*fsmv1.RunEvent)
	nextID    int
	published []Event
}

// Event is one publish the bus saw.
type Event struct {
	Subject string
	Event   *fsmv1.RunEvent
}

// NewBus returns an empty bus with no subscribers.
func NewBus() *Bus {
	return &Bus{subs: map[string]map[int]func(*fsmv1.RunEvent){}}
}

// Publish records the event and delivers it to the subject's current subscribers, each on its
// own goroutine.
func (b *Bus) Publish(subject string, e *fsmv1.RunEvent) {
	b.mu.Lock()
	b.published = append(b.published, Event{Subject: subject, Event: e})
	fns := make([]func(*fsmv1.RunEvent), 0, len(b.subs[subject]))
	for _, fn := range b.subs[subject] {
		fns = append(fns, fn)
	}
	b.mu.Unlock()

	for _, fn := range fns {
		go fn(e)
	}
}

// Subscribe registers fn for the subject's events and returns the function that removes it.
// It never fails; the error is the fsm.EventSubscriber contract's.
func (b *Bus) Subscribe(subject string, fn func(*fsmv1.RunEvent)) (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subs[subject] == nil {
		b.subs[subject] = map[int]func(*fsmv1.RunEvent){}
	}
	id := b.nextID
	b.nextID++
	b.subs[subject][id] = fn

	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.subs[subject], id)
	}, nil
}

// SubscriberCount reports how many live subscriptions a subject has, so a test can wait for a
// subscriber to register before publishing rather than racing it with a sleep.
func (b *Bus) SubscriberCount(subject string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs[subject])
}

// Published returns every publish the bus has seen, in order.
func (b *Bus) Published() []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Event(nil), b.published...)
}
