package fsm

import (
	"fmt"
	"log/slog"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// MockMailbox delivers signals to a request built by MockRequest, so a transition body that reads
// signals can be unit-tested outside a run. It is the mailbox a run gets, with the same delivery:
// a signal counts as received only when the body's receive completes.
type MockMailbox struct {
	mb *mailbox
}

// MockSignals gives req a mailbox that accepts sigs, as WithSignals does for an FSM's runs. It
// panics on an invalid declaration, as a build would refuse it. Close the mailbox when the test
// ends.
func MockSignals(req AnyRequest, sigs ...AnySignal) *MockMailbox {
	if len(sigs) == 0 {
		panic("fsm: MockSignals needs at least one signal")
	}
	accepted, err := acceptedSignals(sigs)
	if err != nil {
		panic(fmt.Sprintf("fsm: MockSignals: %v", err))
	}
	mb := newMailbox(accepted, nil, slog.New(slog.DiscardHandler))
	req.withMailbox(mb)
	return &MockMailbox{mb: mb}
}

// Deliver offers the signal to the request's body and returns its ID. It panics if msg does not
// encode with the signal's codec.
func (m *MockMailbox) Deliver[T any](s Signal[T], msg *T) ulid.ULID {
	if s.err != nil {
		panic(fmt.Sprintf("fsm: Deliver: %v", s.err))
	}
	payload, err := s.codec.Marshal(msg)
	if err != nil {
		panic(fmt.Sprintf("fsm: Deliver %s: %v", s.name, err))
	}
	id := ulid.Make()
	m.mb.offer(&fsmv1.Signal{Id: id.String(), Name: s.name, Payload: payload})
	return id
}

// Received returns the IDs the body has received, in order: what the transition's COMPLETE would
// record as consumed.
func (m *MockMailbox) Received() []ulid.ULID {
	received := m.mb.received()
	ids := make([]ulid.ULID, 0, len(received))
	for _, id := range received {
		ids = append(ids, ulid.MustParse(id))
	}
	return ids
}

// Close stops the mailbox's delivery goroutine.
func (m *MockMailbox) Close() {
	m.mb.close()
}
