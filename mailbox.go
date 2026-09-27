package fsm

import (
	"context"
	"log/slog"
	"reflect"
	"slices"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// mailbox delivers one executing run's signals to its transitions. It exists only for a run whose
// FSM accepts signals. One goroutine owns its state and offers, on every accepted name's
// unbuffered channel at once, the oldest signal of that name not yet received in this attempt,
// so a signal counts as received only when a handler's receive completes. Every other method
// hands that goroutine a function to run, so the state needs no lock.
type mailbox struct {
	outlets map[string]delivery
	ctrl    chan func(*mailboxState)
	done    chan struct{}
	logger  *slog.Logger
}

// mailboxState is what the delivery goroutine owns.
type mailboxState struct {
	// pending holds every offered signal not yet consumed, by ID.
	pending map[string]*fsmv1.Signal

	// consumed holds the IDs a COMPLETE has recorded consumed, so a marker whose delete failed
	// is never offered again.
	consumed map[string]struct{}

	// received holds the IDs handlers received during the current attempt, in order.
	received []string

	// decoded caches each pending signal as its receiver's value.
	decoded map[string]reflect.Value
}

// delivery is one accepted signal's channel for one run.
type delivery interface {
	channel() reflect.Value
	// value is the signal as its receiver's Received value.
	value(sig *fsmv1.Signal) (reflect.Value, error)
}

type outlet[T any] struct {
	codec Codec
	ch    chan Received[T]
}

func (o *outlet[T]) channel() reflect.Value { return reflect.ValueOf(o.ch) }

func (o *outlet[T]) value(sig *fsmv1.Signal) (reflect.Value, error) {
	id, err := ulid.Parse(sig.GetId())
	if err != nil {
		return reflect.Value{}, err
	}
	var msg T
	if err := o.codec.Unmarshal(sig.GetPayload(), &msg); err != nil {
		return reflect.Value{}, err
	}
	return reflect.ValueOf(Received[T]{ID: id, SentAt: ulid.Time(id.Time()), Msg: &msg}), nil
}

// newMailbox starts the delivery goroutine for a run whose FSM accepts signals, seeded with the
// IDs the run has already consumed. It returns nil, and a no-op close, for one that accepts none.
func newMailbox(accepted map[string]AnySignal, consumed []string, logger *slog.Logger) (*mailbox, func()) {
	if len(accepted) == 0 {
		return nil, func() {}
	}
	mb := &mailbox{
		outlets: make(map[string]delivery, len(accepted)),
		ctrl:    make(chan func(*mailboxState)),
		done:    make(chan struct{}),
		logger:  logger,
	}
	for name, s := range accepted {
		mb.outlets[name] = s.newOutlet()
	}
	st := &mailboxState{
		pending:  map[string]*fsmv1.Signal{},
		consumed: map[string]struct{}{},
		decoded:  map[string]reflect.Value{},
	}
	for _, id := range consumed {
		st.consumed[id] = struct{}{}
	}
	go mb.deliver(st)
	return mb, func() { close(mb.done) }
}

// deliver is the mailbox's goroutine: it offers each name's head and applies control functions
// until the run ends.
func (mb *mailbox) deliver(st *mailboxState) {
	for {
		cases, heads := mb.cases(st)
		chosen, recv, _ := reflect.Select(cases)
		switch chosen {
		case 0:
			recv.Interface().(func(*mailboxState))(st)
		case 1:
			return
		default:
			st.received = append(st.received, heads[chosen-2])
		}
	}
}

// cases is one select over the control channel, the run's end, and a send of every accepted
// name's head signal; heads holds the IDs of those sends, in case order.
func (mb *mailbox) cases(st *mailboxState) (cases []reflect.SelectCase, heads []string) {
	cases = []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(mb.ctrl)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(mb.done)},
	}
	for name, o := range mb.outlets {
		id, v, ok := mb.offering(st, name, o)
		if !ok {
			continue
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectSend, Chan: o.channel(), Send: v})
		heads = append(heads, id)
	}
	return cases, heads
}

// offering is name's head signal as its receiver's value, decoded once. A signal that does not
// decode was validated when accepted, so its record is corrupt: it counts as received, so the
// COMPLETE consumes it rather than it holding up the name for the rest of the run.
func (mb *mailbox) offering(st *mailboxState, name string, o delivery) (string, reflect.Value, bool) {
	sig := st.head(name)
	if sig == nil {
		return "", reflect.Value{}, false
	}
	if v, ok := st.decoded[sig.GetId()]; ok {
		return sig.GetId(), v, true
	}
	v, err := o.value(sig)
	if err != nil {
		mb.logger.Error("failed to decode signal, dropping it", "error", err, "signal", sig.GetId(), "name", name)
		st.received = append(st.received, sig.GetId())
		return "", reflect.Value{}, false
	}
	st.decoded[sig.GetId()] = v
	return sig.GetId(), v, true
}

// head is the oldest pending signal of name not received in this attempt.
func (st *mailboxState) head(name string) *fsmv1.Signal {
	var head *fsmv1.Signal
	for id, sig := range st.pending {
		if sig.GetName() != name || slices.Contains(st.received, id) {
			continue
		}
		if head == nil || id < head.GetId() {
			head = sig
		}
	}
	return head
}

// do runs f on the delivery goroutine and waits for it. It reports false, without running f,
// once the run has ended.
func (mb *mailbox) do(f func(*mailboxState)) bool {
	ran := make(chan struct{})
	select {
	case mb.ctrl <- func(st *mailboxState) { f(st); close(ran) }:
	case <-mb.done:
		return false
	}
	<-ran
	return true
}

// outlet returns the delivery of an accepted signal name. A nil mailbox — a run whose FSM
// accepts no signals — has none.
func (mb *mailbox) outlet(name string) (delivery, bool) {
	if mb == nil {
		return nil, false
	}
	o, ok := mb.outlets[name]
	return o, ok
}

// offer adds signals to the pending set, skipping any already pending or consumed.
func (mb *mailbox) offer(sigs ...*fsmv1.Signal) {
	if mb == nil {
		return
	}
	mb.do(func(st *mailboxState) {
		for _, sig := range sigs {
			if _, done := st.consumed[sig.GetId()]; done {
				continue
			}
			st.pending[sig.GetId()] = sig
		}
	})
}

// refresh reads, and offers, the run's pending signals among ids that the mailbox has not seen.
func (mb *mailbox) refresh(ctx context.Context, store Store, version ulid.ULID, ids []string) {
	if mb == nil {
		return
	}
	var unseen []string
	mb.do(func(st *mailboxState) {
		for _, id := range ids {
			_, pending := st.pending[id]
			_, consumed := st.consumed[id]
			if pending || consumed {
				continue
			}
			unseen = append(unseen, id)
		}
	})

	sigs := make([]*fsmv1.Signal, 0, len(unseen))
	for _, id := range unseen {
		sig, err := store.signal(ctx, version, id)
		if err != nil {
			mb.logger.ErrorContext(ctx, "failed to read pending signal", "error", err, versionAttr(version), "signal", id)
			continue
		}
		sigs = append(sigs, sig)
	}
	mb.offer(sigs...)
}

// beginAttempt puts every signal received in a failed attempt back on offer: a retried
// transition receives the unconsumed signals again.
func (mb *mailbox) beginAttempt() {
	if mb == nil {
		return
	}
	mb.do(func(st *mailboxState) { st.received = nil })
}

// received returns the IDs handlers received in the current attempt, which the transition's
// COMPLETE records as consumed.
func (mb *mailbox) received() []string {
	if mb == nil {
		return nil
	}
	var ids []string
	mb.do(func(st *mailboxState) { ids = slices.Clone(st.received) })
	return ids
}

// consume drops signals whose consumption a COMPLETE has recorded.
func (mb *mailbox) consume(ids []string) {
	if mb == nil || len(ids) == 0 {
		return
	}
	mb.do(func(st *mailboxState) {
		for _, id := range ids {
			delete(st.pending, id)
			delete(st.decoded, id)
			st.consumed[id] = struct{}{}
		}
		st.received = nil
	})
}
