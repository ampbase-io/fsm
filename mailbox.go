package fsm

import (
	"context"
	"log/slog"
	"slices"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// mailbox routes one executing run's signals to an outlet per accepted name. Each outlet is a
// goroutine that owns its name's state and offers the oldest signal of that name not yet received
// in this attempt on its unbuffered channel, so a signal counts as received only when a handler's
// receive completes. Every other call hands an outlet an op, which it applies before offering
// again: no state is shared, so nothing needs a lock, and what an outlet reports as received is
// never behind a receive that already happened.
//
// A run whose FSM accepts no signals has a nil mailbox; close, outlet, received and consume, the
// methods every run reaches, accept one.
type mailbox struct {
	outlets map[string]anyOutlet
	done    chan struct{}
	logger  *slog.Logger
}

// anyOutlet is an outlet of any payload type, as the mailbox routes to it.
type anyOutlet interface {
	// start runs the outlet's goroutine until done closes.
	start(done <-chan struct{}, consumed []string, logger *slog.Logger)
	offer(sig *fsmv1.Signal)
	beginAttempt()
	received() []string
	consume(ids []string)
	// unseen returns the ids the outlet neither holds nor has consumed.
	unseen(ids []string) []string
}

// newMailbox starts an outlet for each signal a run's FSM accepts, each seeded with the IDs the
// run has already consumed. It returns nil for a run whose FSM accepts none.
func newMailbox(accepted map[string]AnySignal, consumed []string, logger *slog.Logger) *mailbox {
	if len(accepted) == 0 {
		return nil
	}
	mb := &mailbox{
		outlets: make(map[string]anyOutlet, len(accepted)),
		done:    make(chan struct{}),
		logger:  logger,
	}
	for name, s := range accepted {
		o := s.newOutlet()
		o.start(mb.done, consumed, logger)
		mb.outlets[name] = o
	}
	return mb
}

// close stops the outlets when the run ends.
func (mb *mailbox) close() {
	if mb == nil {
		return
	}
	close(mb.done)
}

// outlet returns the outlet of an accepted signal name.
func (mb *mailbox) outlet(name string) (anyOutlet, bool) {
	if mb == nil {
		return nil, false
	}
	o, ok := mb.outlets[name]
	return o, ok
}

// offer routes each signal to its name's outlet. A signal whose name the running definition does
// not accept, left by an earlier one, is logged and never offered.
func (mb *mailbox) offer(sigs ...*fsmv1.Signal) {
	for _, sig := range sigs {
		o, ok := mb.outlets[sig.GetName()]
		if !ok {
			mb.logger.Error("signal name not accepted by this run's FSM, not offering it", "signal", sig.GetId(), "name", sig.GetName())
			continue
		}
		o.offer(sig)
	}
}

// signalReader reads one pending signal: all refresh needs of a store.
type signalReader interface {
	signal(ctx context.Context, version ulid.ULID, id string) (*fsmv1.Signal, error)
}

// refresh reads, and offers, the run's pending signals among ids that no outlet has seen.
func (mb *mailbox) refresh(ctx context.Context, store signalReader, version ulid.ULID, ids []string) {
	for _, o := range mb.outlets {
		ids = o.unseen(ids)
	}
	sigs := make([]*fsmv1.Signal, 0, len(ids))
	for _, id := range ids {
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
	for _, o := range mb.outlets {
		o.beginAttempt()
	}
}

// received returns the IDs handlers received in the current attempt, which the transition's
// COMPLETE records as consumed.
func (mb *mailbox) received() []string {
	if mb == nil {
		return nil
	}
	var ids []string
	for _, o := range mb.outlets {
		ids = append(ids, o.received()...)
	}
	return ids
}

// consume drops signals whose consumption a COMPLETE has recorded.
func (mb *mailbox) consume(ids []string) {
	if mb == nil || len(ids) == 0 {
		return
	}
	for _, o := range mb.outlets {
		o.consume(ids)
	}
}

// outlet delivers one accepted signal name's signals for one run. Its goroutine alone touches
// pending, consumed and received; everything else reaches them through ops.
type outlet[T any] struct {
	codec Codec
	ch    chan Received[T]
	ops   chan outletOp

	// replies carries received's answer. It is allocated once, since only the run's own
	// goroutine asks.
	replies chan []string

	done   <-chan struct{}
	logger *slog.Logger

	// pending holds this name's offered signals not yet consumed, decoded, by ID.
	pending map[string]Received[T]

	// consumed holds the IDs a COMPLETE recorded consumed, so a marker whose delete failed is
	// never offered again.
	consumed map[string]struct{}

	// taken holds the IDs handlers received during the current attempt, in order.
	taken []string
}

type opKind int

const (
	opOffer opKind = iota
	opBegin
	opConsume
	opReceived
	opUnseen
)

// outletOp is one call handed to an outlet's goroutine.
type outletOp struct {
	kind  opKind
	sig   *fsmv1.Signal
	ids   []string
	reply chan []string
}

func newOutlet[T any](codec Codec) *outlet[T] {
	return &outlet[T]{
		codec:    codec,
		ch:       make(chan Received[T]),
		ops:      make(chan outletOp),
		replies:  make(chan []string),
		pending:  map[string]Received[T]{},
		consumed: map[string]struct{}{},
	}
}

func (o *outlet[T]) start(done <-chan struct{}, consumed []string, logger *slog.Logger) {
	o.done, o.logger = done, logger
	for _, id := range consumed {
		o.consumed[id] = struct{}{}
	}
	go o.run()
}

// run offers the head signal while applying ops, until the run ends. With nothing to offer the
// send case is on a nil channel, which never proceeds.
func (o *outlet[T]) run() {
	for {
		send, id, head := o.offering()
		select {
		case send <- head:
			o.taken = append(o.taken, id)
		case op := <-o.ops:
			o.apply(op)
		case <-o.done:
			return
		}
	}
}

// offering is the channel to offer the head signal on, with its ID and value: the oldest pending
// signal not received in this attempt, or a nil channel when there is none.
func (o *outlet[T]) offering() (chan<- Received[T], string, Received[T]) {
	var head string
	for id := range o.pending {
		if slices.Contains(o.taken, id) {
			continue
		}
		if head == "" || id < head {
			head = id
		}
	}
	if head == "" {
		return nil, "", Received[T]{}
	}
	return o.ch, head, o.pending[head]
}

func (o *outlet[T]) apply(op outletOp) {
	switch op.kind {
	case opOffer:
		o.add(op.sig)
	case opBegin:
		o.taken = nil
	case opConsume:
		for _, id := range op.ids {
			if _, ours := o.pending[id]; ours {
				delete(o.pending, id)
				o.consumed[id] = struct{}{}
			}
		}
	case opReceived:
		o.reply(op.reply, o.taken)
	case opUnseen:
		o.reply(op.reply, o.unseenOf(op.ids))
	}
}

// add decodes and holds a signal the outlet has not seen. A signal that does not decode was
// validated when accepted, so its record is corrupt: it is logged and never offered.
func (o *outlet[T]) add(sig *fsmv1.Signal) {
	if o.seen(sig.GetId()) {
		return
	}
	id, err := ulid.Parse(sig.GetId())
	if err != nil {
		o.logger.Error("malformed signal ID, not offering it", "error", err, "signal", sig.GetId())
		return
	}
	var msg T
	if err := o.codec.Unmarshal(sig.GetPayload(), &msg); err != nil {
		o.logger.Error("failed to decode signal, not offering it", "error", err, "signal", sig.GetId(), "name", sig.GetName())
		return
	}
	o.pending[sig.GetId()] = Received[T]{ID: id, SentAt: ulid.Time(id.Time()), Msg: &msg}
}

// seen reports whether the outlet holds the signal, or has consumed it.
func (o *outlet[T]) seen(id string) bool {
	_, pending := o.pending[id]
	_, consumed := o.consumed[id]
	return pending || consumed
}

func (o *outlet[T]) unseenOf(ids []string) []string {
	var unseen []string
	for _, id := range ids {
		if !o.seen(id) {
			unseen = append(unseen, id)
		}
	}
	return unseen
}

// reply answers an op, unless the run has ended.
func (o *outlet[T]) reply(to chan []string, ids []string) {
	select {
	case to <- ids:
	case <-o.done:
	}
}

// do hands an op to the goroutine. An unbuffered send completes only when the goroutine takes the
// op, and it applies the op before offering again. Once the run has ended it does nothing.
func (o *outlet[T]) do(op outletOp) bool {
	select {
	case o.ops <- op:
		return true
	case <-o.done:
		return false
	}
}

// ask hands the goroutine an op that answers, and waits for the answer.
func (o *outlet[T]) ask(op outletOp) []string {
	if !o.do(op) {
		return nil
	}
	select {
	case ids := <-op.reply:
		return ids
	case <-o.done:
		return nil
	}
}

func (o *outlet[T]) offer(sig *fsmv1.Signal) { o.do(outletOp{kind: opOffer, sig: sig}) }

func (o *outlet[T]) beginAttempt() { o.do(outletOp{kind: opBegin}) }

func (o *outlet[T]) consume(ids []string) { o.do(outletOp{kind: opConsume, ids: ids}) }

func (o *outlet[T]) received() []string {
	return o.ask(outletOp{kind: opReceived, reply: o.replies})
}

// unseen asks with a reply channel of its own: the sweep and a run's start may ask at once.
func (o *outlet[T]) unseen(ids []string) []string {
	return o.ask(outletOp{kind: opUnseen, ids: ids, reply: make(chan []string)})
}
