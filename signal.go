package fsm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

var (
	// errSignalNotDeclared refuses a signal whose name the run's FSM did not declare.
	errSignalNotDeclared = errors.New("signal not declared on the run's FSM")

	// errInvalidSignal refuses a signal whose payload does not decode as its declared type.
	errInvalidSignal = errors.New("signal payload does not decode as its declared type")
)

// Signal is a signal name bound to its payload type T. Declare one with NewSignal, accept it on an
// FSM with WithSignals, send it with Send and read it in a transition with Receive.
type Signal[T any] struct {
	name  string
	codec Codec
	err   error
}

// NewSignal declares a signal named name with payload type T. The payload codec is probed from
// *T as requests' are: protobuf for a proto.Message, the type itself for a Codec, else JSON. A
// type no codec can handle, or an empty name, fails the build of any FSM that accepts it.
func NewSignal[T any](name string) Signal[T] {
	if name == "" {
		return Signal[T]{err: errors.New("signal name must not be empty")}
	}
	var t T
	codec, err := determineCodec(slog.New(slog.DiscardHandler), &t)
	if err != nil {
		err = fmt.Errorf("signal %s: %w", name, err)
	}
	return Signal[T]{name: name, codec: codec, err: err}
}

// Received is a signal as a transition receives it.
type Received[T any] struct {
	// ID identifies the signal. It is stable across redeliveries, so a handler that must act on
	// a signal once dedupes on it.
	ID ulid.ULID

	// SentAt is when the signal was accepted.
	SentAt time.Time

	Msg *T
}

// Send records the signal for the run and returns its ID. The run must be executing or pending:
// a finished or unknown run refuses with ErrFsmNotFound. The run's FSM must accept the signal
// (WithSignals). A transition reading the signal receives it; one sent to a run on another node
// reaches it through that node's sweep.
func (s Signal[T]) Send(ctx context.Context, m *Manager, version ulid.ULID, msg *T) (ulid.ULID, error) {
	if s.err != nil {
		return ulid.ULID{}, s.err
	}
	payload, err := s.codec.Marshal(msg)
	if err != nil {
		return ulid.ULID{}, fmt.Errorf("signal %s: %w", s.name, err)
	}
	return m.signal(ctx, version, s.name, payload)
}

// Receive returns the channel on which the transition running req receives this signal. Signals
// arrive in ID order among those visible, the unconsumed ones first on every attempt. A signal
// counts as received when the receive completes, and is consumed when the transition's COMPLETE
// is recorded. Receive panics if the run's FSM did not accept this signal: a programming error,
// never a channel that silently blocks forever.
func (s Signal[T]) Receive(req AnyRequest) <-chan Received[T] {
	o, ok := req.mailbox().outlet(s.name)
	if !ok {
		panic(fmt.Sprintf("fsm: signal %q is not accepted by this run's FSM (WithSignals)", s.name))
	}
	typed, ok := o.(*outlet[T])
	if !ok {
		panic(fmt.Sprintf("fsm: signal %q is accepted with a different payload type", s.name))
	}
	return typed.ch
}

func (s Signal[T]) signalName() string { return s.name }

func (s Signal[T]) declErr() error { return s.err }

func (s Signal[T]) check(payload []byte) error {
	var t T
	if err := s.codec.Unmarshal(payload, &t); err != nil {
		return fmt.Errorf("%w: %s: %w", errInvalidSignal, s.name, err)
	}
	return nil
}

func (s Signal[T]) newOutlet() delivery {
	return &outlet[T]{codec: s.codec, ch: make(chan Received[T])}
}

// AnySignal is a Signal of any payload type: what WithSignals takes and an FSM holds.
type AnySignal interface {
	signalName() string
	declErr() error
	// check decodes payload as the signal's type, refusing one that does not.
	check(payload []byte) error
	// newOutlet makes the signal's delivery channel for one run.
	newOutlet() delivery
}

type signalsOption[R, W any] []AnySignal

func (o signalsOption[R, W]) applyEnd(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	cfg.signals = append(cfg.signals, o...)
	return cfg
}

// WithSignals declares the signals an FSM's runs accept. Names must be unique within the FSM;
// several names may share a payload type.
func WithSignals[R, W any](signals ...AnySignal) EndOption[R, W] {
	return signalsOption[R, W](signals)
}

// acceptedSignals indexes an FSM's declared signals by name, refusing an invalid declaration or
// a name declared twice.
func acceptedSignals(signals []AnySignal) (map[string]AnySignal, error) {
	if len(signals) == 0 {
		return nil, nil
	}
	accepted := make(map[string]AnySignal, len(signals))
	for _, s := range signals {
		if err := s.declErr(); err != nil {
			return nil, err
		}
		if _, dup := accepted[s.signalName()]; dup {
			return nil, fmt.Errorf("signal %s declared twice", s.signalName())
		}
		accepted[s.signalName()] = s
	}
	return accepted, nil
}

// signal validates a signal against the run's FSM, records it durably and offers it to the run if
// it executes here. It is Send's and the RPC's one path.
func (m *Manager) signal(ctx context.Context, version ulid.ULID, name string, payload []byte) (ulid.ULID, error) {
	run, err := m.store.signalTarget(ctx, version)
	if err != nil {
		return ulid.ULID{}, err
	}
	f, ok := m.registeredFSM(fsmKey{typeName: run.TypeName, action: run.Action})
	if !ok {
		return ulid.ULID{}, fmt.Errorf("%w: %s/%s", errFSMNotRegistered, run.TypeName, run.Action)
	}
	declared, ok := f.signals[name]
	if !ok {
		return ulid.ULID{}, fmt.Errorf("%w: %s", errSignalNotDeclared, name)
	}
	if err := declared.check(payload); err != nil {
		return ulid.ULID{}, err
	}

	id := ulid.Make()
	sig := &fsmv1.Signal{Id: id.String(), Name: name, Payload: payload}
	if err := m.store.recordSignal(ctx, run, sig); err != nil {
		return ulid.ULID{}, err
	}
	if h, ok := m.executing(version); ok {
		h.mailbox.offer(sig)
	}
	return id, nil
}

// sweepSignals offers every executing run with a mailbox its pending signals, from one listing.
// It runs on the heartbeat and on a signal broadcast; a node executing no run that accepts
// signals lists nothing.
func (m *Manager) sweepSignals(ctx context.Context) {
	mailboxes := m.mailboxes()
	if len(mailboxes) == 0 {
		return
	}
	ids, err := m.store.signalIDs(ctx)
	if err != nil {
		m.logger.ErrorContext(ctx, "signal sweep failed", "error", err)
		return
	}
	for version, mb := range mailboxes {
		mb.refresh(ctx, m.store, version, ids[version])
	}
}

// loadSignals offers a run that starts executing here the signals already pending for it: those
// sent before it started, and on a resume those its previous owner had not consumed.
func (m *Manager) loadSignals(ctx context.Context, version ulid.ULID, mb *mailbox) {
	if mb == nil {
		return
	}
	ids, err := m.store.signalIDs(ctx)
	if err != nil {
		m.logger.ErrorContext(ctx, "failed to list pending signals", "error", err, versionAttr(version))
		return
	}
	mb.refresh(ctx, m.store, version, ids[version])
}

// mailboxes returns the mailbox of every executing run whose FSM accepts signals.
func (m *Manager) mailboxes() map[ulid.ULID]*mailbox {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mailboxes := map[ulid.ULID]*mailbox{}
	for version, h := range m.running {
		if h.mailbox != nil {
			mailboxes[version] = h.mailbox
		}
	}
	return mailboxes
}
