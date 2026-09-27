package fsm

import (
	"context"
	"log/slog"
	"runtime"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// testMailbox starts a mailbox accepting pause and advance, seeded with consumed, closed when the
// test ends.
func testMailbox(t *testing.T, consumed ...string) *mailbox {
	t.Helper()
	accepted, err := acceptedSignals([]AnySignal{testPause, testAdvance})
	if err != nil {
		t.Fatal(err)
	}
	mb := newMailbox(accepted, consumed, slog.New(slog.DiscardHandler))
	t.Cleanup(mb.close)
	return mb
}

// channel is the mailbox's receive channel for an accepted signal, as Receive returns it.
func channel(t *testing.T, mb *mailbox, s Signal[command]) <-chan Received[command] {
	t.Helper()
	o, ok := mb.outlet(s.name)
	if !ok {
		t.Fatalf("signal %s not accepted", s.name)
	}
	return o.(*outlet[command]).ch
}

// signalOf is a stored signal of name carrying msg, with a new ID.
func signalOf(t *testing.T, name string, msg command) *fsmv1.Signal {
	t.Helper()
	payload, err := testAdvance.codec.Marshal(&msg)
	if err != nil {
		t.Fatal(err)
	}
	return &fsmv1.Signal{Id: ulid.Make().String(), Name: name, Payload: payload}
}

// TestMailboxOldestFirst verifies a name's signals are offered oldest ID first, whatever order
// they were offered in.
func TestMailboxOldestFirst(t *testing.T) {
	mb := testMailbox(t)
	older := signalOf(t, "advance", command{Note: "older"})
	newer := signalOf(t, "advance", command{Note: "newer"})
	mb.offer(newer, older)

	ch := channel(t, mb, testAdvance)
	for _, want := range []string{"older", "newer"} {
		if got := within(t, ch, time.Second, want).Msg.Note; got != want {
			t.Fatalf("expected %s, got %s", want, got)
		}
	}
}

// TestMailboxNamesIndependent verifies an unread signal of one name does not hold up another.
func TestMailboxNamesIndependent(t *testing.T) {
	mb := testMailbox(t)
	mb.offer(signalOf(t, "pause", command{}), signalOf(t, "advance", command{Note: "go"}))

	if got := within(t, channel(t, mb, testAdvance), time.Second, "advance").Msg.Note; got != "go" {
		t.Fatalf("expected the advance, got %q", got)
	}
}

// TestMailboxAttemptReoffers verifies a signal received in an attempt is offered again after the
// next attempt begins, and counts as received once more.
func TestMailboxAttemptReoffers(t *testing.T) {
	mb := testMailbox(t)
	sig := signalOf(t, "advance", command{})
	mb.offer(sig)
	ch := channel(t, mb, testAdvance)

	within(t, ch, time.Second, "the first attempt's receive")
	mb.beginAttempt()
	again := within(t, ch, time.Second, "the retry's receive")
	if again.ID.String() != sig.GetId() {
		t.Fatalf("expected %s again, got %s", sig.GetId(), again.ID)
	}
	if got := mb.received(); len(got) != 1 || got[0] != sig.GetId() {
		t.Fatalf("expected the retry to have received only %s, got %v", sig.GetId(), got)
	}
}

// TestMailboxConsumedNeverOfferedAgain verifies a consumed signal is not offered again when it is
// offered anew, as a marker whose delete failed is on the next listing, nor when the mailbox starts
// with it already consumed.
func TestMailboxConsumedNeverOfferedAgain(t *testing.T) {
	sig := signalOf(t, "advance", command{})

	mb := testMailbox(t)
	ch := channel(t, mb, testAdvance)
	mb.offer(sig)
	within(t, ch, time.Second, "the signal")
	mb.consume(mb.received())
	mb.beginAttempt()
	mb.offer(sig)
	if !noSignal(ch) {
		t.Fatal("expected a consumed signal not to be offered again")
	}

	resumed := testMailbox(t, sig.GetId())
	resumed.offer(sig)
	if !noSignal(channel(t, resumed, testAdvance)) {
		t.Fatal("expected a signal consumed before the run resumed not to be offered")
	}
}

// TestMailboxSkipsUndecodable verifies a signal whose payload does not decode is dropped, not
// offered, and does not hold up the next signal of its name.
func TestMailboxSkipsUndecodable(t *testing.T) {
	mb := testMailbox(t)
	corrupt := &fsmv1.Signal{Id: ulid.Make().String(), Name: "advance", Payload: []byte("not json")}
	good := signalOf(t, "advance", command{Note: "good"})
	mb.offer(corrupt, good)

	if got := within(t, channel(t, mb, testAdvance), time.Second, "the good signal").Msg.Note; got != "good" {
		t.Fatalf("expected the good signal, got %q", got)
	}
}

// countingStore is a signalStore serving pending signals from memory and counting their reads.
type countingStore struct {
	signals map[string]*fsmv1.Signal
	reads   map[string]int
}

func (s *countingStore) liveRun(context.Context, ulid.ULID) (Run, error) { return Run{}, nil }

func (s *countingStore) recordSignal(context.Context, Run, *fsmv1.Signal) error { return nil }

func (s *countingStore) pendingSignalIDs(context.Context, ulid.ULID) ([]string, error) {
	return nil, nil
}

func (s *countingStore) signal(_ context.Context, _ ulid.ULID, id string) (*fsmv1.Signal, error) {
	s.reads[id]++
	return s.signals[id], nil
}

// TestMailboxRefreshReadsOnlyUnseen verifies refresh reads a signal from the store only when no
// outlet holds it or has consumed it, so a heartbeat does not read pending signals again.
func TestMailboxRefreshReadsOnlyUnseen(t *testing.T) {
	ctx := context.Background()
	first := signalOf(t, "advance", command{})
	second := signalOf(t, "pause", command{})
	store := &countingStore{
		signals: map[string]*fsmv1.Signal{first.GetId(): first, second.GetId(): second},
		reads:   map[string]int{},
	}
	version := ulid.Make()
	mb := testMailbox(t)

	mb.refresh(ctx, store, version, []string{first.GetId()})
	mb.refresh(ctx, store, version, []string{first.GetId(), second.GetId()})
	within(t, channel(t, mb, testAdvance), time.Second, "the first signal")
	mb.consume(mb.received())
	mb.refresh(ctx, store, version, []string{first.GetId(), second.GetId()})

	for _, id := range []string{first.GetId(), second.GetId()} {
		if store.reads[id] != 1 {
			t.Fatalf("expected %s read once, read %d times", id, store.reads[id])
		}
	}
}

// TestMailboxNil verifies the methods every run reaches accept the nil mailbox of a run whose FSM
// accepts no signals.
func TestMailboxNil(t *testing.T) {
	var mb *mailbox
	mb.close()
	if _, ok := mb.outlet("advance"); ok {
		t.Fatal("expected a nil mailbox to have no outlet")
	}
	if got := mb.received(); got != nil {
		t.Fatalf("expected a nil mailbox to have received nothing, got %v", got)
	}
	mb.consume([]string{ulid.Make().String()})
}

// TestMailboxCloseStopsOutlets verifies closing the mailbox ends every outlet's goroutine, so none
// outlives its run.
func TestMailboxCloseStopsOutlets(t *testing.T) {
	before := runtime.NumGoroutine()
	accepted, err := acceptedSignals([]AnySignal{testPause, testAdvance})
	if err != nil {
		t.Fatal(err)
	}
	mb := newMailbox(accepted, nil, slog.New(slog.DiscardHandler))
	mb.offer(signalOf(t, "advance", command{}))
	mb.close()

	eventually(t, time.Second, func() bool { return runtime.NumGoroutine() <= before }, "expected the outlets' goroutines to end")
}
