package fsm

import (
	"crypto/rand"
	"sync"

	"github.com/oklog/ulid/v2"
)

// signalIDs mints signal IDs that sort in the order they were minted, across
// goroutines. ulid.Make reads the clock before it takes the monotonic reader's
// lock, and the reader hands out fresh random entropy whenever the millisecond
// it is given differs from the one it last saw — so a mint that observed its
// millisecond, lost the lock to a mint on the next millisecond, and then took
// it, resets the reader and gets random entropy under the earlier millisecond,
// which sorts before the earlier ID about half the time. Everything that orders
// signals by ID (one name's outlet; a consumer draining several names) would
// then apply the later one first.
//
// Here the clock is read under the same lock as the entropy, and a millisecond
// behind the last one minted is clamped to it, so the reader only ever sees a
// non-decreasing sequence and increments within it. The clamp is also what
// makes a wall-clock step backwards harmless: without it the step would reset
// the reader the same way a stale caller does, and IDs minted during the step
// would sort before the ones minted just before it.
type signalIDs struct {
	mu   sync.Mutex
	now  func() uint64
	last uint64
	mono *ulid.MonotonicEntropy
}

func newSignalIDs(now func() uint64) *signalIDs {
	return &signalIDs{now: now, mono: ulid.Monotonic(rand.Reader, 0)}
}

func (s *signalIDs) mint() ulid.ULID {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms := s.now()
	if ms < s.last {
		ms = s.last
	}
	s.last = ms
	return ulid.MustNew(ms, s.mono)
}

// setClock replaces the clock mint reads. The seam a consumer's test uses to
// put several mints inside one millisecond, where only the entropy orders them.
func (s *signalIDs) setClock(now func() uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}
