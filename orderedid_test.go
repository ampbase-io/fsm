package fsm

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
)

// staleClock is the interleaving ulid.Make admits, as a sequence of the
// milliseconds three mints hand the reader: the third read its clock before
// the second took the lock.
func staleClock() func() uint64 {
	seq := []uint64{1000, 1001, 1000}
	return func() uint64 {
		ms := seq[0]
		seq = seq[1:]
		return ms
	}
}

// TestOrderedIDs_MintOrderSurvivesAStaleMillisecond: through the minter the
// three IDs sort as minted, every run — the clamp hands the reader 1001 for
// the third mint and it increments.
func TestOrderedIDs_MintOrderSurvivesAStaleMillisecond(t *testing.T) {
	t.Parallel()

	for range 200 {
		ids := newOrderedIDs(staleClock())
		first, second, third := ids.mint(), ids.mint(), ids.mint()
		if !(first.Compare(second) < 0 && second.Compare(third) < 0) {
			t.Fatalf("IDs do not sort in mint order: %s %s %s", first, second, third)
		}
	}
}

// TestOrderedIDs_LibraryReaderReordersAStaleMillisecond pins the premise
// against the library the minter wraps: the same three milliseconds through
// ulid's own locked monotonic reader, which is what ulid.Make uses, put the
// third ID before the first in roughly half the runs. If a ulid upgrade makes
// this pass zero times, the minter's clamp has become redundant and this test
// says so rather than letting it rot.
func TestOrderedIDs_LibraryReaderReordersAStaleMillisecond(t *testing.T) {
	t.Parallel()

	const runs = 200
	reordered := 0
	for range runs {
		r := &ulid.LockedMonotonicReader{MonotonicReader: ulid.Monotonic(rand.Reader, 0)}
		first := ulid.MustNew(1000, r)
		_ = ulid.MustNew(1001, r)
		if third := ulid.MustNew(1000, r); third.Compare(first) < 0 {
			reordered++
		}
	}
	if reordered == 0 {
		t.Fatalf("the library reader kept mint order in all %d runs; the premise behind orderedIDs no longer holds", runs)
	}
	t.Logf("the library reader put the third ID before the first in %d of %d runs", reordered, runs)
}

// TestOrderedIDs_SameMillisecondIncrements: a fixed clock, the case a
// consumer's test constructs, orders by the entropy alone and still sorts as
// minted.
func TestOrderedIDs_SameMillisecondIncrements(t *testing.T) {
	t.Parallel()

	ids := newOrderedIDs(func() uint64 { return 1000 })
	prev := ids.mint()
	for range 100 {
		next := ids.mint()
		if next.Compare(prev) <= 0 {
			t.Fatalf("%s does not sort after %s", next, prev)
		}
		if next.Time() != 1000 {
			t.Fatalf("mint under a fixed clock carries ms %d, want 1000", next.Time())
		}
		prev = next
	}
}

// TestOrderedIDs_AreTheOnlyMinterOfSignalIDs: a file that builds a Signal is
// a file that stamps its ID, and ulid.Make there is an ID minted outside the
// lock that keeps them ordered. The package's other ulid.Make calls mint run
// versions, transition IDs, object-store event versions and a node ID, none of
// which a signal is ever ordered against.
func TestOrderedIDs_AreTheOnlyMinterOfSignalIDs(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	builders := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(src), "fsmv1.Signal{") {
			continue
		}
		builders++
		if strings.Contains(string(src), "ulid.Make(") {
			t.Errorf("%s builds a Signal and calls ulid.Make; signal IDs come from orderedIDs.mint", file)
		}
	}
	if builders == 0 {
		t.Fatal("no file builds an fsmv1.Signal; the check above matched nothing")
	}
}
