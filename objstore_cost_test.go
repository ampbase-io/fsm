package fsm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// cost is the object-store requests one phase of a run pays. Latency on a real bucket is one
// round trip per request and a conditional PUT costs north of 100 ms, so these counts are the
// run's wall-clock cost, phase by phase.
type cost struct {
	puts    int
	gets    int
	lists   int
	deletes int
}

// TestRunObjectCost is the cost gate: it pins what each phase of a run spends on the object
// store, so a read creeping back in front of a write fails here rather than showing up in a
// trace. The numbers are a budget, not a measurement to update casually — a phase that needs
// another request needs a reason in the diff. What requests overlap is the other half of the
// cost and is pinned by TestWritesIssuedTogether.
//
// A nine-transition run therefore pays 4 + 1 + 9×2 + 4 = 27 requests, every one a write, in 3 +
// 1 + 9 + 3 = 16 round trips. It used to pay 4 + 2 + 9×3 + 5 = 38 requests — 11 of them reads
// standing in front of a write — in 38 round trips, since none of them overlapped.
func TestRunObjectCost(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)

	spent := func(fn func()) cost {
		before := cost{h.s3.Puts(), h.s3.Gets(), h.s3.Lists(), h.s3.Deletes()}
		fn()
		return cost{h.s3.Puts() - before.puts, h.s3.Gets() - before.gets,
			h.s3.Lists() - before.lists, h.s3.Deletes() - before.deletes}
	}

	run := Run{ID: "cost-1", StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq"}
	for _, phase := range []struct {
		name string
		run  func()
		want cost
	}{
		// The lock, then the event, index marker and parent link together, then the manifest.
		{"start", func() { startCostRun(t, s, run) }, cost{puts: 4}},
		// One conditional PUT against the manifest this node last wrote.
		{"set running", func() { mustSetRunning(t, s, run) }, cost{puts: 1}},
		// The transition's event beside its manifest CAS.
		{"transition", func() { mustComplete(t, s, run, "one") }, cost{puts: 2}},
		// The FINISH event before the flip (the history record names it), the flip, then the
		// lock delete beside the history write.
		{"finish", func() { mustFinish(t, s, run) }, cost{puts: 3, deletes: 1}},
	} {
		if got := spent(phase.run); got != phase.want {
			t.Errorf("%s spent %+v, want %+v", phase.name, got, phase.want)
			continue
		}
		t.Logf("%-12s %+v", phase.name, phase.want)
	}
}

// TestWritesIssuedTogether pins the overlap the budget above cannot see: a phase's independent
// writes have to be in flight at once, since a request count says nothing about how many round
// trips it is. Each write in the group is held until the rest of the group has arrived, so the
// same writes issued one after another cannot pass.
func TestWritesIssuedTogether(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	run := Run{ID: "together-1", StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq"}

	// The START's event and index marker, beside each other behind the lock. (A run with a
	// parent writes its link in the same group; this one has none.)
	events := s.eventPrefix(run.ID, run.Action, run.StartVersion)
	index := s.runIndexKey(run.TypeName, run.ID, run.Action, run.StartVersion)
	start := newPutGroup(2, func(key string) bool {
		return strings.HasPrefix(key, events) || key == index
	})
	h.s3.SetPrePut(start.arrive)
	startCostRun(t, s, run)
	if err := start.err(); err != nil {
		t.Fatalf("the START's provisional objects: %v", err)
	}

	// A transition's event, beside the manifest write that records it.
	manifest := s.manifestKey(run.StartVersion)
	mustSetRunning(t, s, run)
	complete := newPutGroup(2, func(key string) bool {
		return strings.HasPrefix(key, events) || key == manifest
	})
	h.s3.SetPrePut(complete.arrive)
	mustComplete(t, s, run, "one")
	if err := complete.err(); err != nil {
		t.Fatalf("the transition's event and manifest: %v", err)
	}
	h.s3.SetPrePut(nil)
}

// putGroup holds every matching PUT until want of them are in flight at once. A group issued
// together closes its gate and nothing waits; a group issued serially leaves the first write
// waiting out groupWait, which is the failure it reports rather than a hang.
type putGroup struct {
	want  int
	match func(key string) bool

	mu       sync.Mutex
	arrived  int
	together chan struct{}

	serial atomic.Bool
}

// groupWait is how long one write of a group waits for the rest. Every write here is served by
// an in-process fake, so this is a failure bound, not a timing assumption.
const groupWait = 2 * time.Second

func newPutGroup(want int, match func(key string) bool) *putGroup {
	return &putGroup{want: want, match: match, together: make(chan struct{})}
}

func (g *putGroup) arrive(key string) {
	if !g.match(key) {
		return
	}

	g.mu.Lock()
	g.arrived++
	if g.arrived == g.want {
		close(g.together)
	}
	g.mu.Unlock()

	select {
	case <-g.together:
	case <-time.After(groupWait):
		g.serial.Store(true)
	}
}

func (g *putGroup) err() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case g.serial.Load():
		return fmt.Errorf("%d writes were issued one at a time, want %d in flight together", g.arrived, g.want)
	case g.arrived != g.want:
		return fmt.Errorf("%d writes arrived, want %d", g.arrived, g.want)
	}
	return nil
}

// startCostRun starts a three-transition run with no parent — the shape the budget above counts.
func startCostRun(t *testing.T, s *objectStore, run Run) {
	t.Helper()
	_, err := s.Start(context.Background(), run, &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_START,
		Id:           run.ID,
		ResourceType: run.TypeName,
		Action:       run.Action,
		State:        "one",
	}, &startRecord{Resource: []byte("{}"), Transitions: []string{"one", "two", "three", "done"}})
	if err != nil {
		t.Fatalf("failed to start the run: %v", err)
	}
}

func mustSetRunning(t *testing.T, s *objectStore, run Run) {
	t.Helper()
	if err := s.SetRunning(context.Background(), run); err != nil {
		t.Fatalf("failed to set the run running: %v", err)
	}
}

func mustComplete(t *testing.T, s *objectStore, run Run, state string) {
	t.Helper()
	_, err := s.Append(context.Background(), run, &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_COMPLETE,
		Id:           run.ID,
		ResourceType: run.TypeName,
		Action:       run.Action,
		State:        state,
	})
	if err != nil {
		t.Fatalf("failed to complete %s: %v", state, err)
	}
}

func mustFinish(t *testing.T, s *objectStore, run Run) {
	t.Helper()
	if err := appendFinished(s, run); err != nil {
		t.Fatalf("failed to finish the run: %v", err)
	}
}
