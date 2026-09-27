package fsm

import (
	"context"
	"slices"
	"testing"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// boltStoreOf returns the test manager's BoltDB store.
func boltStoreOf(t *testing.T) *boltStore {
	t.Helper()
	m := newTestManager(t)
	bs, ok := m.store.(*boltStore)
	if !ok {
		t.Fatalf("expected boltStore, got %T", m.store)
	}
	return bs
}

// startBoltRun records a run's START directly on the store.
func startBoltRun(t *testing.T, s *boltStore, id string) Run {
	t.Helper()
	run := Run{ID: id, StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq"}
	_, err := s.Start(context.Background(), run, &fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_START,
		Id:           run.ID,
		ResourceType: run.TypeName,
		Action:       run.Action,
		State:        "created",
	}, &startRecord{Resource: []byte("{}"), Transitions: []string{"created", "done"}})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	return run
}

// recordBoltSignal records a signal of name for run directly on the store.
func recordBoltSignal(t *testing.T, s *boltStore, run Run, name string) *fsmv1.Signal {
	t.Helper()
	sig := signalOf(t, name, command{})
	if err := s.recordSignal(context.Background(), run, sig); err != nil {
		t.Fatalf("record signal: %v", err)
	}
	return sig
}

func pendingIDs(t *testing.T, s *boltStore, run Run) []string {
	t.Helper()
	ids, err := s.pendingSignalIDs(context.Background(), run.StartVersion)
	if err != nil {
		t.Fatalf("pending signals: %v", err)
	}
	return ids
}

// TestBoltPendingSignalIDsPerRun verifies a run lists only its own pending entries.
func TestBoltPendingSignalIDsPerRun(t *testing.T) {
	s := boltStoreOf(t)
	mine := startBoltRun(t, s, "sig-mine")
	other := startBoltRun(t, s, "sig-other")

	sig := recordBoltSignal(t, s, mine, "advance")
	recordBoltSignal(t, s, other, "advance")

	if ids := pendingIDs(t, s, mine); !slices.Equal(ids, []string{sig.GetId()}) {
		t.Fatalf("expected only the run's own signal, got %v", ids)
	}
}

// TestBoltCompleteConsumesSignals verifies a COMPLETE that consumes a signal deletes its entry in
// the same transaction, leaving the run's other signals pending.
func TestBoltCompleteConsumesSignals(t *testing.T) {
	s := boltStoreOf(t)
	run := startBoltRun(t, s, "sig-consume")
	consumed := recordBoltSignal(t, s, run, "advance")
	unread := recordBoltSignal(t, s, run, "pause")

	_, err := s.Append(context.Background(), run, &fsmv1.StateEvent{
		Type:            fsmv1.EventType_EVENT_TYPE_COMPLETE,
		Id:              run.ID,
		ResourceType:    run.TypeName,
		Action:          run.Action,
		State:           "created",
		ConsumedSignals: []string{consumed.GetId()},
	})
	if err != nil {
		t.Fatalf("append COMPLETE: %v", err)
	}

	if ids := pendingIDs(t, s, run); !slices.Equal(ids, []string{unread.GetId()}) {
		t.Fatalf("expected only the unread signal pending, got %v", ids)
	}
}

// TestBoltFinishDiscardsSignals verifies a run's FINISH deletes the entries of signals it never
// read.
func TestBoltFinishDiscardsSignals(t *testing.T) {
	s := boltStoreOf(t)
	run := startBoltRun(t, s, "sig-finish")
	recordBoltSignal(t, s, run, "pause")

	if _, err := s.Append(context.Background(), run, finishEvent(run, "done")); err != nil {
		t.Fatalf("append FINISH: %v", err)
	}

	if ids := pendingIDs(t, s, run); len(ids) != 0 {
		t.Fatalf("expected no pending signal after FINISH, got %v", ids)
	}
}
