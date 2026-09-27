package fsm

import (
	"context"
	"slices"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"
)

// recordTestSignal records a signal of name for run directly on the store, as SendSignal does
// once the name and payload are validated.
func recordTestSignal(t *testing.T, s *objectStore, run Run, name string) *fsmv1.Signal {
	t.Helper()
	sig := signalOf(t, name, command{})
	if err := s.recordSignal(context.Background(), run, sig); err != nil {
		t.Fatalf("record signal: %v", err)
	}
	return sig
}

// completeConsuming appends a COMPLETE for run that consumes ids, as the canceller does.
func completeConsuming(t *testing.T, s *objectStore, run Run, ids ...string) {
	t.Helper()
	event := &fsmv1.StateEvent{
		Type:            fsmv1.EventType_EVENT_TYPE_COMPLETE,
		Id:              run.ID,
		ResourceType:    run.TypeName,
		Action:          run.Action,
		State:           "created",
		ConsumedSignals: ids,
	}
	if _, err := s.Append(context.Background(), run, event); err != nil {
		t.Fatalf("append COMPLETE: %v", err)
	}
}

// TestObjectRecordSignalLeavesManifest verifies a send writes the SIGNAL event and the pending
// marker and never the run's manifest, so the lease owner stays its only, fenced, writer.
func TestObjectRecordSignalLeavesManifest(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()
	run := startRun(t, s, "sig-record")
	_, before, err := s.getManifest(ctx, run.StartVersion)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}

	sig := recordTestSignal(t, s, run, "advance")

	marker, err := s.signal(ctx, run.StartVersion, sig.GetId())
	if err != nil || marker.GetName() != "advance" {
		t.Fatalf("expected the marker to carry the signal, got %v, %v", marker, err)
	}
	events, err := s.listRunEvents(ctx, run.ID, run.Action, run.StartVersion)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	recorded := slices.ContainsFunc(events, func(e *fsmv1.StateEvent) bool {
		return e.GetType() == fsmv1.EventType_EVENT_TYPE_SIGNAL && e.GetSignal().GetId() == sig.GetId()
	})
	if !recorded {
		t.Fatal("expected a SIGNAL event in the run's event log")
	}
	if _, after, _ := s.getManifest(ctx, run.StartVersion); after != before {
		t.Fatal("expected the send to leave the manifest unwritten")
	}
}

// TestObjectPendingSignalIDsPerRun verifies a run lists only its own pending markers.
func TestObjectPendingSignalIDsPerRun(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	mine := startRun(t, s, "sig-mine")
	other := startRun(t, s, "sig-other")

	sig := recordTestSignal(t, s, mine, "advance")
	recordTestSignal(t, s, other, "advance")

	ids, err := s.pendingSignalIDs(context.Background(), mine.StartVersion)
	if err != nil {
		t.Fatalf("pending signals: %v", err)
	}
	if !slices.Equal(ids, []string{sig.GetId()}) {
		t.Fatalf("expected only the run's own signal, got %v", ids)
	}
}

// TestObjectCompleteConsumesSignals verifies a COMPLETE that consumes a signal records its ID on
// the manifest and deletes its marker.
func TestObjectCompleteConsumesSignals(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	run := startRun(t, s, "sig-consume")
	sig := recordTestSignal(t, s, run, "advance")

	completeConsuming(t, s, run, sig.GetId())

	if !objectGone(t, s, s.signalKey(run.StartVersion, sig.GetId())) {
		t.Fatal("expected the consumed signal's marker deleted")
	}
	if consumed := mustManifest(t, s, run.StartVersion).GetConsumedSignals(); !slices.Contains(consumed, sig.GetId()) {
		t.Fatalf("expected the manifest to record the consumed ID, got %v", consumed)
	}
}

// TestObjectConsumedSurvivesFailedMarkerDelete verifies a consumed signal whose marker delete
// fails stays listed but is carried, as consumed, into what a resume reads, so it is never offered
// again.
func TestObjectConsumedSurvivesFailedMarkerDelete(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()
	run := startRun(t, s, "sig-stuck")
	sig := recordTestSignal(t, s, run, "advance")
	h.s3.SetFailDelete(s.signalKey(run.StartVersion, sig.GetId()))

	completeConsuming(t, s, run, sig.GetId())

	ids, err := s.pendingSignalIDs(ctx, run.StartVersion)
	if err != nil {
		t.Fatalf("pending signals: %v", err)
	}
	if !slices.Contains(ids, sig.GetId()) {
		t.Fatalf("expected the marker whose delete failed still listed, got %v", ids)
	}
	resumed := manifestResource(run.StartVersion, mustManifest(t, s, run.StartVersion))
	if !slices.Contains(resumed.consumedSignals, sig.GetId()) {
		t.Fatalf("expected a resume to read the ID as consumed, got %v", resumed.consumedSignals)
	}
}

// TestArchiveReapsSignalMarkers verifies the archive reap removes markers of signals a finished
// run never read.
func TestArchiveReapsSignalMarkers(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()
	run := startRun(t, s, "sig-reap")
	sig := recordTestSignal(t, s, run, "pause")
	finishRun(t, s, run)
	ageRun(t, s, run.StartVersion, pastRetention)

	s.runArchive(ctx)

	if !objectGone(t, s, s.signalKey(run.StartVersion, sig.GetId())) {
		t.Fatal("expected the unread signal's marker reaped")
	}
}

// TestObjectUnreadSignalIDsSkipsConsumed verifies a consumed signal whose marker delete failed is
// not reported unread, while one never consumed is.
func TestObjectUnreadSignalIDsSkipsConsumed(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	run := startRun(t, s, "sig-unread")
	read := recordTestSignal(t, s, run, "advance")
	unread := recordTestSignal(t, s, run, "pause")
	h.s3.SetFailDelete(s.signalKey(run.StartVersion, read.GetId()))
	completeConsuming(t, s, run, read.GetId())

	ids, err := s.unreadSignalIDs(context.Background(), run.StartVersion)
	if err != nil {
		t.Fatalf("unread signals: %v", err)
	}
	if !slices.Equal(ids, []string{unread.GetId()}) {
		t.Fatalf("expected only the unconsumed signal, got %v", ids)
	}
}

// TestObjectCancelBeforeExecutionDiscardsSignals verifies a run canceled before it executes lists
// every signal sent to it as discarded on its FINISH.
func TestObjectCancelBeforeExecutionDiscardsSignals(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()
	run := startRun(t, s, "sig-canceled")
	sig := recordTestSignal(t, s, run, "advance")

	if err := s.cancelOwnedRun(ctx, run.StartVersion, &CancelError{Reason: "stop"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	he, err := s.History(ctx, run.StartVersion)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if got := he.GetLastEvent().GetDiscardedSignals(); !slices.Equal(got, []string{sig.GetId()}) {
		t.Fatalf("expected the FINISH to list %s discarded, got %v", sig.GetId(), got)
	}
}
