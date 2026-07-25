package fsm

import (
	"context"
	"errors"
	"testing"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// plantSentinel writes a cancel sentinel directly, bypassing requestCancel's manifest guard, so
// pendingCancellations can be exercised against arbitrary (owned, unowned, malformed) keys.
func plantSentinel(t *testing.T, s *objectStore, version ulid.ULID, cause string) {
	t.Helper()
	if err := s.putIfAbsent(context.Background(), s.cancelKey(version), []byte(cause)); err != nil {
		t.Fatalf("failed to plant cancel sentinel: %v", err)
	}
}

func TestRequestCancelWritesSentinel(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "rc-1")
	if err := s.requestCancel(ctx, run.StartVersion, errors.New("operator stop")); err != nil {
		t.Fatalf("requestCancel failed: %v", err)
	}

	body, _, err := s.getObject(ctx, s.cancelKey(run.StartVersion))
	if err != nil {
		t.Fatalf("expected a cancel sentinel, got %v", err)
	}
	if string(body) != "operator stop" {
		t.Fatalf("expected the sentinel body to carry the cause, got %q", body)
	}
}

func TestRequestCancelIsIdempotentFirstCauseWins(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "rc-2")
	if err := s.requestCancel(ctx, run.StartVersion, errors.New("first")); err != nil {
		t.Fatalf("first requestCancel failed: %v", err)
	}
	// A second cancel of a still-active run observes the write-once sentinel (412) and reports
	// success — the run is already being canceled.
	if err := s.requestCancel(ctx, run.StartVersion, errors.New("second")); err != nil {
		t.Fatalf("second requestCancel should be idempotent success, got %v", err)
	}

	body, _, err := s.getObject(ctx, s.cancelKey(run.StartVersion))
	if err != nil {
		t.Fatalf("expected a cancel sentinel, got %v", err)
	}
	if string(body) != "first" {
		t.Fatalf("expected the first cause to win, got %q", body)
	}
}

func TestRequestCancelTerminalRunNotFound(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "rc-3")
	finishRun(t, s, run)

	if err := s.requestCancel(ctx, run.StartVersion, errors.New("too late")); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected ErrFsmNotFound for a terminal run, got %v", err)
	}
	if _, _, err := s.getObject(ctx, s.cancelKey(run.StartVersion)); !errors.Is(err, ErrFsmNotFound) {
		t.Fatal("a terminal run must not get a cancel sentinel")
	}
}

func TestRequestCancelUnknownRunNotFound(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)

	if err := s.requestCancel(context.Background(), ulid.Make(), errors.New("nobody home")); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected ErrFsmNotFound for an unknown run, got %v", err)
	}
}

func TestPendingCancellationsReturnsOnlyOwned(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "pc-1") // owned by node-a
	other := ulid.Make()          // not owned by this store
	plantSentinel(t, s, run.StartVersion, "owned-cause")
	plantSentinel(t, s, other, "other-cause")

	cancels, err := s.pendingCancellations(ctx)
	if err != nil {
		t.Fatalf("pendingCancellations failed: %v", err)
	}
	if len(cancels) != 1 {
		t.Fatalf("expected exactly the owned run's cancel, got %d", len(cancels))
	}
	if cause, ok := cancels[run.StartVersion]; !ok || cause.Error() != "owned-cause" {
		t.Fatalf("expected the owned run's cause, got %v (present=%v)", cause, ok)
	}
	if _, ok := cancels[other]; ok {
		t.Fatal("a sentinel for a run this node does not own must be skipped")
	}
}

func TestPendingCancellationsEmptyWhenNothingOwned(t *testing.T) {
	h := newLeaseHarness(t)
	// This store starts no runs, so it owns nothing; a sentinel exists all the same.
	s := h.store("node-b", 10*time.Second)
	plantSentinel(t, s, ulid.Make(), "someone-elses")

	cancels, err := s.pendingCancellations(context.Background())
	if err != nil {
		t.Fatalf("pendingCancellations failed: %v", err)
	}
	if len(cancels) != 0 {
		t.Fatalf("a store owning no runs must return no cancellations, got %d", len(cancels))
	}
}

func TestPendingCancellationsSkipsMalformedKey(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "pc-mal")
	plantSentinel(t, s, run.StartVersion, "good")
	// A stray non-ULID key under the cancel prefix must be skipped, not fail the whole sweep.
	if err := s.putIfAbsent(ctx, s.cancelPrefix()+"not-a-ulid", []byte("garbage")); err != nil {
		t.Fatalf("failed to plant malformed key: %v", err)
	}

	cancels, err := s.pendingCancellations(ctx)
	if err != nil {
		t.Fatalf("pendingCancellations failed: %v", err)
	}
	if len(cancels) != 1 || cancels[run.StartVersion] == nil {
		t.Fatalf("expected only the well-formed owned cancel, got %v", cancels)
	}
}

func TestCancelOwnedRunDrivesTerminal(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "cor-1") // owned, PENDING — never executed a transition
	if err := s.cancelOwnedRun(ctx, run.StartVersion, errors.New("cancel before run")); err != nil {
		t.Fatalf("cancelOwnedRun failed: %v", err)
	}

	m := mustManifest(t, s, run.StartVersion)
	if !manifestTerminal(m) {
		t.Fatalf("expected a terminal manifest, got status %v", m.GetStatus())
	}
	if m.GetError() != "cancel before run" || m.GetErrorState() != cancelBeforeExecState {
		t.Fatalf("expected the cause recorded, got error=%q state=%q", m.GetError(), m.GetErrorState())
	}
	if _, _, err := s.getObject(ctx, runLockKey(s, run)); !errors.Is(err, ErrFsmNotFound) {
		t.Fatal("expected the resource lock deleted")
	}
	if _, err := s.History(ctx, run.StartVersion); err != nil {
		t.Fatalf("expected a history record, got %v", err)
	}
	if _, ok := s.ownedEpoch(run.StartVersion); ok {
		t.Fatal("expected the lease dropped after driving the run terminal")
	}
}

func TestCancelOwnedRunNotOwnedNoop(t *testing.T) {
	h := newLeaseHarness(t)
	a := h.store("node-a", 10*time.Second)
	b := h.store("node-b", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, a, "cor-2") // owned by node-a

	// node-b does not hold the lease, so it must not drive a run it does not own.
	if err := b.cancelOwnedRun(ctx, run.StartVersion, errors.New("not yours")); err != nil {
		t.Fatalf("cancelOwnedRun on an unowned run should be a no-op, got %v", err)
	}
	m := mustManifest(t, b, run.StartVersion)
	if manifestTerminal(m) {
		t.Fatal("a store drove a run it does not own to terminal")
	}
	if m.GetOwnerNode() != "node-a" {
		t.Fatalf("expected the run still owned by node-a, got %q", m.GetOwnerNode())
	}
}

func TestCancelOwnedRunTerminalNoop(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)
	ctx := context.Background()

	run := startRun(t, s, "cor-3")
	// Force the manifest terminal while this store still holds the lease locally — the race the
	// terminal guard defends against. cancelOwnedRun must not run a second finish.
	if _, err := s.casManifest(ctx, run.StartVersion, func(m *fsmv1.RunManifest) error {
		m.Status = fsmv1.RunState_RUN_STATE_COMPLETE
		return nil
	}); err != nil {
		t.Fatalf("failed to force terminal manifest: %v", err)
	}

	if err := s.cancelOwnedRun(ctx, run.StartVersion, errors.New("already done")); err != nil {
		t.Fatalf("cancelOwnedRun on a terminal run should be a no-op, got %v", err)
	}
	// The guard returns before the finish path, so the lease is left as-is (not dropped).
	if _, ok := s.ownedEpoch(run.StartVersion); !ok {
		t.Fatal("cancelOwnedRun ran the finish path on an already-terminal run")
	}
}
