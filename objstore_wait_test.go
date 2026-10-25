package fsm

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

// TestWaitDuringStartWindow covers the other side of a START's lock-before-manifest ordering: a
// run exists from the moment its lock lands, but nothing keyed by its version does until the
// manifest is written. A wait that begins in that window — on a version read from a listing, a
// run_after dependency, or Wait by id — must wait for the run rather than be told it does not
// exist. Past startVisibilityWindow the version is unknown, which TestWaitUnknownRun pins.
func TestWaitDuringStartWindow(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	owner, waiter := h.store("node-a", 10*time.Second), h.store("node-b", 10*time.Second)

	run := Run{ID: "waited-1", StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq"}
	gate, arrived := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	var once sync.Once
	h.s3.SetPrePut(func(key string) {
		if key != owner.manifestKey(run.StartVersion) {
			return
		}
		once.Do(func() { close(arrived) })
		<-gate
	})

	started := make(chan error, 1)
	go func() { started <- appendStarted(owner, run) }()
	select {
	case <-arrived:
	case err := <-started:
		t.Fatalf("the start finished without writing a manifest: %v", err)
	}

	waited := make(chan error, 1)
	go func() { waited <- waiter.WaitRun(ctx, run.StartVersion) }()
	select {
	case err := <-waited:
		t.Fatalf("the wait answered %v while the run's START was still writing its manifest", err)
	case <-time.After(300 * time.Millisecond):
	}

	release()
	if err := <-started; err != nil {
		t.Fatalf("the start failed: %v", err)
	}
	finishRun(t, owner, run)

	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("the wait returned %v, want the run's outcome", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the wait never saw the finished run")
	}
}

// TestStartInFlight pins the window's edges: the age of the version is the only signal, so a
// version minted now is one a START may still be writing and the zero version — what a Wait by
// id resolves an unknown id to — is not.
func TestStartInFlight(t *testing.T) {
	if !startInFlight(ulid.Make()) {
		t.Fatal("a version minted now should be treated as a START in flight")
	}
	if startInFlight(ulid.ULID{}) {
		t.Fatal("the zero version should never be treated as a START in flight")
	}
	old := testULID(t, uint64(time.Now().Add(-time.Minute).UnixMilli()))
	if startInFlight(old) {
		t.Fatalf("a version minted a minute ago should be past the window")
	}
}

// TestWaitUnknownVersionStaysPrompt guards the window's cost on the error path: a version no
// START ever minted is answered NotFound, not held for the window.
func TestWaitUnknownVersionStaysPrompt(t *testing.T) {
	h := newLeaseHarness(t)
	s := h.store("node-a", 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	began := time.Now()
	if err := s.WaitRun(ctx, testULID(t, uint64(time.Now().Add(-time.Minute).UnixMilli()))); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("a wait on a version past the window = %v, want ErrFsmNotFound", err)
	}
	if took := time.Since(began); took > startVisibilityWindow {
		t.Fatalf("the wait took %s to report an unknown version", took)
	}
}
