package fsm

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// TestActiveRunsReportsALockWithoutAManifest covers the window a START opens between its lock
// (written first) and its manifest (written last): a competing Start is refused by the lock from
// the moment it lands, so a query asked which runs hold the resource must name it — with no
// state, since none is readable yet — rather than report the resource idle. The resume and admin
// paths came for the run's state and still skip it.
func TestActiveRunsReportsALockWithoutAManifest(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a, b := h.store("node-a", 10*time.Second), h.store("node-b", 10*time.Second)

	run := Run{ID: "held-1", StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq"}
	gate, arrived := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	var once sync.Once
	h.s3.SetPrePut(func(key string) {
		if key != a.manifestKey(run.StartVersion) {
			return
		}
		once.Do(func() { close(arrived) })
		<-gate
	})

	started := make(chan error, 1)
	go func() { started <- appendStarted(a, run) }()
	select {
	case <-arrived:
	case err := <-started:
		t.Fatalf("the start finished without writing a manifest: %v", err)
	}

	second := appendStarted(b, Run{ID: run.ID, StartVersion: ulid.Make(), Action: run.Action, TypeName: run.TypeName})
	already, refused := errors.AsType[*AlreadyRunningError](second)
	if !refused {
		t.Fatalf("a start against the held lock = %v, want AlreadyRunningError", second)
	}
	if already.Version != run.StartVersion {
		t.Fatalf("the refusal names %s, want the holder %s", already.Version, run.StartVersion)
	}

	key := ActiveKey{Action: run.Action, Version: run.StartVersion}
	active, err := b.ActiveRuns(ctx, run.TypeName, run.ID)
	if err != nil {
		t.Fatalf("failed to read the refused resource's active runs: %v", err)
	}
	if state, held := active[key]; !held || state != fsmv1.RunState_RUN_STATE_UNSPECIFIED {
		t.Fatalf("the held resource's active runs = %v, want %v held with no state yet", active, key)
	}

	resumable, err := b.Active(ctx, deployKey)
	if err != nil {
		t.Fatalf("failed to read the resumable runs: %v", err)
	}
	if len(resumable) != 0 {
		t.Fatalf("expected a run with no recorded state to be unresumable, got %v", resumable)
	}
	listed, err := b.ListActive(ctx)
	if err != nil {
		t.Fatalf("failed to list active runs: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("expected the admin listing to skip a run with no manifest, got %v", listed)
	}

	release()
	if err := <-started; err != nil {
		t.Fatalf("the start failed: %v", err)
	}
	active, err = b.ActiveRuns(ctx, run.TypeName, run.ID)
	if err != nil {
		t.Fatalf("failed to read the started resource's active runs: %v", err)
	}
	if state := active[key]; state != fsmv1.RunState_RUN_STATE_PENDING {
		t.Fatalf("once the manifest landed the resource's active runs = %v, want %v pending", active, key)
	}
}

// TestActiveRunsFailsOnAnUnreadableLock covers the other way a held lock resists resolution: the
// lock itself does not read. The query reports the failure rather than an empty set, which would
// read as an idle resource a Start could take; the resume path logs it and takes the run on a
// later pass.
func TestActiveRunsFailsOnAnUnreadableLock(t *testing.T) {
	h := newLeaseHarness(t)
	ctx := context.Background()
	a := h.store("node-a", 10*time.Second)

	lockKey := a.lockKey(Run{TypeName: "orderReq", ID: "unreadable-1", Action: "deploy"})
	if _, err := a.putIfAbsent(ctx, lockKey, []byte("not-a-run-version")); err != nil {
		t.Fatalf("failed to plant the lock: %v", err)
	}

	active, err := a.ActiveRuns(ctx, "orderReq", "unreadable-1")
	if err == nil {
		t.Fatalf("ActiveRuns over an unreadable lock = %v, want an error", active)
	}
	if !strings.Contains(err.Error(), lockKey) {
		t.Fatalf("the error does not name the lock it could not read: %v", err)
	}

	entry, err := a.resolveLock(ctx, lockKey, scanResumable)
	if entry != nil || err != nil {
		t.Fatalf("a resume scan of an unreadable lock = (%v, %v), want it skipped", entry, err)
	}
}

// TestResolveLockSkipsAReleasedLock covers the finish race: a lock listed by the scan and deleted
// before the scan read it is a run that completed, so nothing holds the resource and nothing is
// reported — a query must not turn that into an error.
func TestResolveLockSkipsAReleasedLock(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestObjectStore(t)

	gone := store.lockKey(Run{TypeName: "orderReq", ID: "released-1", Action: "deploy"})
	for _, scan := range []lockScan{scanResumable, scanHeld} {
		entry, err := store.resolveLock(ctx, gone, scan)
		if entry != nil || err != nil {
			t.Fatalf("scan %d of a released lock = (%v, %v), want it skipped", scan, entry, err)
		}
	}
}
