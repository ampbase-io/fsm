package fsm

import (
	"context"
	"errors"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// cancelBeforeExecState labels the terminal state recorded for a run canceled before it began
// executing: no transition was in flight, so there is no real state to attribute the cancel to,
// so the record names this label as where it stopped.
const cancelBeforeExecState = "canceled"

func (s *objectStore) cancelKey(version ulid.ULID) string {
	return s.key("cancel", version.String())
}

func (s *objectStore) cancelPrefix() string {
	return s.key("cancel") + "/"
}

// requestCancel records a cancel durably, then broadcasts it. The sentinel is write-once, so
// concurrent cancels of the same run collapse to the first cause — a 412 is success, the run is
// already being canceled. A terminal or missing run has nothing to cancel and reports
// ErrFsmNotFound. The owner reacts via its sweep or the broadcast; requestCancel never drives
// the run itself, so a cancel accepted on any node reaches whichever node owns the run.
func (s *objectStore) requestCancel(ctx context.Context, version ulid.ULID, cause error) error {
	if _, err := s.liveRun(ctx, version); err != nil {
		return err
	}

	switch err := s.putIfAbsent(ctx, s.cancelKey(version), []byte(cause.Error())); {
	case err == nil, errors.Is(err, errPreconditionFailed):
		// Written, or already recorded by a prior cancel — either way the intent is durable.
	default:
		return err
	}

	if busIsLive(s.bus) {
		s.publishControl(subjectCancel, fsmv1.RunEventKind_RUN_EVENT_KIND_CANCEL, version, cause.Error())
	}
	return nil
}

// pendingCancellations returns the cancel sentinels covering runs this node owns — keyed by run
// version, valued by cause — as ONE keys-only listing of the cancel/ prefix intersected with the
// owned set. The sentinel body (the cause) is fetched only on a match, so the check costs one
// list plus a GET per owned cancel, never a GET per run. Sentinels for runs another node owns
// are that node's concern; a sentinel for a terminal run is inert here (its run is unowned) and
// waits for retention to prune the cancel/ prefix.
func (s *objectStore) pendingCancellations(ctx context.Context) (map[ulid.ULID]error, error) {
	owned := s.snapshotOwned()
	if len(owned) == 0 {
		return nil, nil
	}
	keys, err := s.listKeys(ctx, s.cancelPrefix())
	if err != nil {
		return nil, err
	}

	cancels := map[ulid.ULID]error{}
	for _, key := range keys {
		version, err := versionFromKey(key)
		if err != nil {
			s.logger.WarnContext(ctx, "malformed cancel sentinel key", "error", err, "key", key)
			continue
		}
		if _, ok := owned[version]; !ok {
			continue
		}
		body, _, err := s.getObject(ctx, key)
		if err != nil {
			s.logger.ErrorContext(ctx, "failed to read cancel sentinel", "error", err, "key", key)
			continue
		}
		cancels[version] = &CancelError{Reason: string(body)}
	}
	return cancels, nil
}

// cancelOwnedRun drives a run this node owns but is not executing — pending, delayed, or queued
// when the cancel arrived — to a terminal canceled state, so its waiters resolve instead of
// polling to their deadline. It reuses the finish path: a single FINISH append records the
// cause, deletes the lock, writes history, drops the lease, and publishes done; no transitions
// run and no finalizers fire, because nothing has happened that needs finalizing. A run that has
// started — a takeover not yet dispatched — or gone terminal, or that this node no longer owns,
// is left alone.
func (s *objectStore) cancelOwnedRun(ctx context.Context, version ulid.ULID, cause error) error {
	if _, ok := s.ownedEpoch(version); !ok {
		return nil
	}
	manifest, _, err := s.getManifest(ctx, version)
	switch {
	case errors.Is(err, ErrFsmNotFound):
		return nil
	case err != nil:
		return err
	}
	// A run that has started is the run loop's to cancel, through its context, so its finalizers
	// run; once this node dispatches it, the sweep reaches it there. A terminal run has nothing left.
	if manifest.GetStatus() != fsmv1.RunState_RUN_STATE_PENDING {
		return nil
	}

	run := runFromManifest(version, manifest)
	run.fsmErr = RunErr{Err: halt(cause), State: cancelBeforeExecState}
	event := finishEvent(run, cancelBeforeExecState)
	// A run canceled before it ran read nothing, so every signal sent to it is discarded.
	event.DiscardedSignals, err = s.unreadSignalIDs(ctx, version)
	if err != nil {
		s.logger.WarnContext(ctx, "failed to list unread signals at cancel", "error", err, versionAttr(version))
	}
	_, err = s.Append(ctx, run, event)
	return err
}

// cancelPending settles a run no node has started — pending, or queued behind a full queue, where
// nothing applies the cancel sentinel until a slot frees. Ownership is the arbiter: it takes the
// same lease claim a claim pass takes, minus queue admission, so a cancel and an admission can
// never both win the run. Losing the claim is not a failure: the run is owned, or has started and
// belongs to whichever node takes it over, and either way the sentinel reaches it through its
// context; or it has gone terminal.
func (s *objectStore) cancelPending(ctx context.Context, version ulid.ULID, cause error) error {
	manifest, err := s.claimPending(ctx, version)
	switch {
	case errors.Is(err, errClaimLost), errors.Is(err, ErrFsmNotFound):
		return nil
	case err != nil:
		return err
	}

	if err := s.cancelOwnedRun(ctx, version, cause); err != nil {
		// The lease was taken to settle the run, not to execute it: held, the heartbeat would
		// extend it for as long as this node lives and no other node could take the run over.
		s.releaseLease(ctx, version, manifest.GetLeaseEpoch())
		return err
	}
	return nil
}
