package fsm

import (
	"context"
	"strings"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

func (s *objectStore) signalKey(version ulid.ULID, id string) string {
	return s.key("signals", version.String(), id)
}

func (s *objectStore) signalPrefix(version ulid.ULID) string {
	return s.key("signals", version.String()) + "/"
}

// liveRun returns the run a command is addressed to, refusing a terminal or unknown run with
// ErrFsmNotFound.
func (s *objectStore) liveRun(ctx context.Context, version ulid.ULID) (Run, error) {
	manifest, _, err := s.getManifest(ctx, version)
	if err != nil {
		return Run{}, err
	}
	if manifestTerminal(manifest) {
		return Run{}, ErrFsmNotFound
	}
	return runFromManifest(version, manifest), nil
}

// recordSignal writes the signal's SIGNAL event, then its pending marker, then broadcasts it. The
// sender never writes the manifest, so the lease owner stays its only, fenced, writer.
func (s *objectStore) recordSignal(ctx context.Context, run Run, sig *fsmv1.Signal) error {
	event, err := signalEvent(run, sig)
	if err != nil {
		return err
	}
	marker, err := proto.Marshal(sig)
	if err != nil {
		return err
	}

	if err := s.appendEvent(ctx, run.ID, run.Action, run.StartVersion, ulid.Make(), event); err != nil {
		return err
	}
	if err := s.putIdempotent(ctx, s.signalKey(run.StartVersion, sig.GetId()), marker); err != nil {
		return err
	}

	if busIsLive(s.bus) {
		s.publishControl(subjectSignal, fsmv1.RunEventKind_RUN_EVENT_KIND_SIGNAL, run.StartVersion, "")
	}
	return nil
}

// pendingSignalIDs returns the IDs of the run's pending signal markers, from a keys-only listing
// of its signals/<version>/ prefix. A marker whose signal was consumed but whose delete failed is
// listed too; the run's consumed IDs filter it.
func (s *objectStore) pendingSignalIDs(ctx context.Context, version ulid.ULID) ([]string, error) {
	keys, err := s.listKeys(ctx, s.signalPrefix(version))
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(keys))
	for _, key := range keys {
		ids = append(ids, strings.TrimPrefix(key, s.signalPrefix(version)))
	}
	return ids, nil
}

// signal reads one pending signal's marker.
func (s *objectStore) signal(ctx context.Context, version ulid.ULID, id string) (*fsmv1.Signal, error) {
	body, _, err := s.getObject(ctx, s.signalKey(version, id))
	if err != nil {
		return nil, err
	}
	var sig fsmv1.Signal
	if err := proto.Unmarshal(body, &sig); err != nil {
		return nil, err
	}
	return &sig, nil
}

// deleteConsumedSignals removes the markers of signals a COMPLETE consumed. It runs after the
// manifest records them consumed, so a failed delete only leaves a marker the run never offers
// again, which retention removes with the run.
func (s *objectStore) deleteConsumedSignals(ctx context.Context, version ulid.ULID, ids []string) {
	for _, id := range ids {
		if err := s.deleteObject(ctx, s.signalKey(version, id)); err != nil {
			s.logger.ErrorContext(ctx, "failed to delete consumed signal marker", "error", err, versionAttr(version), "signal", id)
		}
	}
}
