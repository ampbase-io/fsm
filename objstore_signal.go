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

func (s *objectStore) signalsPrefix() string {
	return s.key("signals") + "/"
}

// signalTarget returns the run a signal is addressed to, refusing a terminal or unknown run with
// ErrFsmNotFound.
func (s *objectStore) signalTarget(ctx context.Context, version ulid.ULID) (Run, error) {
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
// sender never writes the manifest, so the lease owner stays its only, fenced, writer. Both
// objects are keyed by the signal's ID, so a retried send rewrites them rather than adding more.
func (s *objectStore) recordSignal(ctx context.Context, run Run, sig *fsmv1.Signal) error {
	id, err := ulid.Parse(sig.GetId())
	if err != nil {
		return err
	}
	runVersion, err := run.StartVersion.MarshalText()
	if err != nil {
		return err
	}
	event, err := proto.Marshal(&fsmv1.StateEvent{
		Type:         fsmv1.EventType_EVENT_TYPE_SIGNAL,
		Id:           run.ID,
		ResourceType: run.TypeName,
		Action:       run.Action,
		RunVersion:   runVersion,
		Signal:       sig,
	})
	if err != nil {
		return err
	}
	marker, err := proto.Marshal(sig)
	if err != nil {
		return err
	}

	if err := s.putIdempotent(ctx, s.eventKey(run.ID, run.Action, run.StartVersion, id), event); err != nil {
		return err
	}
	if err := s.putIdempotent(ctx, s.signalKey(run.StartVersion, sig.GetId()), marker); err != nil {
		return err
	}

	if busIsLive(s.bus) {
		s.publishBroadcast(subjectSignal, fsmv1.RunEventKind_RUN_EVENT_KIND_SIGNAL, run.StartVersion, "")
	}
	return nil
}

// signalIDs returns the IDs of every pending signal marker, by run, oldest first, from one
// keys-only listing of the signals/ prefix. A marker whose signal was consumed but whose delete
// failed is listed too; the run's consumed IDs filter it.
func (s *objectStore) signalIDs(ctx context.Context) (map[ulid.ULID][]string, error) {
	keys, err := s.listKeys(ctx, s.signalsPrefix())
	if err != nil {
		return nil, err
	}

	ids := map[ulid.ULID][]string{}
	for _, key := range keys {
		rest := strings.TrimPrefix(key, s.signalsPrefix())
		runPart, id, ok := strings.Cut(rest, "/")
		if !ok {
			s.logger.WarnContext(ctx, "malformed signal marker key", "key", key)
			continue
		}
		version, err := ulid.Parse(runPart)
		if err != nil {
			s.logger.WarnContext(ctx, "malformed signal marker key", "error", err, "key", key)
			continue
		}
		ids[version] = append(ids[version], id)
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
