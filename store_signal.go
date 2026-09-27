package fsm

import (
	"bytes"
	"context"
	"fmt"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// signalsBucket holds a run's pending signals, <run_version>#<signal_id>, until the COMPLETE that
// consumes them or the run's FINISH deletes them.
var signalsBucket = []byte("SIGNALS")

func signalEntryKey(version ulid.ULID, id string) []byte {
	return bytes.Join([][]byte{[]byte(version.String()), []byte(id)}, keySeparator)
}

func signalEntryPrefix(version ulid.ULID) []byte {
	return append([]byte(version.String()), keySeparator...)
}

// signalTarget returns the run a signal is addressed to from the in-memory index, refusing a
// terminal or unknown run with ErrFsmNotFound.
func (s *boltStore) signalTarget(_ context.Context, version ulid.ULID) (Run, error) {
	txn := s.memDB.Txn(false)
	defer txn.Abort()
	item, err := txn.First(fsmTable, idIndex, version.String())
	if err != nil {
		return Run{}, err
	}
	rs, ok := item.(runSnapshot)
	if !ok || rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
		return Run{}, ErrFsmNotFound
	}
	return rs.Run, nil
}

// recordSignal writes the SIGNAL event and the pending entry in one transaction.
func (s *boltStore) recordSignal(_ context.Context, run Run, sig *fsmv1.Signal) error {
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
	entry, err := proto.Marshal(sig)
	if err != nil {
		return err
	}

	// EVENT Bucket, keyed by the signal's ID as the event version:
	// <resource_id>#<action>#<run_version>#<signal_id>
	eventKey := bytes.Join([][]byte{[]byte(run.ID), []byte(run.Action), runVersion, []byte(sig.GetId())}, keySeparator)
	return s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.Bucket(eventsBucket).Put(eventKey, event); err != nil {
			return err
		}
		return tx.Bucket(signalsBucket).Put(signalEntryKey(run.StartVersion, sig.GetId()), entry)
	})
}

// signalIDs returns the IDs of every pending signal, by run, oldest first.
func (s *boltStore) signalIDs(context.Context) (map[ulid.ULID][]string, error) {
	ids := map[ulid.ULID][]string{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(signalsBucket).ForEach(func(k, _ []byte) error {
			runPart, id, ok := bytes.Cut(k, keySeparator)
			if !ok {
				return fmt.Errorf("malformed signal entry key %q", k)
			}
			version, err := ulid.Parse(string(runPart))
			if err != nil {
				return err
			}
			ids[version] = append(ids[version], string(id))
			return nil
		})
	})
	return ids, err
}

// signal reads one pending signal.
func (s *boltStore) signal(_ context.Context, version ulid.ULID, id string) (*fsmv1.Signal, error) {
	var sig fsmv1.Signal
	err := s.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(signalsBucket).Get(signalEntryKey(version, id))
		if v == nil {
			return ErrFsmNotFound
		}
		return proto.Unmarshal(v, &sig)
	})
	if err != nil {
		return nil, err
	}
	return &sig, nil
}

// consumeSignals deletes, within tx, the pending entries of the signals a COMPLETE consumed.
func consumeSignals(tx *bbolt.Tx, version ulid.ULID, ids []string) error {
	b := tx.Bucket(signalsBucket)
	for _, id := range ids {
		if err := b.Delete(signalEntryKey(version, id)); err != nil {
			return err
		}
	}
	return nil
}

// discardSignals deletes, within tx, every pending entry of a finished run.
func discardSignals(tx *bbolt.Tx, version ulid.ULID) error {
	b := tx.Bucket(signalsBucket)
	prefix := signalEntryPrefix(version)
	var keys [][]byte
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		keys = append(keys, bytes.Clone(k))
	}
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}
