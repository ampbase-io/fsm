package fsm

import (
	"bytes"
	"context"

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

// liveRun returns the run a command is addressed to from the in-memory index, refusing a terminal
// or unknown run with ErrFsmNotFound.
func (s *boltStore) liveRun(_ context.Context, version ulid.ULID) (Run, error) {
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
	event, err := signalEvent(run, sig)
	if err != nil {
		return err
	}
	eventBytes, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	entry, err := proto.Marshal(sig)
	if err != nil {
		return err
	}

	// EVENT Bucket
	// <resource_id>#<action>#<run_version>#<event_version>
	eventVersion := []byte(ulid.Make().String())
	eventKey := bytes.Join([][]byte{[]byte(run.ID), []byte(run.Action), event.GetRunVersion(), eventVersion}, keySeparator)
	return s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.Bucket(eventsBucket).Put(eventKey, eventBytes); err != nil {
			return err
		}
		return tx.Bucket(signalsBucket).Put(signalEntryKey(run.StartVersion, sig.GetId()), entry)
	})
}

// pendingSignalIDs returns the IDs of the run's pending signals.
func (s *boltStore) pendingSignalIDs(_ context.Context, version ulid.ULID) ([]string, error) {
	var ids []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		for _, k := range signalEntryKeys(tx.Bucket(signalsBucket), version) {
			ids = append(ids, string(bytes.TrimPrefix(k, signalEntryPrefix(version))))
		}
		return nil
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

// signalEntryKeys returns the keys of the run's pending entries, copied so they outlive a cursor.
func signalEntryKeys(b *bbolt.Bucket, version ulid.ULID) [][]byte {
	prefix := signalEntryPrefix(version)
	var keys [][]byte
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		keys = append(keys, bytes.Clone(k))
	}
	return keys
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
	for _, k := range signalEntryKeys(b, version) {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}
