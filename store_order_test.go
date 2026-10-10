package fsm

import (
	"bytes"
	"context"
	"slices"
	"testing"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"
	"github.com/oklog/ulid/v2"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// TestBoltEventVersions_SortInRecordOrder: a run's events, each recorded after
// a mint on the next millisecond took the store's minter — the interleaving
// under which ulid.Make draws fresh entropy under the stale millisecond — come
// back in the order they were recorded. Resume folds a run's events from its
// START in key order and the archive moves START..FINISH, so a COMPLETE sorting
// before START would re-run its transition and a SIGNAL sorting after FINISH
// would be left behind.
func TestBoltEventVersions_SortInRecordOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for round := range 16 {
		s := boltStoreOf(t)
		ms := []uint64{1000, 1001, 1000, 1002, 1000, 1003, 1000}
		s.versions = newOrderedIDs(func() uint64 {
			next := ms[0]
			ms = ms[1:]
			return next
		})
		foreign := func() { _ = s.versions.mint() }

		run := startBoltRun(t, s, "order")
		foreign()
		if _, err := s.Append(ctx, run, &fsmv1.StateEvent{
			Type: fsmv1.EventType_EVENT_TYPE_COMPLETE, Id: run.ID, Action: run.Action, State: "created",
		}); err != nil {
			t.Fatalf("round %d: complete: %v", round, err)
		}
		foreign()
		recordBoltSignal(t, s, run, "nudge")
		foreign()
		if _, err := s.Append(ctx, run, &fsmv1.StateEvent{
			Type: fsmv1.EventType_EVENT_TYPE_FINISH, Id: run.ID, Action: run.Action, State: "done",
		}); err != nil {
			t.Fatalf("round %d: finish: %v", round, err)
		}

		want := []fsmv1.EventType{
			fsmv1.EventType_EVENT_TYPE_START,
			fsmv1.EventType_EVENT_TYPE_COMPLETE,
			fsmv1.EventType_EVENT_TYPE_SIGNAL,
			fsmv1.EventType_EVENT_TYPE_FINISH,
		}
		got, versions := eventsInKeyOrder(t, s, run)
		if !slices.Equal(got, want) {
			t.Fatalf("round %d: events in key order are %v, want %v", round, got, want)
		}
		for i, v := range versions {
			if v.Time() > 1003 {
				t.Fatalf("round %d: %s's version %s carries ms %d, not the store minter's clock", round, got[i], v, v.Time())
			}
		}
	}
}

// eventsInKeyOrder lists run's event types and versions as bolt's cursor
// returns them.
func eventsInKeyOrder(t *testing.T, s *boltStore, run Run) ([]fsmv1.EventType, []ulid.ULID) {
	t.Helper()
	runVersion, err := run.StartVersion.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	prefix := bytes.Join([][]byte{[]byte(run.ID), []byte(run.Action), runVersion, emptyPrefix}, keySeparator)
	var (
		types    []fsmv1.EventType
		versions []ulid.ULID
	)
	err = s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(eventsBucket).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var ev fsmv1.StateEvent
			if err := proto.Unmarshal(v, &ev); err != nil {
				return err
			}
			version, err := ulid.ParseStrict(string(k[len(prefix):]))
			if err != nil {
				return err
			}
			types = append(types, ev.GetType())
			versions = append(versions, version)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return types, versions
}
