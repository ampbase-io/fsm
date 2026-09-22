package fsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/ampbase-io/fsm/fsmtest/fake"
	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// newTestObjectStore builds an objectStore over a fresh fake S3, reached through the fake's client.
func newTestObjectStore(t *testing.T) (*objectStore, *fake.S3) {
	t.Helper()

	s3 := fake.NewS3(t)
	store, err := newObjectStore(context.Background(), slog.Default(), testInstruments(t), &ObjectStorageConfig{
		Bucket: s3.Bucket(),
		Client: s3.Client(),
	}, "node-test", nil, nil)
	if err != nil {
		t.Fatalf("failed to create object store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store, s3
}

// testULID builds a deterministic ULID whose timestamp controls ordering.
func testULID(t *testing.T, ms uint64) ulid.ULID {
	t.Helper()
	id, err := ulid.New(ms, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatalf("failed to build ULID: %v", err)
	}
	return id
}

func TestObjectStoreRequiresBucket(t *testing.T) {
	if _, err := newObjectStore(context.Background(), slog.Default(), testInstruments(t), &ObjectStorageConfig{}, "node-test", nil, nil); err == nil {
		t.Fatal("expected error for missing bucket")
	}
}

// TestObjectStoreUsesInjectedClient proves a consumer's client is used as is: the store reaches
// the fake through it with no Endpoint configured, so a client carrying the consumer's retryer
// and middleware carries every operation.
func TestObjectStoreUsesInjectedClient(t *testing.T) {
	ctx := context.Background()
	store, s3 := newTestObjectStore(t)

	if err := store.putIfAbsent(ctx, "fsm/injected/key", []byte("a")); err != nil {
		t.Fatalf("write through the injected client failed: %v", err)
	}
	if s3.Puts() == 0 {
		t.Fatal("the write did not reach the fake through the injected client")
	}
}

// TestObjectStoreBuildsClientFromEndpoint covers the other constructor path — no Client, so the
// store builds one from Endpoint and Region with the SDK's default credential chain. The only
// test that reads credentials from the environment, so the only one that cannot run in parallel.
func TestObjectStoreBuildsClientFromEndpoint(t *testing.T) {
	ctx := context.Background()
	s3 := fake.NewS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	store, err := newObjectStore(ctx, slog.Default(), testInstruments(t), &ObjectStorageConfig{
		Bucket:   s3.Bucket(),
		Endpoint: s3.URL(),
		Region:   "auto",
	}, "node-test", nil, nil)
	if err != nil {
		t.Fatalf("failed to create object store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.putIfAbsent(ctx, "fsm/endpoint/key", []byte("a")); err != nil {
		t.Fatalf("write through the built client failed: %v", err)
	}
	if s3.Puts() == 0 {
		t.Fatal("the write did not reach the fake through the built client")
	}
}

func TestPutIfAbsent(t *testing.T) {
	store, _ := newTestObjectStore(t)
	ctx := context.Background()

	if err := store.putIfAbsent(ctx, "fsm/test/key", []byte("a")); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if err := store.putIfAbsent(ctx, "fsm/test/key", []byte("b")); !errors.Is(err, errPreconditionFailed) {
		t.Fatalf("expected errPreconditionFailed for duplicate write, got %v", err)
	}
}

func TestPutIfAbsentRetriesConflict(t *testing.T) {
	store, s3 := newTestObjectStore(t)
	ctx := context.Background()

	s3.Conflicts = 2
	if err := store.putIfAbsent(ctx, "fsm/test/conflict", []byte("a")); err != nil {
		t.Fatalf("expected write to succeed after 409 retries, got %v", err)
	}
	if got := s3.Puts(); got != 3 {
		t.Fatalf("expected 3 put attempts (2 conflicts + success), got %d", got)
	}
}

func TestPutIfMatchCAS(t *testing.T) {
	store, _ := newTestObjectStore(t)
	ctx := context.Background()

	const key = "fsm/cas/key"
	if err := store.putIfAbsent(ctx, key, []byte("v1")); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}
	_, etag, err := store.getObject(ctx, key)
	if err != nil {
		t.Fatalf("failed to read object: %v", err)
	}

	if err := store.putIfMatch(ctx, key, []byte("v2"), etag); err != nil {
		t.Fatalf("CAS with current etag failed: %v", err)
	}
	if err := store.putIfMatch(ctx, key, []byte("v3"), etag); !errors.Is(err, errEtagMismatch) {
		t.Fatalf("expected errEtagMismatch for stale etag, got %v", err)
	}

	body, _, err := store.getObject(ctx, key)
	if err != nil {
		t.Fatalf("failed to read object: %v", err)
	}
	if string(body) != "v2" {
		t.Fatalf("stale CAS overwrote object: %q", body)
	}

	if err := store.deleteObject(ctx, key); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, _, err := store.getObject(ctx, key); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected ErrFsmNotFound after delete, got %v", err)
	}
	if err := store.deleteObject(ctx, key); err != nil {
		t.Fatalf("deleting a missing key should be a no-op, got %v", err)
	}
}

func TestGetObjectNotFound(t *testing.T) {
	store, _ := newTestObjectStore(t)

	if _, _, err := store.getObject(context.Background(), "fsm/missing"); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected ErrFsmNotFound, got %v", err)
	}
}

func TestEventRoundTrip(t *testing.T) {
	store, s3 := newTestObjectStore(t)
	ctx := context.Background()

	runVersion := testULID(t, 1)
	var eventVersions []ulid.ULID
	for i := range 3 {
		ev := testULID(t, uint64(10+i))
		eventVersions = append(eventVersions, ev)
		event := &fsmv1.StateEvent{
			Type:  fsmv1.EventType_EVENT_TYPE_COMPLETE,
			Id:    "app-1",
			State: fmt.Sprintf("state-%d", i),
		}
		if err := store.appendEvent(ctx, "app-1", "deploy", runVersion, ev, event); err != nil {
			t.Fatalf("failed to append event %d: %v", i, err)
		}
	}

	// A retry with the same event version is treated as already-applied.
	if err := store.appendEvent(ctx, "app-1", "deploy", runVersion, eventVersions[0], &fsmv1.StateEvent{}); err != nil {
		t.Fatalf("duplicate append should be a no-op, got %v", err)
	}

	events, err := store.listRunEvents(ctx, "app-1", "deploy", runVersion)
	if err != nil {
		t.Fatalf("failed to list events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	for i, event := range events {
		if got := event.GetState(); got != fmt.Sprintf("state-%d", i) {
			t.Fatalf("event %d out of order: got state %q", i, got)
		}
	}

	if got := s3.NonConsistentReads(); got > 0 {
		t.Fatalf("expected all reads to set X-Tigris-Consistent, %d did not", got)
	}
	if s3.ConsistentReads() == 0 {
		t.Fatal("expected consistent reads to be recorded")
	}
}

func TestKeyEscaping(t *testing.T) {
	store, _ := newTestObjectStore(t)

	runVersion := testULID(t, 1)
	eventVersion := testULID(t, 2)

	key := store.eventKey("a/b", "../evil", runVersion, eventVersion)
	if !strings.HasPrefix(key, "fsm/events/") {
		t.Fatalf("key escaped the events namespace: %q", key)
	}
	// prefix + events + escaped id + escaped action + run version + event version
	if got := len(strings.Split(key, "/")); got != 6 {
		t.Fatalf("caller-supplied values changed the key hierarchy: %q (%d segments)", key, got)
	}

	// Ids differing only around the separator must not share a listing prefix.
	p1 := store.eventPrefix("a/b", "deploy", runVersion)
	p2 := store.eventPrefix("a", "b/deploy", runVersion)
	if p1 == p2 {
		t.Fatalf("distinct id/action pairs mapped to the same prefix: %q", p1)
	}
}

func TestHistoryRoundTrip(t *testing.T) {
	store, _ := newTestObjectStore(t)
	ctx := context.Background()

	runVersion := testULID(t, 42)
	history := &fsmv1.HistoryEvent{
		LastEvent: &fsmv1.StateEvent{Type: fsmv1.EventType_EVENT_TYPE_FINISH, Id: "app-1"},
	}

	if err := store.writeHistory(ctx, runVersion, history); err != nil {
		t.Fatalf("failed to write history: %v", err)
	}
	if err := store.writeHistory(ctx, runVersion, history); err != nil {
		t.Fatalf("duplicate history write should be a no-op, got %v", err)
	}

	got, err := store.readHistory(ctx, runVersion)
	if err != nil {
		t.Fatalf("failed to read history: %v", err)
	}
	if got.GetLastEvent().GetId() != "app-1" {
		t.Fatalf("unexpected history event: %+v", got)
	}

	if _, err := store.readHistory(ctx, testULID(t, 43)); !errors.Is(err, ErrFsmNotFound) {
		t.Fatalf("expected ErrFsmNotFound for unknown run, got %v", err)
	}
}

func TestLinkParent(t *testing.T) {
	store, _ := newTestObjectStore(t)
	ctx := context.Background()

	if err := store.linkParent(ctx, ulid.ULID{}, ulid.Make()); err != nil {
		t.Fatalf("expected a zero parent to be a no-op, got %v", err)
	}

	parent, child := testULID(t, 1), testULID(t, 2)
	if err := store.linkParent(ctx, parent, child); err != nil {
		t.Fatalf("failed to link parent: %v", err)
	}
	children, err := store.listChildren(ctx, parent)
	if err != nil {
		t.Fatalf("failed to list children: %v", err)
	}
	if len(children) != 1 || children[0] != child {
		t.Fatalf("expected children [%s], got %v", child, children)
	}
}

func TestChildrenRoundTrip(t *testing.T) {
	store, _ := newTestObjectStore(t)
	ctx := context.Background()

	parent := testULID(t, 1)
	c1, c2 := testULID(t, 2), testULID(t, 3)

	for _, child := range []ulid.ULID{c1, c2} {
		if err := store.writeChild(ctx, parent, child); err != nil {
			t.Fatalf("failed to write child: %v", err)
		}
	}
	if err := store.writeChild(ctx, parent, c1); err != nil {
		t.Fatalf("duplicate child write should be a no-op, got %v", err)
	}

	children, err := store.listChildren(ctx, parent)
	if err != nil {
		t.Fatalf("failed to list children: %v", err)
	}
	if len(children) != 2 || children[0] != c1 || children[1] != c2 {
		t.Fatalf("expected children [%s %s], got %v", c1, c2, children)
	}
}
