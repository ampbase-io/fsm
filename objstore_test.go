package fsm

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

// fakeS3 is a minimal path-style S3 implementation covering the operations objectStore uses:
// conditional PUT (If-None-Match: * and If-Match CAS), GET, DELETE, and ListObjectsV2. It can
// inject 409 Conflict responses to exercise the conditional-write retry path.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	revs    map[string]int

	// conflicts is the number of conditional PUTs to reject with 409 before accepting.
	conflicts int

	puts               int
	consistentReads    int
	nonConsistentReads int
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string][]byte{}, revs: map[string]int{}}
}

// etag returns the current ETag for a key; it changes on every accepted write so If-Match
// detects concurrent modification.
func (f *fakeS3) etag(key string) string {
	return fmt.Sprintf("%q", fmt.Sprintf("%s#%d", key, f.revs[key]))
}

func (f *fakeS3) handler(bucket string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()

		key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/"+bucket), "/")

		switch {
		case r.Method == http.MethodPut:
			f.puts++
			conditional := r.Header.Get("If-None-Match") == "*" || r.Header.Get("If-Match") != ""
			if conditional && f.conflicts > 0 {
				f.conflicts--
				w.WriteHeader(http.StatusConflict)
				return
			}
			if r.Header.Get("If-None-Match") == "*" {
				if _, ok := f.objects[key]; ok {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
			if im := r.Header.Get("If-Match"); im != "" {
				if _, ok := f.objects[key]; !ok || im != f.etag(key) {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
			body, _ := io.ReadAll(r.Body)
			f.objects[key] = body
			f.revs[key]++
			w.Header().Set("ETag", f.etag(key))
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodDelete:
			delete(f.objects, key)
			delete(f.revs, key)
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			if r.Header.Get("X-Tigris-Consistent") == "true" {
				f.consistentReads++
			} else {
				f.nonConsistentReads++
			}
			prefix := r.URL.Query().Get("prefix")
			keys := make([]string, 0, len(f.objects))
			for k := range f.objects {
				if strings.HasPrefix(k, prefix) {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)

			type contents struct {
				Key string `xml:"Key"`
			}
			result := struct {
				XMLName     xml.Name   `xml:"ListBucketResult"`
				Name        string     `xml:"Name"`
				IsTruncated bool       `xml:"IsTruncated"`
				KeyCount    int        `xml:"KeyCount"`
				Contents    []contents `xml:"Contents"`
			}{Name: bucket, KeyCount: len(keys)}
			for _, k := range keys {
				result.Contents = append(result.Contents, contents{Key: k})
			}
			w.Header().Set("Content-Type", "application/xml")
			xml.NewEncoder(w).Encode(result)

		case r.Method == http.MethodGet:
			if r.Header.Get("X-Tigris-Consistent") == "true" {
				f.consistentReads++
			} else {
				f.nonConsistentReads++
			}
			body, ok := f.objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", f.etag(key))
			w.Write(body)

		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	})
}

func newTestObjectStore(t *testing.T) (*objectStore, *fakeS3) {
	t.Helper()

	const bucket = "test-bucket"
	fake := newFakeS3()
	server := httptest.NewServer(fake.handler(bucket))
	t.Cleanup(server.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	store, err := newObjectStore(context.Background(), logrus.New(), &ObjectStorageConfig{
		Bucket:   bucket,
		Endpoint: server.URL,
		Region:   "auto",
	}, "node-test")
	if err != nil {
		t.Fatalf("failed to create object store: %v", err)
	}
	return store, fake
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
	if _, err := newObjectStore(context.Background(), logrus.New(), &ObjectStorageConfig{}, "node-test"); err == nil {
		t.Fatal("expected error for missing bucket")
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
	store, fake := newTestObjectStore(t)
	ctx := context.Background()

	fake.conflicts = 2
	if err := store.putIfAbsent(ctx, "fsm/test/conflict", []byte("a")); err != nil {
		t.Fatalf("expected write to succeed after 409 retries, got %v", err)
	}
	if fake.puts != 3 {
		t.Fatalf("expected 3 put attempts (2 conflicts + success), got %d", fake.puts)
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
	store, fake := newTestObjectStore(t)
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

	if fake.nonConsistentReads > 0 {
		t.Fatalf("expected all reads to set X-Tigris-Consistent, %d did not", fake.nonConsistentReads)
	}
	if fake.consistentReads == 0 {
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
