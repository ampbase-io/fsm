package fsm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/oklog/ulid/v2"
)

// The live tests run against a real S3-compatible server rather than the in-process fake, where
// every operation answers instantly: on a real store a START's lock lands milliseconds before
// its manifest, and a concurrent Start is refused inside that gap. They are skipped unless
// FSM_TEST_S3_ENDPOINT names a server (FSM_TEST_S3_BUCKET overrides the bucket, created on
// demand), and they are the only tests that read credentials from the environment, so they do
// not run in parallel:
//
//	container run -d --name fsm-s3 -p 127.0.0.1:9000:9000 \
//	  -e RUSTFS_ACCESS_KEY=minioadmin -e RUSTFS_SECRET_KEY=minioadmin \
//	  rustfs/rustfs:1.0.0 server /data
//	FSM_TEST_S3_ENDPOINT=http://127.0.0.1:9000 AWS_ACCESS_KEY_ID=minioadmin \
//	  AWS_SECRET_ACCESS_KEY=minioadmin go test -race -run TestLive .
const (
	liveEndpointEnv = "FSM_TEST_S3_ENDPOINT"
	liveBucketEnv   = "FSM_TEST_S3_BUCKET"
)

// liveBucket is the bucket the live tests use when FSM_TEST_S3_BUCKET names none.
const liveBucket = "fsm-test"

// liveStores builds one objectStore per node id over a bucket on the configured server, all
// under a key prefix of this test's own so a shared bucket (a CI container, a developer's
// local server) carries no state between tests. http, when non-nil, replaces the first store's
// HTTP client, which is how a test holds one request of one node back.
func liveStores(t *testing.T, httpClient aws.HTTPClient, nodes ...string) []*objectStore {
	t.Helper()

	endpoint := os.Getenv(liveEndpointEnv)
	if endpoint == "" {
		t.Skipf("set %s to an S3-compatible endpoint to run the live tests", liveEndpointEnv)
	}
	keyID, secret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if keyID == "" || secret == "" {
		t.Skipf("%s is set but AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are not", liveEndpointEnv)
	}
	bucket := os.Getenv(liveBucketEnv)
	if bucket == "" {
		bucket = liveBucket
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	ctx := context.Background()
	client := func(hc aws.HTTPClient) *s3.Client {
		return s3.New(s3.Options{
			Region:       region,
			BaseEndpoint: aws.String(endpoint),
			UsePathStyle: true,
			Credentials:  credentials.NewStaticCredentialsProvider(keyID, secret, ""),
			HTTPClient:   hc,
		})
	}
	// An existing bucket answers BucketAlreadyOwnedByYou, which is what every run after the
	// first sees; a genuinely unreachable server fails the first store's writes instead, with
	// the underlying error.
	var owned *types.BucketAlreadyOwnedByYou
	if _, err := client(nil).CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket}); err != nil && !errors.As(err, &owned) {
		t.Logf("create bucket %s: %v", bucket, err)
	}

	prefix := "live-" + ulid.Make().String() + "/"
	stores := make([]*objectStore, 0, len(nodes))
	for i, node := range nodes {
		c := client(nil)
		if i == 0 && httpClient != nil {
			c = client(httpClient)
		}
		store, err := newObjectStore(ctx, slog.New(slog.NewTextHandler(os.Stderr, nil)).With("node", node),
			testInstruments(t), &ObjectStorageConfig{Bucket: bucket, Client: c, Prefix: prefix}, node, nil, nil)
		if err != nil {
			t.Fatalf("failed to create the live object store: %v", err)
		}
		t.Cleanup(func() { store.Close() })
		stores = append(stores, store)
	}
	t.Cleanup(func() {
		if err := stores[len(stores)-1].deletePrefix(context.Background(), prefix); err != nil {
			t.Logf("failed to clean up %s: %v", prefix, err)
		}
	})
	return stores
}

// raceResult is what one node got out of a Start race: the refusal, if it lost, and the
// resource's active runs as of that moment.
type raceResult struct {
	already *AlreadyRunningError

	active    ActiveSet
	activeErr error
}

// startRace runs one Start per store for the same resource, each store racing with its own run
// version. A refused node reads the resource's active runs from inside its own goroutine, while
// the winner is still writing: a refusal answered after the winner finished is not the window
// under test.
func startRace(ctx context.Context, stores []*objectStore, id string) ([]raceResult, error) {
	results := make([]raceResult, len(stores))
	starts := make([]error, len(stores))
	var wg sync.WaitGroup
	for i, s := range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := appendStarted(s, Run{ID: id, StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq"})
			already, refused := errors.AsType[*AlreadyRunningError](err)
			if !refused {
				starts[i] = err
				return
			}
			results[i].already = already
			results[i].active, results[i].activeErr = s.ActiveRuns(ctx, "orderReq", id)
		}()
	}
	wg.Wait()
	return results, errors.Join(starts...)
}

// TestLiveConcurrentStartsKeepTheResourceActive is the reported shape: two nodes race Start on
// one resource and the loser, the moment it is refused, asks which runs are active. A refusal
// means a lock exists that a Start cannot take, so the answer can never be the empty set,
// whether or not the winner's manifest has landed by then. How often the refusal falls inside
// that window is a property of the store and of how many writes a START makes — it logs the
// split rather than asserting one; TestLiveStartHoldsTheResourceBeforeItsManifest is what pins
// the window itself.
func TestLiveConcurrentStartsKeepTheResourceActive(t *testing.T) {
	ctx := context.Background()
	stores := liveStores(t, nil, "node-a", "node-b")

	const races = 20
	states := map[fsmv1.RunState]int{}
	for i := range races {
		id := fmt.Sprintf("race-%02d", i)
		results, err := startRace(ctx, stores, id)
		if err != nil {
			t.Fatalf("race %d: a start failed with %v, want success or AlreadyRunningError", i, err)
		}
		for n, r := range results {
			if r.already == nil {
				continue
			}
			if r.activeErr != nil {
				t.Fatalf("race %d: reading the refused resource's active runs: %v", i, r.activeErr)
			}
			state, held := r.active[ActiveKey{Action: "deploy", Version: r.already.Version}]
			if !held {
				t.Fatalf("race %d: node %d was refused naming %s, but the resource's active runs are %v",
					i, n, r.already.Version, r.active)
			}
			states[state]++
		}
	}
	t.Logf("%d races, refusals by the state the held run reported: %v", races, states)
}

// TestLiveStartHoldsTheResourceBeforeItsManifest pins the same invariant without racing for the
// window: the winner's manifest PUT is held back, so the refusal is answered while the lock is
// all there is of the run. The lock is reported with no state — not omitted — and once the
// manifest lands the same run reads PENDING.
func TestLiveStartHoldsTheResourceBeforeItsManifest(t *testing.T) {
	ctx := context.Background()
	held := newHeldPut("/runs/")
	stores := liveStores(t, held, "node-a", "node-b")
	winner, loser := stores[0], stores[1]
	defer held.releaseAll()

	run := Run{ID: "held-1", StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq"}
	started := make(chan error, 1)
	go func() { started <- appendStarted(winner, run) }()
	select {
	case <-held.arrived:
	case err := <-started:
		t.Fatalf("the winner's start finished without writing a manifest: %v", err)
	}

	err := appendStarted(loser, Run{ID: run.ID, StartVersion: ulid.Make(), Action: run.Action, TypeName: run.TypeName})
	already, refused := errors.AsType[*AlreadyRunningError](err)
	if !refused {
		t.Fatalf("a start against the held lock = %v, want AlreadyRunningError", err)
	}
	if already.Version != run.StartVersion {
		t.Fatalf("the refusal names %s, want the holder %s", already.Version, run.StartVersion)
	}

	key := ActiveKey{Action: run.Action, Version: run.StartVersion}
	active, err := loser.ActiveRuns(ctx, run.TypeName, run.ID)
	if err != nil {
		t.Fatalf("reading the refused resource's active runs: %v", err)
	}
	if state, ok := active[key]; !ok || state != fsmv1.RunState_RUN_STATE_UNSPECIFIED {
		t.Fatalf("the held resource's active runs = %v, want %v held with no state yet", active, key)
	}

	held.releaseAll()
	if err := <-started; err != nil {
		t.Fatalf("the winner's start failed: %v", err)
	}
	active, err = loser.ActiveRuns(ctx, run.TypeName, run.ID)
	if err != nil {
		t.Fatalf("reading the started resource's active runs: %v", err)
	}
	if state := active[key]; state != fsmv1.RunState_RUN_STATE_PENDING {
		t.Fatalf("once the manifest landed the resource's active runs = %v, want %v pending", active, key)
	}
}

// TestLiveRunCASesAgainstRealETags runs a whole run against the real store, which is the only
// place the cached compare-and-swap target can be checked for what it is: an ETag the provider
// minted on a PUT. The fake always returns one, so a provider that did not would show up here as
// a cache that never warms — safe, since the CAS then reads first, but every checkpoint would pay
// for it. Each write after the START also proves an If-Match against a real ETag is accepted.
func TestLiveRunCASesAgainstRealETags(t *testing.T) {
	ctx := context.Background()
	s := liveStores(t, nil, "node-a")[0]

	run := Run{ID: "etag-1", StartVersion: ulid.Make(), Action: "deploy", TypeName: "orderReq"}
	startCostRun(t, s, run)
	if _, etag, cached := s.cachedManifest(run.StartVersion); !cached {
		t.Fatal("the START's manifest write returned no ETag, so no checkpoint can skip its read")
	} else if !strings.HasPrefix(etag, `"`) {
		t.Fatalf("the cached CAS target is not a quoted ETag: %q", etag)
	}

	mustSetRunning(t, s, run)
	for _, state := range []string{"one", "two", "three"} {
		mustComplete(t, s, run, state)
	}
	mustFinish(t, s, run)

	history, err := s.History(ctx, run.StartVersion)
	if err != nil {
		t.Fatalf("failed to read the finished run's history: %v", err)
	}
	if got := history.GetLastEvent().GetType(); got != fsmv1.EventType_EVENT_TYPE_FINISH {
		t.Fatalf("the history record ends in %v, want FINISH", got)
	}
	if got := history.GetActiveEvent().GetTransitions(); len(got) != 4 {
		t.Fatalf("the history record carries %v, want the run's four states", got)
	}
}

// heldPut is an HTTP client that holds back every PUT whose path contains match until released,
// closing arrived as the first one is held. It opens a chosen window inside a multi-object write
// against a real store, where the fake's prePut hook cannot reach.
type heldPut struct {
	inner http.Client

	match   string
	arrived chan struct{}
	release chan struct{}

	arrivedOnce sync.Once
	releaseOnce sync.Once
}

func newHeldPut(match string) *heldPut {
	return &heldPut{match: match, arrived: make(chan struct{}), release: make(chan struct{})}
}

func (h *heldPut) Do(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPut && strings.Contains(req.URL.Path, h.match) {
		h.arrivedOnce.Do(func() { close(h.arrived) })
		<-h.release
	}
	return h.inner.Do(req)
}

// releaseAll lets every held and future PUT through. Safe to call twice, so a test can release
// in its body and still defer it against a failure before that line.
func (h *heldPut) releaseAll() {
	h.releaseOnce.Do(func() { close(h.release) })
}
