// Package fake holds in-process doubles for the two external systems fsm talks to: an S3-compatible
// object store and an event bus. Both are what the library's own tests run against, and they let
// a consumer test takeover, cancel and restart against the object storage backend with no bucket.
//
// The package imports nothing from fsm, so fsm's own tests can use it too.
package fake

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// bucket is the one bucket the fake serves.
const bucket = "test-bucket"

// S3 is a minimal path-style S3 server covering the operations the object storage backend uses:
// conditional PUT (If-None-Match: * and If-Match compare-and-swap), GET, DELETE, and
// ListObjectsV2. It answers instantly and holds everything in memory. The fault hooks let a test
// land a write at an exact point inside a CAS loop, or fail one operation, instead of racing on
// timing.
type S3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	revs    map[string]int

	// Conflicts is the number of conditional PUTs to reject with 409 before accepting. Set it
	// before the writes it should affect.
	Conflicts int

	// LostPuts is the number of conditional PUTs to apply but answer with 409, simulating a
	// write that succeeds server-side while its response is lost.
	LostPuts int

	failDelete string
	prePut     func(key string)

	puts               int
	consistentReads    int
	nonConsistentReads int

	server *httptest.Server
}

// NewS3 starts a fake S3 server for the test and stops it at cleanup.
func NewS3(t testing.TB) *S3 {
	t.Helper()

	f := &S3{objects: map[string][]byte{}, revs: map[string]int{}}
	f.server = httptest.NewServer(f.Handler(bucket))
	t.Cleanup(f.server.Close)
	return f
}

// URL is the server's endpoint, for a consumer that configures the store by Endpoint.
func (f *S3) URL() string {
	return f.server.URL
}

// Bucket is the name of the one bucket the server holds.
func (f *S3) Bucket() string {
	return bucket
}

// Client returns an S3 client for the server, with static credentials and path-style addressing —
// what ObjectStorageConfig.Client takes. Nothing reads the environment, so tests using it may
// run in parallel.
func (f *S3) Client() *s3.Client {
	return s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(f.server.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
}

// SetFailDelete makes a DELETE of exactly key answer 500, to exercise a cleanup that fails partway
// through its deletes; an empty key clears it.
func (f *S3) SetFailDelete(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failDelete = key
}

// SetPrePut arms (or, with nil, disarms) a hook that runs just before a PUT is applied, outside
// the server's lock so the hook may itself write through a store. It lets a test land a competing
// write at an exact point inside a CAS loop — the losing attempt has read and mutated, but has not
// yet written.
//
// A hook that blocks holds one of the server's request goroutines, and the server's Close waits
// for it. Release it on every test exit — a deferred sync.OnceFunc, not a line after the
// assertions — or a failed assertion hangs the package to its timeout instead of failing.
func (f *S3) SetPrePut(fn func(key string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prePut = fn
}

// Puts is the number of PUT requests served, conditional or not, accepted or refused.
func (f *S3) Puts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts
}

// ConsistentReads is the number of GET and list requests that asked for a consistent read.
func (f *S3) ConsistentReads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.consistentReads
}

// NonConsistentReads is the number of GET and list requests that did not.
func (f *S3) NonConsistentReads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nonConsistentReads
}

// firePrePut invokes the armed hook, if any, with the lock released — the hook is expected to
// drive the store, whose requests need the server's lock in turn.
func (f *S3) firePrePut(key string) {
	f.mu.Lock()
	fn := f.prePut
	f.mu.Unlock()
	if fn != nil {
		fn(key)
	}
}

// etag returns the current ETag for a key; it changes on every accepted write so If-Match
// detects concurrent modification. Callers hold f.mu.
func (f *S3) etag(key string) string {
	return fmt.Sprintf("%q", fmt.Sprintf("%s#%d", key, f.revs[key]))
}

// Handler serves the fake for the named bucket, for a consumer that runs its own server.
func (f *S3) Handler(bucket string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/"+bucket), "/")
		if r.Method == http.MethodPut {
			f.firePrePut(key)
		}

		f.mu.Lock()
		defer f.mu.Unlock()

		switch r.Method {
		case http.MethodPut:
			f.put(w, r, key)
		case http.MethodDelete:
			f.del(w, key)
		case http.MethodGet:
			f.get(w, r, bucket, key)
		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	})
}

// put applies a write under its If-None-Match: * or If-Match condition, then the armed faults:
// Conflicts refuses before applying, LostPuts refuses after. Callers hold f.mu.
func (f *S3) put(w http.ResponseWriter, r *http.Request, key string) {
	f.puts++
	ifNoneMatch, ifMatch := r.Header.Get("If-None-Match") == "*", r.Header.Get("If-Match")
	conditional := ifNoneMatch || ifMatch != ""
	if conditional && f.Conflicts > 0 {
		f.Conflicts--
		w.WriteHeader(http.StatusConflict)
		return
	}
	_, exists := f.objects[key]
	if ifNoneMatch && exists {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	if ifMatch != "" && (!exists || ifMatch != f.etag(key)) {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	body, _ := io.ReadAll(r.Body)
	f.objects[key] = body
	f.revs[key]++
	if conditional && f.LostPuts > 0 {
		f.LostPuts--
		w.WriteHeader(http.StatusConflict)
		return
	}
	w.Header().Set("ETag", f.etag(key))
	w.WriteHeader(http.StatusOK)
}

// del removes a key, or fails it when it is the armed failDelete. Callers hold f.mu.
func (f *S3) del(w http.ResponseWriter, key string) {
	if f.failDelete != "" && key == f.failDelete {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	delete(f.objects, key)
	delete(f.revs, key)
	w.WriteHeader(http.StatusNoContent)
}

// get answers a ListObjectsV2 or a single object read. Callers hold f.mu.
func (f *S3) get(w http.ResponseWriter, r *http.Request, bucket, key string) {
	f.countRead(r)
	if r.URL.Query().Get("list-type") == "2" {
		f.list(w, bucket, r.URL.Query().Get("prefix"))
		return
	}
	body, ok := f.objects[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", f.etag(key))
	w.Write(body)
}

// list answers ListObjectsV2 for a prefix with every matching key, sorted, in one page.
// Callers hold f.mu.
func (f *S3) list(w http.ResponseWriter, bucket, prefix string) {
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
}

// countRead tallies a read by whether it asked for the leader-routed consistent read. Callers
// hold f.mu.
func (f *S3) countRead(r *http.Request) {
	if r.Header.Get("X-Tigris-Consistent") == "true" {
		f.consistentReads++
		return
	}
	f.nonConsistentReads++
}
