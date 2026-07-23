package fsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/cenkalti/backoff/v4"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// ObjectStorageConfig configures the S3-compatible object storage backend.
type ObjectStorageConfig struct {
	Bucket   string // S3 bucket name (required)
	Endpoint string // e.g., "https://fly.storage.tigris.dev"
	Region   string // e.g., "auto" for Tigris
	Prefix   string // Key namespace prefix, default "fsm/"

	LeaseTimeout    time.Duration // Default 30s
	HeartbeatPeriod time.Duration // Default 10s

	// ClaimInterval is how often the claim loop scans for runs this node can take over.
	ClaimInterval time.Duration // Default LeaseTimeout / 2

	// WaitPollInterval and WaitPollMaxInterval bound the exponential backoff of WaitRun's
	// manifest poll floor.
	WaitPollInterval    time.Duration // Default 100ms
	WaitPollMaxInterval time.Duration // Default 2s
}

func (c *ObjectStorageConfig) prefix() string {
	if c.Prefix != "" {
		return c.Prefix
	}
	return "fsm/"
}

// orDefault returns d when set (positive), def otherwise.
func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

func (c *ObjectStorageConfig) leaseTimeout() time.Duration {
	return orDefault(c.LeaseTimeout, 30*time.Second)
}

func (c *ObjectStorageConfig) heartbeatPeriod() time.Duration {
	return orDefault(c.HeartbeatPeriod, 10*time.Second)
}

func (c *ObjectStorageConfig) claimInterval() time.Duration {
	return orDefault(c.ClaimInterval, c.leaseTimeout()/2)
}

func (c *ObjectStorageConfig) waitPollInterval() time.Duration {
	return orDefault(c.WaitPollInterval, 100*time.Millisecond)
}

func (c *ObjectStorageConfig) waitPollMaxInterval() time.Duration {
	return orDefault(c.WaitPollMaxInterval, 2*time.Second)
}

// objectStore implements the Store interface over S3-compatible object storage.
type objectStore struct {
	logger logrus.FieldLogger
	client *s3.Client
	cfg    *ObjectStorageConfig

	// nodeID identifies this node in run manifests for lease ownership. It must be unique per
	// process: a zombie sharing a NodeID is still fenced by the lease epoch.
	nodeID string

	// owned maps each run this node holds a lease on to the lease epoch it claimed with; every
	// run-mutating manifest CAS re-verifies both. fenced buffers runs whose fence tripped in
	// Append so the next heartbeat pass reports them for local cancellation.
	leaseMu sync.Mutex
	owned   map[ulid.ULID]int64
	fenced  []ulid.ULID

	// finished records terminal outcomes of runs this process completed, preserving typed run
	// errors for same-process waiters. It is a bounded convenience, not state: absence just
	// means WaitRun consults the manifest, whose recorded error string is the durable outcome.
	// finishing tracks runs mid-appendFinish here so WaitRun releases waiters only after the
	// finish cleanup (lock delete, history write) lands, matching the pre-terminal-manifest
	// visibility BoltDB waiters get.
	finishedMu sync.Mutex
	finished   map[ulid.ULID]RunErr
	finishing  map[ulid.ULID]struct{}
}

func newObjectStore(ctx context.Context, logger logrus.FieldLogger, cfg *ObjectStorageConfig, nodeID string) (*objectStore, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("object storage bucket is required")
	}
	if nodeID == "" {
		host, _ := os.Hostname()
		nodeID = host + "-" + ulid.Make().String()
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = &cfg.Endpoint
		}
		o.UsePathStyle = true
	})

	return &objectStore{
		logger:    logger.WithField("node_id", nodeID),
		client:    client,
		cfg:       cfg,
		nodeID:    nodeID,
		owned:     map[ulid.ULID]int64{},
		finished:  map[ulid.ULID]RunErr{},
		finishing: map[ulid.ULID]struct{}{},
	}, nil
}

// consistentRead routes the request to the leader on Tigris so reads observe all prior
// writes, which the read-then-conditional-write flows depend on. Other S3 implementations
// ignore the header (S3 itself is strongly consistent).
func consistentRead(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, smithyhttp.SetHeaderValue("X-Tigris-Consistent", "true"))
}

// --- Key helpers ---

// escapeSegment makes a caller-supplied value safe to embed as a single object key segment.
// Escaping "/" (and friends) prevents a resource id or action from introducing extra key
// hierarchy and breaking the prefix-listing contract. Keys are joined verbatim rather than
// path-cleaned, so segments like ".." remain literal segment names with no traversal semantics.
func escapeSegment(s string) string {
	return url.PathEscape(s)
}

// unescapeSegment reverses escapeSegment when parsing values back out of listed keys.
func unescapeSegment(s string) (string, error) {
	return url.PathUnescape(s)
}

// key joins the configured prefix and the given segments verbatim. Caller-supplied segments
// must already be escaped with escapeSegment.
func (s *objectStore) key(segments ...string) string {
	parts := make([]string, 0, len(segments)+1)
	parts = append(parts, strings.TrimSuffix(s.cfg.prefix(), "/"))
	parts = append(parts, segments...)
	return strings.Join(parts, "/")
}

func (s *objectStore) eventKey(resourceID, action string, runVersion, eventVersion ulid.ULID) string {
	return s.key("events", escapeSegment(resourceID), escapeSegment(action), runVersion.String(), eventVersion.String())
}

func (s *objectStore) eventPrefix(resourceID, action string, runVersion ulid.ULID) string {
	return s.key("events", escapeSegment(resourceID), escapeSegment(action), runVersion.String()) + "/"
}

func (s *objectStore) historyKey(date string, runVersion ulid.ULID) string {
	return s.key("history", date, runVersion.String())
}

func (s *objectStore) runIndexKey(resourceType, resourceID, action string, runVersion ulid.ULID) string {
	return s.key("index", escapeSegment(resourceType), escapeSegment(resourceID), escapeSegment(action), runVersion.String())
}

func (s *objectStore) runIndexPrefix(resourceType, resourceID string) string {
	return s.key("index", escapeSegment(resourceType), escapeSegment(resourceID)) + "/"
}

func (s *objectStore) childKey(parent, child ulid.ULID) string {
	return s.key("children", parent.String(), child.String())
}

func (s *objectStore) childPrefix(parent ulid.ULID) string {
	return s.key("children", parent.String()) + "/"
}

// --- Conditional write helpers ---

var errPreconditionFailed = errors.New("precondition failed: object already exists")

// httpStatusError is satisfied by smithy-go response errors, which carry the HTTP status
// code of the failed request. AWS SDK v2 does not expose typed errors for conditional-write
// failures, so classification goes by status code.
type httpStatusError interface {
	error
	HTTPStatusCode() int
}

// hasHTTPStatus reports whether err's chain contains an HTTP response error with the given
// status code.
func hasHTTPStatus(err error, status int) bool {
	httpErr, ok := errors.AsType[httpStatusError](err)
	return ok && httpErr.HTTPStatusCode() == status
}

// isPreconditionFailed checks if an S3 error is a 412 Precondition Failed response.
func isPreconditionFailed(err error) bool {
	return hasHTTPStatus(err, http.StatusPreconditionFailed)
}

// isConditionalConflict checks if an S3 error is a 409 Conflict response. S3 returns 409
// (ConditionalRequestConflict) when concurrent conditional writes race on the same key; the
// request should be retried, after which the winner's object exists and the retry observes 412.
func isConditionalConflict(err error) bool {
	return hasHTTPStatus(err, http.StatusConflict)
}

// isNotFound checks if an S3 error is a 404 Not Found response.
func isNotFound(err error) bool {
	return hasHTTPStatus(err, http.StatusNotFound)
}

// maxConditionalRetries bounds retries of conditional writes that fail with 409 Conflict.
const maxConditionalRetries = 5

var errEtagMismatch = errors.New("precondition failed: etag mismatch")

// expBackoff builds the exponential backoff shared by the store's retry loops: bounded by the
// caller's wrapper (max retries) or context, never wall-clock.
func expBackoff(initial, max time.Duration) *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = initial
	b.MaxInterval = max
	b.MaxElapsedTime = 0
	return b
}

// retryBackoff returns a context-bound exponential backoff capped at maxRetries, used for the
// transient 409 conflicts and manifest CAS contention that conditional writes hit.
func retryBackoff(ctx context.Context, maxRetries uint64) backoff.BackOff {
	return backoff.WithContext(backoff.WithMaxRetries(expBackoff(50*time.Millisecond, 2*time.Second), maxRetries), ctx)
}

// waitBackoff returns the backoff pacing WaitRun's manifest polls: bounded only by ctx, so a
// wait outlives any fixed retry budget.
func (s *objectStore) waitBackoff(ctx context.Context) backoff.BackOff {
	return backoff.WithContext(expBackoff(s.cfg.waitPollInterval(), s.cfg.waitPollMaxInterval()), ctx)
}

// putConditional issues a conditional PutObject — set applies the condition header — and retries
// on 409 Conflict, which S3 returns when concurrent conditional writes race on a key. It returns
// the raw PutObject error otherwise; callers map a 412 to their own sentinel.
func (s *objectStore) putConditional(ctx context.Context, key string, body []byte, set func(*s3.PutObjectInput)) error {
	op := func() error {
		in := &s3.PutObjectInput{Bucket: &s.cfg.Bucket, Key: &key, Body: bytes.NewReader(body)}
		set(in)
		switch _, err := s.client.PutObject(ctx, in); {
		case err == nil:
			return nil
		case isConditionalConflict(err):
			s.logger.WithField("key", key).Debug("conditional write conflict, retrying")
			return err
		default:
			return backoff.Permanent(err)
		}
	}
	return backoff.Retry(op, retryBackoff(ctx, maxConditionalRetries))
}

// putIfAbsent writes an object only if it does not already exist (If-None-Match: *), returning
// errPreconditionFailed if it does.
func (s *objectStore) putIfAbsent(ctx context.Context, key string, body []byte) error {
	switch err := s.putConditional(ctx, key, body, func(in *s3.PutObjectInput) {
		in.IfNoneMatch = aws.String("*")
	}); {
	case err == nil:
		return nil
	case isPreconditionFailed(err):
		return errPreconditionFailed
	default:
		return fmt.Errorf("put object %s: %w", key, err)
	}
}

// putIfMatch writes an object only if its current ETag matches (If-Match), giving compare-and-
// swap semantics. Returns errEtagMismatch if the object changed since etag was read.
func (s *objectStore) putIfMatch(ctx context.Context, key string, body []byte, etag string) error {
	switch err := s.putConditional(ctx, key, body, func(in *s3.PutObjectInput) {
		in.IfMatch = aws.String(etag)
	}); {
	case err == nil:
		return nil
	case isPreconditionFailed(err):
		return errEtagMismatch
	default:
		return fmt.Errorf("put object %s: %w", key, err)
	}
}

// putIdempotent writes a write-once object, treating an already-existing object (412) as
// success. Used for records a crash-retry may re-issue with identical content.
func (s *objectStore) putIdempotent(ctx context.Context, key string, body []byte) error {
	if err := s.putIfAbsent(ctx, key, body); err != nil && !errors.Is(err, errPreconditionFailed) {
		return err
	}
	return nil
}

// deleteObject removes an object. Deleting a missing key is not an error.
func (s *objectStore) deleteObject(ctx context.Context, key string) error {
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.cfg.Bucket,
		Key:    &key,
	}); err != nil {
		return fmt.Errorf("delete object %s: %w", key, err)
	}
	return nil
}

// getObject reads an object and returns its body and ETag.
func (s *objectStore) getObject(ctx context.Context, key string) ([]byte, string, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.cfg.Bucket,
		Key:    &key,
	}, consistentRead)
	if err != nil {
		if isNotFound(err) {
			return nil, "", ErrFsmNotFound
		}
		return nil, "", fmt.Errorf("get object %s: %w", key, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read object %s: %w", key, err)
	}

	etag := aws.ToString(resp.ETag)
	return body, etag, nil
}

// listKeys returns all object keys under the given prefix in lexicographic order.
func (s *objectStore) listKeys(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: &s.cfg.Bucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx, consistentRead)
		if err != nil {
			return nil, fmt.Errorf("list objects with prefix %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

// --- Event operations ---

// appendEvent writes a single event object with create-if-absent semantics.
//
// Idempotency contract: retrying after a crash is only safe when the SAME eventVersion is
// reused for the retry — the write then observes 412 and is treated as already-applied. A
// caller that mints a fresh event version per attempt (as boltStore.Append does today) would
// append a duplicate event under a new key instead. Phase 3's Append must derive the event
// version before the first write attempt and reuse it across retries.
func (s *objectStore) appendEvent(ctx context.Context, resourceID, action string, runVersion, eventVersion ulid.ULID, event *fsmv1.StateEvent) error {
	body, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	return s.putIdempotent(ctx, s.eventKey(resourceID, action, runVersion, eventVersion), body)
}

// listEventFetchers bounds the concurrent object reads performed by listRunEvents.
const listEventFetchers = 8

// listRunEvents lists all events for a run in chronological order (ULID lexicographic order).
//
// Cost: one GET per event body (object storage has no batch GET), fetched listEventFetchers at
// a time — linear in run length. This is intended for audit/history views and crash
// reconciliation only; resume must read the run manifest instead, which materializes the same
// state in a single GET (see the "Resumability" section of the object storage RFC).
//
// Any failure to read or decode an event fails the whole listing: a silently dropped event
// would misrepresent the run's history to reconciliation, which could re-execute a completed
// transition.
func (s *objectStore) listRunEvents(ctx context.Context, resourceID, action string, runVersion ulid.ULID) ([]*fsmv1.StateEvent, error) {
	keys, err := s.listKeys(ctx, s.eventPrefix(resourceID, action, runVersion))
	if err != nil {
		return nil, err
	}

	var (
		events   = make([]*fsmv1.StateEvent, len(keys))
		sem      = make(chan struct{}, listEventFetchers)
		wg       sync.WaitGroup
		mu       sync.Mutex
		fetchErr error
	)
	for i, key := range keys {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			body, _, err := s.getObject(ctx, key)
			if err == nil {
				var event fsmv1.StateEvent
				if uerr := proto.Unmarshal(body, &event); uerr != nil {
					err = fmt.Errorf("unmarshal event %s: %w", key, uerr)
				} else {
					events[i] = &event
					return
				}
			}

			mu.Lock()
			fetchErr = errors.Join(fetchErr, err)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if fetchErr != nil {
		return nil, fetchErr
	}
	return events, nil
}

// --- History operations ---

// writeHistory writes a history record for a completed run.
// Idempotent — duplicate writes are silently ignored.
func (s *objectStore) writeHistory(ctx context.Context, runVersion ulid.ULID, history *fsmv1.HistoryEvent) error {
	body, err := proto.Marshal(history)
	if err != nil {
		return fmt.Errorf("marshal history: %w", err)
	}

	date := ulid.Time(runVersion.Time()).Format(time.DateOnly)
	return s.putIdempotent(ctx, s.historyKey(date, runVersion), body)
}

// readHistory reads the history record for a completed run.
func (s *objectStore) readHistory(ctx context.Context, runVersion ulid.ULID) (*fsmv1.HistoryEvent, error) {
	date := ulid.Time(runVersion.Time()).Format(time.DateOnly)
	key := s.historyKey(date, runVersion)

	body, _, err := s.getObject(ctx, key)
	if err != nil {
		return nil, err
	}

	var history fsmv1.HistoryEvent
	if err := proto.Unmarshal(body, &history); err != nil {
		return nil, fmt.Errorf("unmarshal history: %w", err)
	}
	return &history, nil
}

// --- Children operations ---

// writeChild records a parent-to-child run relationship.
// Idempotent — duplicate writes are silently ignored.
func (s *objectStore) writeChild(ctx context.Context, parent, child ulid.ULID) error {
	return s.putIdempotent(ctx, s.childKey(parent, child), []byte(child.String()))
}

// listChildren returns all child run versions for a parent.
func (s *objectStore) listChildren(ctx context.Context, parent ulid.ULID) ([]ulid.ULID, error) {
	keys, err := s.listKeys(ctx, s.childPrefix(parent))
	if err != nil {
		return nil, err
	}

	children := make([]ulid.ULID, 0, len(keys))
	for _, key := range keys {
		// Extract child ULID from the last path segment
		parts := strings.Split(key, "/")
		childStr := parts[len(parts)-1]

		var child ulid.ULID
		if err := child.UnmarshalText([]byte(childStr)); err != nil {
			s.logger.WithError(err).WithField("key", key).Error("failed to parse child ULID from key")
			continue
		}
		children = append(children, child)
	}

	return children, nil
}
