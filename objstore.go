package fsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
}

func (c *ObjectStorageConfig) prefix() string {
	if c.Prefix != "" {
		return c.Prefix
	}
	return "fsm/"
}

func (c *ObjectStorageConfig) leaseTimeout() time.Duration {
	if c.LeaseTimeout > 0 {
		return c.LeaseTimeout
	}
	return 30 * time.Second
}

func (c *ObjectStorageConfig) heartbeatPeriod() time.Duration {
	if c.HeartbeatPeriod > 0 {
		return c.HeartbeatPeriod
	}
	return 10 * time.Second
}

// objectStore implements low-level object storage operations for events, history,
// and children. It does not yet implement the full Store interface — that comes in Phase 3
// when the run manifest and Append()/Active() are added.
type objectStore struct {
	logger logrus.FieldLogger
	client *s3.Client
	cfg    *ObjectStorageConfig
}

func newObjectStore(ctx context.Context, logger logrus.FieldLogger, cfg *ObjectStorageConfig) (*objectStore, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("object storage bucket is required")
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
		logger: logger,
		client: client,
		cfg:    cfg,
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

func (s *objectStore) childKey(parent, child ulid.ULID) string {
	return s.key("children", parent.String(), child.String())
}

func (s *objectStore) childPrefix(parent ulid.ULID) string {
	return s.key("children", parent.String()) + "/"
}

// --- Conditional write helpers ---

var errPreconditionFailed = errors.New("precondition failed: object already exists")

// isPreconditionFailed checks if an S3 error is a 412 Precondition Failed response.
// AWS SDK v2 does not expose a typed error for 412; instead, extract the HTTP status
// code from the underlying response via the smithy-go HTTPStatusCode() interface.
func isPreconditionFailed(err error) bool {
	var httpErr interface{ HTTPStatusCode() int }
	return errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusPreconditionFailed
}

// isConditionalConflict checks if an S3 error is a 409 Conflict response. S3 returns 409
// (ConditionalRequestConflict) when concurrent conditional writes race on the same key; the
// request should be retried, after which the winner's object exists and the retry observes 412.
func isConditionalConflict(err error) bool {
	var httpErr interface{ HTTPStatusCode() int }
	return errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusConflict
}

// isNotFound checks if an S3 error is a 404 Not Found response.
func isNotFound(err error) bool {
	var httpErr interface{ HTTPStatusCode() int }
	return errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotFound
}

// maxConditionalRetries bounds retries of conditional writes that fail with 409 Conflict.
const maxConditionalRetries = 5

// putIfAbsent writes an object only if it does not already exist (If-None-Match: *).
// Returns errPreconditionFailed if the object already exists. Retries on 409 Conflict,
// which S3 returns when concurrent conditional writes race on the same key.
func (s *objectStore) putIfAbsent(ctx context.Context, key string, body []byte) error {
	for attempt := 0; ; attempt++ {
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &s.cfg.Bucket,
			Key:         &key,
			Body:        bytes.NewReader(body),
			IfNoneMatch: aws.String("*"),
		})
		switch {
		case err == nil:
			return nil
		case isPreconditionFailed(err):
			return errPreconditionFailed
		case isConditionalConflict(err) && attempt < maxConditionalRetries:
			delay := time.Duration(50<<attempt) * time.Millisecond
			s.logger.WithField("key", key).WithField("attempt", attempt).Debug("conditional write conflict, retrying")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		default:
			return fmt.Errorf("put object %s: %w", key, err)
		}
	}
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

	key := s.eventKey(resourceID, action, runVersion, eventVersion)
	if err := s.putIfAbsent(ctx, key, body); err != nil {
		if errors.Is(err, errPreconditionFailed) {
			s.logger.WithField("key", key).Debug("event already exists, ignoring duplicate")
			return nil
		}
		return err
	}
	return nil
}

// listEventFetchers bounds the concurrent object reads performed by listRunEvents.
const listEventFetchers = 8

// listRunEvents lists all events for a run in chronological order (ULID lexicographic order).
// Any failure to read or decode an event fails the whole listing: callers use the result to
// decide which transitions to re-execute on resume, so a silently dropped event would cause a
// completed transition to run again.
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
	key := s.historyKey(date, runVersion)
	if err := s.putIfAbsent(ctx, key, body); err != nil {
		if errors.Is(err, errPreconditionFailed) {
			s.logger.WithField("key", key).Debug("history already exists, ignoring duplicate")
			return nil
		}
		return err
	}
	return nil
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
	key := s.childKey(parent, child)
	body := []byte(child.String())
	if err := s.putIfAbsent(ctx, key, body); err != nil {
		if errors.Is(err, errPreconditionFailed) {
			return nil
		}
		return err
	}
	return nil
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
