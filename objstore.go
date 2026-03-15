package fsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// ObjectStorageConfig configures the S3-compatible object storage backend.
type ObjectStorageConfig struct {
	Bucket   string // S3 bucket name
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

// --- Key helpers ---

func (s *objectStore) eventKey(resourceID, action string, runVersion, eventVersion ulid.ULID) string {
	return path.Join(s.cfg.prefix(), "events", resourceID, action, runVersion.String(), eventVersion.String())
}

func (s *objectStore) eventPrefix(resourceID, action string, runVersion ulid.ULID) string {
	return path.Join(s.cfg.prefix(), "events", resourceID, action, runVersion.String()) + "/"
}

func (s *objectStore) historyKey(date string, runVersion ulid.ULID) string {
	return path.Join(s.cfg.prefix(), "history", date, runVersion.String())
}

func (s *objectStore) childKey(parent, child ulid.ULID) string {
	return path.Join(s.cfg.prefix(), "children", parent.String(), child.String())
}

func (s *objectStore) childPrefix(parent ulid.ULID) string {
	return path.Join(s.cfg.prefix(), "children", parent.String()) + "/"
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

// isNotFound checks if an S3 error is a 404 Not Found response.
func isNotFound(err error) bool {
	var httpErr interface{ HTTPStatusCode() int }
	return errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotFound
}

// putIfAbsent writes an object only if it does not already exist (If-None-Match: *).
// Returns errPreconditionFailed if the object already exists.
func (s *objectStore) putIfAbsent(ctx context.Context, key string, body []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s.cfg.Bucket,
		Key:         &key,
		Body:        bytes.NewReader(body),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return errPreconditionFailed
		}
		return fmt.Errorf("put object %s: %w", key, err)
	}
	return nil
}

// getObject reads an object and returns its body and ETag.
func (s *objectStore) getObject(ctx context.Context, key string) ([]byte, string, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.cfg.Bucket,
		Key:    &key,
	})
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

// --- Event operations ---

// appendEvent writes a single event object. The event is immutable — if it already
// exists (duplicate write after crash), the 412 is silently ignored.
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

// listRunEvents lists all events for a run in chronological order (ULID lexicographic order).
func (s *objectStore) listRunEvents(ctx context.Context, resourceID, action string, runVersion ulid.ULID) ([]*fsmv1.StateEvent, error) {
	prefix := s.eventPrefix(resourceID, action, runVersion)

	var events []*fsmv1.StateEvent
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: &s.cfg.Bucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list events with prefix %s: %w", prefix, err)
		}

		for _, obj := range page.Contents {
			body, _, err := s.getObject(ctx, aws.ToString(obj.Key))
			if err != nil {
				return nil, err
			}

			var event fsmv1.StateEvent
			if err := proto.Unmarshal(body, &event); err != nil {
				s.logger.WithError(err).WithField("key", aws.ToString(obj.Key)).Error("failed to unmarshal event")
				continue
			}
			events = append(events, &event)
		}
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
	prefix := s.childPrefix(parent)

	var children []ulid.ULID
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: &s.cfg.Bucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list children with prefix %s: %w", prefix, err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
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
	}

	return children, nil
}
