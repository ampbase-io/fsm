package fsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/cenkalti/backoff/v4"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

// The queues/ prefix holds one QueueFile per queue name — the shared, CAS-serialized roster of a
// queue's admitted (in-flight) runs. It is how the object backend enforces a queue's concurrency
// limit across the whole cluster, replacing the in-process queuedRunner the BoltDB backend keeps.
// Admission is integrated into the claim loop (claimEntry, objstore_lease.go): before a node
// leases a queued run's manifest it CAS-admits the run here iff the roster is below capacity; on
// FINISH it CAS-removes. Pending queued runs are not recorded here — they live under locks/ and are
// admitted as slots free.

// errQueueNoChange is returned by a casQueue mutation when the roster already holds the desired
// state, so the CAS write is skipped. It is not surfaced to callers.
var errQueueNoChange = errors.New("queue roster unchanged")

func (s *objectStore) queueKey(name string) string {
	return s.key("queues", escapeSegment(name))
}

// queueCapacity reports the configured capacity for a queue and whether this node knows it. The
// roster object's stored capacity is authoritative once created; this value seeds it on the first
// write and gates admission for a queue this node has no configuration for.
func (s *objectStore) queueCapacity(name string) (int, bool) {
	c, ok := s.queues[name]
	return c, ok
}

// getQueue reads a queue roster with a consistent read, returning it with the ETag to CAS against.
func (s *objectStore) getQueue(ctx context.Context, name string) (*fsmv1.QueueFile, string, error) {
	body, etag, err := s.getObject(ctx, s.queueKey(name))
	if err != nil {
		return nil, "", err
	}
	var q fsmv1.QueueFile
	if err := proto.Unmarshal(body, &q); err != nil {
		return nil, "", fmt.Errorf("unmarshal queue %s: %w", name, err)
	}
	return &q, etag, nil
}

// casQueue applies mutate to the queue roster under a compare-and-swap, creating the object
// (seeded with this node's configured capacity) when absent and retrying on concurrent
// modification, so mutate always observes fresh state. mutate may return errQueueNoChange to skip
// the write when the roster already holds the desired state — leaving a not-yet-created roster
// uncreated. It returns the roster as written (or as read, for a no-op).
func (s *objectStore) casQueue(ctx context.Context, name string, mutate func(*fsmv1.QueueFile) error) (*fsmv1.QueueFile, error) {
	key := s.queueKey(name)
	var result *fsmv1.QueueFile
	op := func() error {
		queue, etag, err := s.getQueue(ctx, name)
		switch {
		case errors.Is(err, ErrFsmNotFound):
			// The roster does not exist yet. Only a node that has the queue configured can seed its
			// authoritative capacity, so an unconfigured node skips here (no write): admitQueued
			// then leaves the run pending for a configured node — rather than admitting ungated and
			// breaking the cluster limit — and release/heartbeat have nothing to do on an absent
			// roster. The empty etag from the miss signals a create to putRoster below.
			capacity, ok := s.queueCapacity(name)
			if !ok {
				return nil
			}
			queue = &fsmv1.QueueFile{Capacity: int32(capacity)}
		case err != nil:
			return backoff.Permanent(err)
		}

		switch mErr := mutate(queue); {
		case errors.Is(mErr, errQueueNoChange):
			result = queue
			return nil
		case mErr != nil:
			return backoff.Permanent(mErr)
		}

		body, err := proto.Marshal(queue)
		if err != nil {
			return backoff.Permanent(fmt.Errorf("marshal queue %s: %w", name, err))
		}

		switch werr := s.putRoster(ctx, key, body, etag); {
		case werr == nil:
			queueCommitsVec.WithLabelValues(name).Inc()
			queueDepthVec.WithLabelValues(name).Set(float64(len(queue.GetJobs())))
			result = queue
			return nil
		case errors.Is(werr, errEtagMismatch):
			casRetriesVec.WithLabelValues("queue").Inc()
			s.logger.WithField("queue", name).Debug("queue roster changed concurrently, retrying")
			return werr
		default:
			return backoff.Permanent(werr)
		}
	}
	if err := backoff.Retry(op, retryBackoff(ctx, maxManifestCASRetries)); err != nil {
		return nil, err
	}
	return result, nil
}

// putRoster persists the roster body under the right conditional and classifies the result for
// casQueue's retry loop: a new roster (no prior etag) is created with If-None-Match, an existing one
// updated with an If-Match compare-and-swap. A create that loses the race (412) is reported as an
// etag mismatch, so casQueue re-reads and CASes against the winner's roster.
func (s *objectStore) putRoster(ctx context.Context, key string, body []byte, etag string) error {
	if etag != "" {
		return s.putIfMatch(ctx, key, body, etag)
	}
	err := s.putIfAbsent(ctx, key, body)
	if errors.Is(err, errPreconditionFailed) {
		return errEtagMismatch
	}
	return err
}

// admitQueued CAS-admits version into the queue roster iff there is capacity, reclaiming jobs whose
// heartbeat has expired, and reports whether the run was admitted. It is called from the claim path
// only after the run's manifest passed the claimable check, so this node is the run's legitimate
// taker: a job already owned by this node is treated as admitted (idempotent); a fresh job held by
// another node is left alone (a stale scan — that node is handling the run, resolved next pass); a
// full roster returns (false, nil) so the run stays pending for a later pass. Admission always gates
// against the roster's stored capacity, so a node that lacks the queue in its own config still
// honors the cluster limit once a configured node has seeded the roster; if no roster exists yet and
// this node cannot seed one (no configured capacity), it returns (false, nil) — leaving the run
// pending for a configured node rather than running it ungated (casQueue).
func (s *objectStore) admitQueued(ctx context.Context, name string, version ulid.ULID) (bool, error) {
	admitted := false
	_, err := s.casQueue(ctx, name, func(q *fsmv1.QueueFile) error {
		changed := s.reclaimStaleJobs(q)

		switch job := findJob(q, version); {
		case job != nil:
			// Already rostered: admitted if it is ours (idempotent); otherwise a live peer holds
			// the slot and we leave it be.
			admitted = job.GetOwnerNode() == s.node
		case len(q.GetJobs()) < int(q.GetCapacity()):
			q.Jobs = append(q.Jobs, &fsmv1.QueueJob{
				RunVersion: versionBytes(version),
				OwnerNode:  s.node,
				Heartbeat:  time.Now().UnixMilli(),
			})
			admitted = true
			return nil
		}
		// Not admitted (a peer holds it, or the queue is full): persist only a reclamation, if any.
		if !changed {
			return errQueueNoChange
		}
		return nil
	})
	return admitted, err
}

// releaseQueued removes this node's job for version from the queue roster, freeing its slot.
// Owner-checked, so a slot already taken over by another node is left intact; a missing job or a
// missing roster is a no-op. Called on manifest-claim-loss, on FINISH, and on early lease release.
func (s *objectStore) releaseQueued(ctx context.Context, name string, version ulid.ULID) error {
	_, err := s.casQueue(ctx, name, func(q *fsmv1.QueueFile) error {
		if !removeOwnedJob(q, version, s.node) {
			return errQueueNoChange
		}
		return nil
	})
	return err
}

// heartbeatQueues refreshes this node's roster entries in every queue it holds admitted runs in,
// and reclaims stale entries, as one CAS per queue. It upserts — a job for an owned run that is
// missing is re-added — so a rare roster/lease miscount from a takeover race self-heals: it only
// asserts jobs for runs whose lease this node still holds. Driven from extendLeases after lost
// leases are dropped, so it never re-warms a slot this node is losing.
func (s *objectStore) heartbeatQueues(ctx context.Context) {
	for name, versions := range s.ownedByQueue() {
		// A heartbeat always rewrites now into this node's entries, so the CAS always writes; it
		// reclaims any stale entries it passes over on the way.
		_, err := s.casQueue(ctx, name, func(q *fsmv1.QueueFile) error {
			s.reclaimStaleJobs(q)
			s.refreshOwnedJobs(q, versions)
			return nil
		})
		switch {
		case err == nil, errors.Is(err, context.Canceled):
		default:
			s.logger.WithError(err).WithField("queue", name).Error("failed to heartbeat queue roster")
		}
	}
}

// reclaimStaleJobs drops roster entries whose heartbeat is older than the lease timeout — the slots
// of nodes that died or lost the run — and reports whether any were removed. Staleness uses the
// lease timeout so a job and its owner's manifest lease expire together on a crash.
//
// Staleness is a local-clock liveness heuristic, mirroring lease expiry (claimable): a roster slot
// carries no fencing token, unlike the manifest's lease epoch. So a node whose clock runs far ahead
// of a peer — or a peer paused longer than the lease timeout — can reclaim a still-live slot and
// admit another run, briefly exceeding capacity. Capacity is therefore a soft limit under clock
// skew, the same trade-off the RFC accepts for lease takeover (correctness of run *state* still
// rests on the epoch CAS; only the concurrency count is heuristic).
func (s *objectStore) reclaimStaleJobs(q *fsmv1.QueueFile) bool {
	cutoff := time.Now().Add(-s.cfg.leaseTimeout()).UnixMilli()
	kept := q.Jobs[:0]
	removed := false
	for _, job := range q.Jobs {
		if job.GetHeartbeat() < cutoff {
			removed = true
			continue
		}
		kept = append(kept, job)
	}
	q.Jobs = kept
	return removed
}

// refreshOwnedJobs upserts this node's roster entries for versions — refreshing a present job's
// heartbeat, re-adding a missing one, and reclaiming one currently attributed to another node
// (this node holds the run's lease, so it is the true owner).
func (s *objectStore) refreshOwnedJobs(q *fsmv1.QueueFile, versions []ulid.ULID) {
	now := time.Now().UnixMilli()
	for _, version := range versions {
		job := findJob(q, version)
		if job == nil {
			q.Jobs = append(q.Jobs, &fsmv1.QueueJob{RunVersion: versionBytes(version), OwnerNode: s.node, Heartbeat: now})
			continue
		}
		job.OwnerNode = s.node
		job.Heartbeat = now
	}
}

// findJob returns version's job in the roster, or nil.
func findJob(q *fsmv1.QueueFile, version ulid.ULID) *fsmv1.QueueJob {
	target := versionBytes(version)
	for _, job := range q.GetJobs() {
		if bytes.Equal(job.GetRunVersion(), target) {
			return job
		}
	}
	return nil
}

// removeOwnedJob removes version's job iff it is owned by node, reporting whether it removed one.
func removeOwnedJob(q *fsmv1.QueueFile, version ulid.ULID, node string) bool {
	target := versionBytes(version)
	for i, job := range q.GetJobs() {
		if bytes.Equal(job.GetRunVersion(), target) && job.GetOwnerNode() == node {
			q.Jobs = append(q.Jobs[:i], q.Jobs[i+1:]...)
			return true
		}
	}
	return false
}

// versionBytes is the text encoding of a run version, matching how run versions are stored in
// manifests (RunManifest.run_version) and lock bodies.
func versionBytes(version ulid.ULID) []byte {
	b, _ := version.MarshalText() // a valid ULID never fails to marshal
	return b
}
