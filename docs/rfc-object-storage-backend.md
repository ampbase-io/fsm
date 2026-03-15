# [RFC] Object Storage Backend for superfly/fsm

|           |               |
|-----------|---------------|
|**Created**|March 14, 2026 |
|**Status** |WIP            |
|**Owner**  |JP             |
|**Target** |superfly/fsm v2|

## Overview

This RFC proposes adding an S3-compatible object storage backend (Tigris) to `superfly/fsm` as an alternative to the existing BoltDB persistence layer, enabling the FSM library to optionally run as a multi-node cluster with no local disk dependencies. The existing BoltDB backend remains the default and is unchanged in behavior. The design uses conditional writes (`If-None-Match` and `If-Match`) as the sole coordination primitive, a per-run manifest object as the linearization point for state transitions, and a brokered queue file (inspired by turbopuffer's object storage queue pattern) for distributed concurrency-limited execution. The result is a durable, crash-recoverable, horizontally scalable FSM runtime built entirely on object storage — available as an opt-in backend behind a `Store` interface that both implementations satisfy.

## Background

### What superfly/fsm is

`superfly/fsm` is an internal Go library for defining and executing durable finite state machines. Users register an FSM as an ordered sequence of named transitions using a builder API (`Register[R,W]().Start(...).To(...).End(...).Build()`). Each FSM run is identified by a resource ID and versioned with a ULID. The library handles persistence, retry with exponential backoff, cancellation, parent-child relationships, delayed starts, run-after dependencies, and queued execution with concurrency limits.

### Current storage architecture

The library uses a three-layer storage stack:

**BoltDB (bbolt)** provides durable, on-disk persistence across two database files. `fsm-state.db` holds four buckets: `ACTIVE` (one record per running FSM instance), `EVENTS` (an append-only log of state transition events), `ARCHIVE` (staging area for completed runs), and `CHILDREN` (parent-to-child run relationships). `fsm-history.db` holds completed runs partitioned by date. All mutations flow through a single method, `store.Append()`, which executes inside a BoltDB write transaction to get atomicity across multiple bucket updates.

**go-memdb** is an in-process, in-memory database that provides fast indexed lookups by run version, run ID, and parent ULID. It supports watch-based notifications via `WatchSet`, which powers the blocking `Wait()` and `WaitByID()` methods. It is populated on startup from BoltDB's `ACTIVE` bucket and updated as a write-through cache during `Append()`.

**Protobuf serialization** is used for all persisted state. The key message types are `ActiveEvent` (tracks a running FSM, its resource payload, transition list, and options), `StateEvent` (individual events: START, COMPLETE, ERROR, CANCEL, FINISH), and `HistoryEvent` (archived completed runs).

### Why change

BoltDB binds the FSM manager to a single process on a single machine. The database files live on local disk, so state does not survive machine loss, and only one `Manager` instance can open the database at a time. This creates three operational limitations:

**No horizontal scaling.** A single Manager instance must host all FSM runs. If the workload exceeds what one process can handle, there is no way to distribute runs across nodes.

**No fault tolerance.** If the machine dies, in-flight runs are lost until the process restarts on the same volume. There is no mechanism for another node to take over.

**Disk dependency.** On platforms like Fly.io where ephemeral VMs are the norm, requiring durable local disk (Fly Volumes) adds operational complexity and constrains placement. Object storage (Tigris) is globally available, requires no provisioning, and has no machine affinity.

### Conditional writes on object storage

S3-compatible object storage now supports conditional write operations that enable compare-and-swap semantics:

**`If-None-Match: *`** fails the write with 412 if an object with that key already exists. This provides atomic create-if-not-exists (put-if-absent) semantics.

**`If-Match: <etag>`** fails the write with 412 if the object's current ETag does not match the provided value. This provides atomic read-modify-write (compare-and-swap) semantics.

Tigris (Fly.io's S3-compatible object storage) supports both primitives, plus a `X-Tigris-Consistent: true` header for leader-routed consistent reads, which is essential for read-then-conditional-write patterns.

### Prior art: turbopuffer's object storage queue

turbopuffer published a design for building a distributed job queue using a single JSON file on object storage with CAS. The key insight is that a stateless broker process serializes all mutations to the queue file through a group commit loop, eliminating CAS contention from multiple concurrent writers. The broker buffers incoming requests in memory and flushes them as a single CAS write, decoupling request rate from write rate. High availability is achieved by writing the broker's address into the queue file itself; if the broker becomes unreachable, any node can start a new broker and claim ownership via CAS. Worker liveness is tracked through heartbeat timestamps in the queue file.

This pattern directly solves the hardest problem in distributing the FSM library's queued runner feature: coordinating a shared concurrency limit across nodes without a dedicated coordination service.

## Proposal

Extract a `Store` interface from the existing `store` struct and introduce an S3-compatible object storage backend as an alternative implementation. The existing BoltDB backend remains the default and is unmodified in behavior; the object storage backend is opt-in via configuration. This additive design ensures the library can be committed upstream with no impact on existing consumers while enabling new deployments to run as a multi-node cluster with no local disk dependencies.

In the object storage backend, each FSM run gets its own manifest object that acts as the linearization point for all state mutations. Events are stored as immutable, individually-keyed objects. Run ownership in a cluster is managed through leases on the manifest. Queued execution uses a brokered queue file per queue name, following the turbopuffer pattern. The memdb layer is retained as a local cache for in-process `Wait()` semantics.

### Scope

**In scope:** `Store` interface extraction, object storage backend implementation, per-run manifest design, run leasing and ownership, distributed cancel, brokered queue for concurrency-limited execution, delayed start and run-after on object storage, archive and history on object storage.

**Out of scope:** Changes to the BoltDB backend, FSM builder API, transition function signatures, or the `Request`/`Response` types. Changes to the interceptor chain, error hierarchy, or codec system. Multi-region replication strategy (Tigris handles this transparently).

## Technical Design

### Object Key Schema

All objects live under a configurable prefix (default `fsm/`) within a single S3 bucket.

|Prefix     |Key Format                                            |Mutability              |Purpose                                            |
|-----------|------------------------------------------------------|------------------------|---------------------------------------------------|
|`events/`  |`<resource_id>/<action>/<run_version>/<event_version>`|Immutable (write-once)  |Append-only event log per run                      |
|`runs/`    |`<resource_id>/<action>/<run_version>`                |Mutable (CAS)           |Run manifest: linearization point for all run state|
|`locks/`   |`<resource_type>/<resource_id>/<action>`              |Mutable (create/delete) |Enforces one-active-run-per-resource constraint    |
|`children/`|`<parent_run_version>/<child_run_version>`            |Immutable (write-once)  |Parent-to-child run index                          |
|`history/` |`<date>/<run_version>`                                |Immutable (write-once)  |Archived completed runs                            |
|`queues/`  |`<queue_name>`                                        |Mutable (CAS via broker)|Brokered queue state file                          |

**Key design property:** ULID-based event versions are both monotonically increasing and lexicographically sortable. S3's `ListObjectsV2` returns keys in lexicographic order. This means prefix listing under `events/<resource_id>/<action>/<run_version>/` returns events in chronological order with no additional sorting, directly replacing BoltDB's cursor-based iteration.

### Run Manifest

The run manifest is the central coordination object for each FSM run. It replaces the role that the BoltDB transaction played in providing atomic multi-field updates. All state transitions are sequenced through conditional writes to this object.

```protobuf
message RunManifest {
  string status = 1;             // "pending", "running", "complete", "archived"
  string resource_type = 2;
  string resource_id = 3;
  string action = 4;
  bytes  run_version = 5;
  bytes  start_event_key = 6;
  bytes  end_event_key = 7;      // Set on FINISH
  bytes  latest_event_key = 8;
  int32  event_count = 9;
  repeated string transitions = 10;
  repeated string completed_states = 11;
  bytes  resource = 12;          // Serialized request (the R in Request[R,W])
  bytes  latest_response = 13;   // Serialized response from last COMPLETE
  string queue = 14;
  bytes  parent = 15;
  map<string, string> trace_context = 16;
  int64  delay_until = 17;
  bytes  run_after = 18;
  uint64 retry_count = 19;
  string error = 20;
  string error_state = 21;

  // Cluster coordination
  string owner_node = 22;        // Node ID holding the lease
  int64  lease_expiry = 23;      // Unix timestamp; must be refreshed
  int64  lease_epoch = 24;       // Monotonically increasing fencing token

  int64  created_at = 25;
  int64  updated_at = 26;
}
```

### Event Storage

Events are immutable, append-only writes. Each event gets a unique key containing a ULID event version, so keys never collide.

```
PUT fsm/events/<resource_id>/<action>/<run_version>/<event_version>
Headers: If-None-Match: *
Body: Protobuf-encoded StateEvent
```

If the write returns 412 (object already exists), the event was already written (e.g., retry after a crash). This is safe because events are idempotent by key.

### State Transition Flows

#### START (create a new FSM run)

```
1. Acquire resource lock:
   PUT fsm/locks/<type>/<id>/<action>          [If-None-Match: *]
   Body: run_version
   → 412 means another run is active → return AlreadyRunningError

2. Write START event:
   PUT fsm/events/<id>/<action>/<run_ver>/<evt_ver>  [If-None-Match: *]

3. Write run manifest:
   PUT fsm/runs/<id>/<action>/<run_ver>        [If-None-Match: *]
   Body: RunManifest { status: "pending", owner_node: self, ... }
```

All three operations use `If-None-Match: *`, making them individually idempotent. If step 2 or 3 fails after step 1 succeeds, a recovery scan (on startup or periodic) detects orphaned locks by checking whether a corresponding manifest exists.

For queued runs, the lock key includes the run version (`locks/<type>/<id>/<action>/<run_version>`) to allow multiple runs for the same resource.

#### COMPLETE / ERROR / CANCEL (mid-run events)

```
1. Write event:
   PUT fsm/events/<id>/<action>/<run_ver>/<evt_ver>  [If-None-Match: *]

2. Update manifest (CAS):
   GET fsm/runs/<id>/<action>/<run_ver>        [X-Tigris-Consistent: true]
   → read current ETag
   Modify: bump latest_event_key, event_count, append to completed_states
   PUT fsm/runs/<id>/<action>/<run_ver>        [If-Match: <etag>]
   → 412 means concurrent modification → re-read and retry
```

The retry loop on the manifest CAS is bounded (e.g., 10 attempts). In practice, contention on a single run's manifest is near-zero because a single goroutine drives the run sequentially.

#### FINISH (run completed)

```
1. Write FINISH event:
   PUT fsm/events/<id>/<action>/<run_ver>/<evt_ver>  [If-None-Match: *]

2. Update manifest:
   PUT fsm/runs/<id>/<action>/<run_ver>        [If-Match: <etag>]
   Body: RunManifest { status: "complete", end_event_key: ... }

3. Delete resource lock:
   DELETE fsm/locks/<type>/<id>/<action>

4. Write history:
   PUT fsm/history/<date>/<run_ver>            [If-None-Match: *]
```

If the process crashes between steps 2 and 3, the lock is orphaned but the manifest records completion. The archive/recovery process detects this: "manifest says complete, lock still exists → delete the lock."

### Cluster Coordination

#### Run Leasing

Each active run manifest contains an `owner_node`, `lease_expiry`, and `lease_epoch`. A node owns a run if and only if its node ID matches `owner_node` and `lease_expiry` is in the future.

**Claiming a run:** A node reads the manifest. If `owner_node` is empty or `lease_expiry` is in the past, the node writes a new manifest with itself as owner, a fresh lease expiry (now + 30s), and an incremented `lease_epoch`, using `If-Match` to ensure nobody else claimed it simultaneously.

**Heartbeat:** The owning node CAS-updates the manifest every 10 seconds to extend `lease_expiry`. If the CAS fails (another node stole the run), the owning node cancels its local context, causing the running transition to observe `ctx.Done()`.

**Lease expiry:** If a node dies, its lease expires after 30 seconds. Any other node's periodic scan discovers the expired lease and claims the run, resuming from the last completed transition (recorded in `completed_states`).

**Fencing:** The `lease_epoch` is a monotonically increasing integer that acts as a fencing token. It prevents a stale owner (whose lease expired but whose goroutine hasn't noticed yet) from making progress. Every manifest CAS update verifies the epoch matches the value the node claimed with. If it doesn't, the node knows it's been fenced and must stop.

#### Distributed Cancel

Cancelling a run from a node that doesn't own it:

```
1. Read manifest to find owner_node address
2. Send cancel RPC directly to owning node (low latency path)
3. If RPC fails (node unreachable), write a cancel sentinel:
   PUT fsm/cancel/<run_version>                [If-None-Match: *]
4. Owning node checks for cancel sentinels during heartbeat loop
```

The direct RPC path provides sub-millisecond cancel latency when the owning node is reachable. The sentinel path is a fallback with latency bounded by the heartbeat interval.

#### Resumability

For any node to resume a run mid-flight, the manifest must contain sufficient state. The current design already stores:

- The serialized request resource (`resource` field from `ActiveEvent`)
- The list of all transitions and which have completed (`transitions`, `completed_states`)
- The latest response bytes (written into COMPLETE events, also cached in manifest)
- The retry count for the current transition
- The error state if the run was halted

On resume, a node deserializes the resource, reconstructs the `Request` object, skips completed transitions, and continues execution from the next uncompleted transition. This is the same logic as the existing `resume()` function, generalized to work from the manifest rather than from BoltDB.

Transitions SHOULD be idempotent. If a node crashes mid-transition and another node resumes, the transition will execute again. The library cannot guarantee exactly-once execution at the transition level; it provides at-least-once semantics with a fencing token (`lease_epoch`) that external systems can use to reject stale operations.

### Brokered Queue (Distributed Queued Runs)

The queued runner provides concurrency-limited execution: "only N runs of this queue can execute simultaneously." Distributing this requires a shared, consistent view of inflight count across all nodes.

Following turbopuffer's pattern, each queue gets a single JSON file on object storage managed by a stateless broker process.

#### Queue File Structure

```
PUT fsm/queues/<queue_name>
```

```json
{
  "broker": "fdaa:0:1::3:8080",
  "capacity": 5,
  "jobs": [
    {
      "run_version": "01HXYZ...",
      "resource_id": "app-123",
      "action": "deploy",
      "status": "running",
      "claimed_by": "node-a",
      "heartbeat": 1710432000,
      "enqueued_at": 1710431900
    },
    {
      "run_version": "01HXYZ...",
      "resource_id": "app-456",
      "action": "deploy",
      "status": "pending",
      "enqueued_at": 1710431950
    }
  ]
}
```

#### Broker Design

The broker is a lightweight goroutine (or sidecar process) that owns all reads and writes to the queue file. It runs a group commit loop:

1. Accept push (enqueue) and claim (dequeue) requests from FSM nodes over an internal RPC interface.
1. Buffer requests in memory while a write is in-flight.
1. When the current write completes, flush the buffer: apply all pending pushes and claims to the in-memory queue state, then CAS-write the updated queue file.
1. Acknowledge each request only after the CAS write succeeds.

**Throughput:** Each CAS write takes ~5-50ms to Tigris. With group commit, hundreds of enqueue/claim operations batch into a single write. Throughput is bounded by network bandwidth, not write latency.

**Broker discovery:** The broker's address is written into the queue file. Nodes read the queue file to find the broker. If the broker is unreachable (connection timeout), any node can start a new broker and claim ownership by CAS-writing a new broker address into the queue file. The old broker discovers it's been replaced when its next CAS write fails.

**Broker failover:** The broker is stateless; all durable state is in the queue file. Starting a new broker means reading the queue file and resuming the group commit loop. Two brokers running simultaneously is safe because CAS prevents conflicting writes; it's merely slower due to contention, and resolves within one write cycle.

**Worker heartbeats:** Each claimed job in the queue file has a heartbeat timestamp. The claiming node sends periodic heartbeats to the broker, which writes them into the queue file during group commit. If a job's heartbeat is older than a timeout (e.g., 60 seconds), the broker marks it as pending again, allowing another node to claim it.

**Interaction with run manifests:** The queue file tracks which runs are inflight and pending. The run manifest tracks the actual execution state of each run. A node that claims a job from the queue then claims the corresponding run manifest via lease CAS. If the manifest claim fails (another node got there first), the queue entry is released.

#### Scaling Considerations

Each queue is a single file, so all operations for that queue serialize through one broker. For the FSM library's use cases (rate-limiting external APIs, controlling deployment concurrency), this is more than sufficient. If a queue needed thousands of operations per second, it could be sharded by resource type or action, but this is not expected to be necessary.

### Delayed Start and Run-After

**`WithDelayedStart(time.Time)`:** The `delay_until` field is stored in the run manifest. The owning node checks this timestamp before executing transitions. While waiting, it continues heartbeating the lease. If the node dies, the next owner reads `delay_until` from the manifest and waits the remaining time.

```go
func (n *node) runDelayed(ctx context.Context, manifest *RunManifest, fn func()) {
    remaining := time.Until(manifest.DelayUntil)
    if remaining <= 0 {
        fn()
        return
    }
    timer := time.NewTimer(remaining)
    heartbeat := time.NewTicker(10 * time.Second)
    defer heartbeat.Stop()
    defer timer.Stop()
    for {
        select {
        case <-timer.C:
            fn()
            return
        case <-heartbeat.C:
            if err := n.renewLease(ctx, manifest); err != nil {
                return // Lost ownership
            }
        case <-ctx.Done():
            return
        }
    }
}
```

**`WithRunAfter(ulid.ULID)`:** The `run_after` field stores the dependency run version. The owning node polls the dependency's manifest (via consistent read) on an interval until it reaches `status: "complete"`. Lease heartbeating continues during the polling period.

### Archive and History

The background archive loop is simplified compared to the BoltDB version. There is no separate history database; history objects are written at FINISH time. The archive loop becomes a cleanup process:

1. List manifests with `status: "complete"` older than a retention threshold.
1. Verify the history object exists (write it if not, idempotently).
1. Delete event objects under `events/<id>/<action>/<run_version>/`.
1. Delete children entries under `children/<run_version>/`.
1. Update the manifest to `status: "archived"` (or delete it).

S3 lifecycle policies serve as a backstop: configure auto-deletion for objects under the `events/` prefix older than N days.

### In-Process State Cache (memdb)

memdb is retained as a local, in-process cache. It continues to power `Wait()` and `WaitByID()` via `WatchSet` notifications for runs owned by this node. On startup, memdb is populated by listing active run manifests from object storage.

For `Wait()` on a run owned by another node, the method falls back to polling the run manifest in object storage:

```go
func (m *Manager) Wait(ctx context.Context, version ulid.ULID) error {
    // Try local memdb first
    if state := m.localLookup(version); state != nil {
        return m.waitLocal(ctx, version) // Uses WatchSet
    }
    // Fall back to polling the manifest
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
            manifest, err := m.store.GetManifest(ctx, version)
            if err != nil {
                return err
            }
            switch manifest.Status {
            case "complete", "archived":
                if manifest.Error != "" {
                    return errors.New(manifest.Error)
                }
                return nil
            }
        }
    }
}
```

### Store Interface

```go
type Store interface {
    // Append writes an event and updates the run manifest.
    Append(ctx context.Context, run Run, event *fsmv1.StateEvent,
        queue string, opts ...appendOptionFunc) (ulid.ULID, error)

    // Active returns all incomplete runs for the given FSM type.
    Active(ctx context.Context, f *fsm) ([]*activeResource, error)

    // History returns the archived history for a completed run.
    History(ctx context.Context, runVersion ulid.ULID) (*fsmv1.HistoryEvent, error)

    // Children returns child run versions for a parent.
    Children(ctx context.Context, parent ulid.ULID) ([]ulid.ULID, error)

    // GetManifest reads a run manifest with consistent read.
    GetManifest(ctx context.Context, runVersion ulid.ULID) (*RunManifest, string, error)

    // Close shuts down the store.
    Close() error
}
```

The existing `*store` struct is refactored to satisfy this interface as `boltStore`, with no changes to its internal behavior. The new `objectStore` implements the same interface against S3. The `Manager` accepts a `Store` via configuration. When `DBPath` is set, the manager creates a `boltStore` (identical to today's behavior). When `ObjectStorage` is set, it creates an `objectStore`.

Note that `GetManifest` is only used by the object storage backend; the `boltStore` implementation returns `ErrNotSupported`. This method exists on the interface to support the cross-node `Wait()` polling path, which is not relevant in single-node BoltDB deployments.

### Configuration

```go
type Config struct {
    Logger logrus.FieldLogger

    // Storage backend: set DBPath for BoltDB (default, existing behavior),
    // or ObjectStorage for the S3-compatible backend. Exactly one must be set.
    DBPath        string              // BoltDB backend (existing, unchanged)
    ObjectStorage *ObjectStorageConfig // Object storage backend (new, opt-in)

    Queues map[string]int

    // Cluster identity (required only for object storage backend; ignored for BoltDB)
    NodeID   string // Unique identifier for this node
    NodeAddr string // Address reachable by other nodes (for cancel RPC)
}

type ObjectStorageConfig struct {
    Bucket   string // S3 bucket name
    Endpoint string // e.g., "https://fly.storage.tigris.dev"
    Region   string // e.g., "auto" for Tigris
    Prefix   string // Key namespace prefix, default "fsm/"

    LeaseTimeout    time.Duration // Default 30s
    HeartbeatPeriod time.Duration // Default 10s
}
```

Existing consumers that set only `DBPath` get identical behavior to today. The `NodeID`, `NodeAddr`, and `ObjectStorage` fields are additive and have no effect when `DBPath` is used.

## Implementation Plan

The implementation is structured as a series of independently deployable phases. Each phase is backwards compatible and can be rolled back.

### Phase 1: Extract Store Interface

Introduce the `Store` interface. Refactor the existing `*store` struct into `boltStore` satisfying the interface. Update `Manager` to select a backend based on `Config`. No behavior changes for existing consumers; this is pure refactoring. The `boltStore` implementation should be byte-for-byte identical in behavior to the current `*store` — the refactoring is limited to satisfying the new interface and moving the construction into a factory function.

### Phase 2: Implement Object Storage for Events and History

Events and history records are immutable, write-once objects. This is the lowest-risk place to start. Implement `objectStore.appendEvent()` and `objectStore.writeHistory()` using `If-None-Match: *`. Implement `objectStore.listRunEvents()` using `ListObjectsV2` with prefix.

### Phase 3: Implement Run Manifest and Core State Transitions

Implement the run manifest with CAS-based create and update. Implement START, COMPLETE, ERROR, CANCEL, and FINISH flows. Implement the resource lock for one-active-run-per-resource enforcement. This is the core of the new backend.

### Phase 4: Implement Cluster Coordination

Add run leasing (claim, heartbeat, expiry). Add the fencing token (lease_epoch). Add distributed cancel (direct RPC + sentinel fallback). Add the periodic scan for expired leases and orphaned locks.

### Phase 5: Implement Brokered Queue

Implement the queue file format and CAS read-modify-write loop. Implement the stateless broker with group commit. Implement broker discovery and failover. Implement worker heartbeats and stale job reclamation. Wire into the `runnerFromOpts` selection logic.

### Phase 6: Migration Tooling (Optional)

For deployments transitioning from BoltDB to object storage, provide optional tooling: a dual-write mode (write to both backends, read from BoltDB) for validation, a shadow-read mode (write to both, read from object storage, compare with BoltDB) for confidence building, and a one-time export tool to migrate existing BoltDB state to object storage. These tools are not required for new deployments starting directly on object storage, and the BoltDB backend remains fully supported and the default for upstream consumers.

## UI/UX Changes

### CLI / API Surface

No changes to the FSM builder API or transition function signatures. The `Config` struct gains new fields (`ObjectStorage`, `NodeID`, `NodeAddr`), but existing code using `DBPath` continues to work with the BoltDB backend.

The admin gRPC service (`FSMService`) is unchanged in its interface. `ListActive` and `GetHistoryEvent` work transparently against either backend.

### Observability

New metrics:

- `fsm_object_storage_cas_retries_total` — Counter of CAS retry attempts per operation type.
- `fsm_object_storage_latency_seconds` — Histogram of object storage operation latency.
- `fsm_lease_renewals_total` — Counter of successful and failed lease renewals.
- `fsm_queue_broker_commits_total` — Counter of group commit flushes per queue.
- `fsm_queue_depth` — Gauge of pending jobs per queue.

New trace attributes on run spans: `fsm.owner_node`, `fsm.lease_epoch`, `fsm.storage_backend`.

## Failure Modes and Recovery

|Failure                                                |Detection                                                      |Recovery                                                                                    |
|-------------------------------------------------------|---------------------------------------------------------------|--------------------------------------------------------------------------------------------|
|**Crash between event write and manifest update**      |Startup scan: events exist beyond manifest's `latest_event_key`|Reconcile manifest from event log                                                           |
|**Crash between manifest completion and lock deletion**|Archive scan: manifest says "complete" but lock exists         |Delete the orphaned lock                                                                    |
|**Crash between START steps (partial create)**         |Startup scan: lock exists but no manifest                      |Delete the orphaned lock                                                                    |
|**Node dies while owning a run**                       |Lease expiry (30s)                                             |Another node claims the expired lease and resumes                                           |
|**Node dies while owning the queue broker**            |Broker connection timeout from clients                         |Any node starts a new broker, claims via CAS                                                |
|**Two brokers running simultaneously**                 |CAS failure on queue file                                      |Stale broker discovers replacement on next write attempt, shuts down                        |
|**Network partition (split brain)**                    |Lease CAS fails for partitioned node                           |Partitioned node's lease expires; it stops executing. Majority-side node claims.            |
|**Duplicate transition execution after failover**      |Not automatically detected                                     |Transitions SHOULD be idempotent; `lease_epoch` fencing token available for external systems|

## Trade-offs

**Latency.** Each transition now incurs ~10-50ms of object storage write latency (event + manifest CAS) versus ~1ms for BoltDB. This is acceptable because FSM transitions typically represent real work (API calls, provisioning) that dominates the storage overhead.

**Complexity.** The object storage backend has more moving parts than BoltDB: CAS retry loops, lease management, broker process. The trade-off is operational simplicity at the infrastructure level: no volumes to provision, no single-machine dependency, no backup/restore procedures.

**Consistency.** BoltDB provides strict serializability within a single process. Object storage provides linearizability per object via conditional writes. Multi-object atomicity is not available; the design works around this by concentrating coordination in the run manifest and accepting that partial failures between objects require recovery/reconciliation.

**Queue throughput.** The brokered queue serializes all operations for a given queue through a single broker. Under expected workloads (tens to low hundreds of operations per second per queue), this is not a bottleneck. If it becomes one, the queue can be sharded, but this adds operational complexity.

## Abandoned Ideas

### Direct CAS without a run manifest

An early design attempted to use the `ACTIVE` object directly as the CAS target, mirroring BoltDB's bucket structure. This was abandoned because BoltDB's `Append()` atomically touches multiple buckets (ACTIVE + EVENTS + CHILDREN) in a single transaction. Without a manifest, each state transition would require multiple conditional writes with no atomicity guarantee between them, making failure recovery significantly more complex.

### DynamoDB / FoundationDB for coordination

Using a proper database for coordination state was considered. DynamoDB would provide transactions, conditional updates, and TTL-based lease expiry. FoundationDB would provide full ACID transactions. Both were abandoned because they introduce an additional stateful dependency. The goal is to build on object storage alone, keeping the operational surface area minimal. The conditional write primitives on S3/Tigris are sufficient for the coordination patterns required.

### Slot-based distributed semaphore for queues

An alternative to the brokered queue was a slot-based approach: create N slot objects (`queues/<name>/slot-0` through `slot-N`), claim one with `If-None-Match`, release by deleting. This was abandoned because it requires cleanup on node failure (detecting and releasing abandoned slots), doesn't provide FIFO ordering, and has worse behavior under contention (N parallel CAS attempts per claim). The brokered queue provides FIFO, group commit for throughput, heartbeat-based liveness, and a single point of serialization that matches the problem's natural structure.

### Replacing memdb entirely

An earlier version of this design proposed replacing memdb with polling for all `Wait()` calls. This was abandoned because it would add 500ms+ latency to all in-process wait operations, even for runs owned by the local node. memdb is retained as a write-through cache for local runs, with polling as a fallback only for cross-node waits.

### Dropping the queued runner

Removing the queued runner entirely was considered since it's the hardest feature to distribute. The concurrency-limiting behavior can be approximated by other means (HTTP client middleware, in-transition semaphores, ingestion-layer throttling). However, the queue provides this at the FSM scheduling layer with zero awareness required from transitions, and the brokered queue pattern from turbopuffer provides a proven, clean implementation path. The operational cost of one additional lightweight broker process per queue is justified by the abstraction's value.

## Open Questions

1. **Queue broker lifecycle.** Should the broker run as a goroutine within one of the FSM manager nodes (elected via CAS), or as a separate sidecar process? The goroutine approach is simpler to deploy but means the broker competes for resources with FSM execution. The sidecar approach provides isolation but adds a deployment artifact.
1. **Lease timeout tuning.** 30 seconds balances fast failover against false positives from GC pauses or transient network issues. Should this be configurable per-FSM or globally? Too short risks unnecessary failovers; too long delays recovery.
1. **Cross-node Wait() latency.** The 500ms polling interval for cross-node `Wait()` adds observable latency. An alternative is a lightweight pub/sub notification layer (e.g., nodes subscribe to a shared channel for run completions), but this adds another coordination mechanism. Is the polling latency acceptable for known use cases?
1. **History retention policy.** With object storage, history is cheap to store. Should the library define a default retention policy (e.g., 90 days), or leave it entirely to S3 lifecycle policies configured by the operator?
1. **Multi-region behavior.** Tigris automatically replicates globally, but consistent reads require routing to the leader (`X-Tigris-Consistent: true`). Should the library always use consistent reads, or allow eventual consistency for read-only queries like `ListActive` and `History`?
