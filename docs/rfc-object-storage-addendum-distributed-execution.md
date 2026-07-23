# [RFC Addendum] Distributed Execution: RPC Ingress and Event Publishing

|           |                                              |
|-----------|----------------------------------------------|
|**Created**|2026-07-22                                    |
|**Status** |Draft                                         |
|**Parent** |[Object Storage Backend](rfc-object-storage-backend.md)|
|**Target** |superfly/fsm v2                               |

## Overview

The base RFC assumes the `Manager` is embedded in the process that both *submits* runs and *executes* their transitions. This holds for single-process and single-fleet deployments, where the caller of `start()` is also the node that runs the FSM.

Some deployments separate the tier that requests work from the tier that executes it — for example, a stateless request-handling tier fronting a pool of worker processes. This addendum specifies two additive capabilities that make fsm usable in that topology **without introducing any dependency beyond object storage**:

1. A synchronous **RPC ingress** so a non-executing client can submit, wait on, and cancel runs.
2. An **event-publishing** system so consumers can react to run lifecycle transitions in real time.

Both are opt-in. Embedded single-process deployments ignore them and behave exactly as the base RFC describes.

## Design principle: workers own object storage

In the distributed topology, **only worker processes embed the `Manager` and hold object-storage credentials.** Requesting clients never touch the bucket; they reach workers through the RPC ingress. This establishes a single-writer authority boundary — every mutation of run state originates from a worker — which simplifies reasoning about who can create, advance, or delete a run.

This choice has one direct consequence that shapes the ingress design:

**Submission durability moves from object storage to the ingress.** In the embedded model, `start()` persists a pending run to object storage; even if the caller then dies, the run is durably recorded and a worker's claim loop eventually executes it. When the requesting client no longer writes to the bucket, that safety net is gone: a submission that fails to reach a worker leaves *no object-storage trace to reconcile against*, because nothing was written. A lost submission is lost work — categorically worse than a lost *notification*, which is always recoverable by re-reading the manifest.

Therefore the ingress must provide its own durability guarantee, via synchronous persist-then-ack (below). This is the reason the ingress is RPC rather than a best-effort message.

## Part 1: RPC Ingress

fsm already exposes a Connect-RPC admin service (`ListRegistered`, `ListActive`, `GetHistoryEvent`). This addendum extends it into a control API:

| Method | Behavior |
|--------|----------|
|`Start(action, id, resource, opts)`|Worker persists the pending run (manifest + START event + index) to object storage, **then** returns the run version. The ack means *durably submitted*.|
|`Wait(version)`|Server-side blocks against object storage (poll floor, with the event stream as a fast path) until the run completes; returns the terminal state/error.|
|`Cancel(version, cause)`|Writes the durable cancel sentinel, then publishes the cancel subject on the bus; the owning worker reacts immediately or on its next lease heartbeat (see Part 2).|
|`Runs(id)`|Lists the versions of runs recorded for a resource (see base RFC `index/` prefix).|
|`History(version)`|Returns the archived record for a completed run.|

Key properties:

- **Persist-then-ack durability.** `Start` returns only after the pending run is durably in object storage. A failed `Start` means *not submitted*; the client retries. Retries are idempotent on the run id — a second `Start` for an already-active id returns `AlreadyRunning` with the existing version, which the base RFC's resource lock already enforces. Once acked, object storage is authoritative and all downstream operations (execution, `Wait`, events, the claim loop) proceed as designed.
- **Opaque payloads.** The client sends the request as serialized bytes. The worker, which has the FSM's `R`/`W` types registered for that action, decodes with the existing codec system. The client needs only the generated protobuf definitions — not the Go request/response types, and not the fsm library itself.
- **No new dependency.** The transport is `connectrpc.com/connect`, already a dependency. The ingress is a service the worker optionally hosts; it introduces nothing new to the library's dependency set.
- **Execution placement.** A worker that accepts a `Start` persists the pending run but need not execute it itself. The claim loop (base RFC Phase 4) distributes pending runs across the worker pool; the accepting worker is simply the one that recorded the submission. This keeps ingress capacity and execution capacity independent.

## Part 2: Event Publishing and the Event Bus

fsm already writes a durable, ordered, replayable event log — the `events/` prefix, one immutable `StateEvent` per transition, keyed by lexicographically-sortable ULIDs. **The event system exposes and accelerates this existing log; it is not a new store.**

### Interface

An earlier sketch specified a publish-only `EventSink`. Subject-addressed control signals (cancel, below) and worker wakeup need a receive side as well. The two directions are split into single-method interfaces, composed `io.Reader`/`io.Writer`-style, because their consumers are asymmetric: the run loop only publishes; `Wait`, claim wakeup, and the cancel listener only subscribe; an audit or metrics tap is publish-only.

```go
// EventPublisher delivers events AFTER they are durably persisted.
// Best-effort: implementations MUST NOT block callers, and no correctness
// property depends on delivery — the object-storage log and sentinel
// objects remain the source of truth.
type EventPublisher interface {
    Publish(Event)
}

// EventSubscriber registers interest in a subject. Best-effort: a
// subscription that never fires costs latency (the polling floors carry
// everything), never correctness.
type EventSubscriber interface {
    Subscribe(subject string, fn func(Event)) (unsubscribe func(), err error)
}

// EventBus is the injection point: a transport that carries both
// directions. Internal components depend on the narrow half they use.
type EventBus interface {
    EventPublisher
    EventSubscriber
}
```

(Signatures are illustrative; the exact shape is settled at implementation time. The default is a no-op bus — publishes drop, subscriptions never fire.)

`Event` carries the run identity (resource type, id, action, version), the event version (ULID, for ordering and dedup), the event kind, and the current state/error. The kinds derive directly from the persisted `StateEvent` types: run started, transition completed, transition errored, canceled, finished. Granularity is per-transition — free, because every transition is already persisted.

### Subject addressing: by run, never by node

Messages are addressed by **subject keyed on the run**, not by node address (illustrative naming):

| Subject | Purpose |
|---------|---------|
|`fsm.run.pending`|broadcast wakeup — a run became claimable; idle workers claim immediately instead of on the next scan tick|
|`fsm.run.done.<run_version>`|completion — unblocks waiters for that run|
|`fsm.run.cancel.<run_version>`|cancellation signal — the owning worker reacts (see below)|

Because addressing is by subject, **no component ever needs another node's network address**: not the manifest, not object storage, not the library configuration. Network topology stays out of domain objects entirely.

### Invariants

These are load-bearing; the event system is easy to get subtly wrong, and the invariants are what preserve reliability and the object-storage-only guarantee.

1. **Publish after the state a subject announces is fully observable, never before.** A consumer observing an event must be able to trust that everything the event implies has already happened. For most subjects that means after the durable write. For `fsm.run.done.<version>` it means after **finish cleanup** — the manifest flipped to COMPLETE *and* the resource lock deleted *and* the history object written — not merely after the COMPLETE flip: a waiter released at the flip can restart the same resource id into the not-yet-deleted lock (spurious `AlreadyRunning`) or look up history before it exists. (The object backend's poll path gates on exactly this finish-cleanup point for the same reason.)
2. **Best-effort, non-blocking, correctness-neutral — on both sides.** The transition has already committed durably before `Publish` is called; a slow or dead bus must never block, delay, or fail a transition, and delivery is fire-and-forget through a bounded buffer with drop-and-log on overflow. The same rule applies to the subscribe side: a slow subscriber callback must not block the bus or the loops that consume it (heartbeat, claim); implementations dispatch callbacks asynchronously.
3. **The log is the source of truth; the stream is an accelerator.** `Wait` may subscribe for a fast path but reconciles against the manifest and falls back to polling. A dropped event therefore never hangs a waiter. This mirrors the base RFC's "events are an audit trail, not the recovery source."
4. **Every control signal has a durable counterpart.** A signal carried on the bus (cancel, pending) is written to object storage first; the bus only accelerates its discovery. A bus outage degrades latency to the relevant polling floor, never behavior.
5. **No-op default preserves object-storage-only.** The default bus is a no-op; consumers who want the stream can tail the `events/` prefix. A live bus is injected via `Config`. The library never imports a pub/sub client.

### Two delivery tiers

Consumers choose by their reliability need, and neither requires a dependency the library imports:

- **Live stream** — an injected bus (e.g., a pub/sub broker such as NATS or Redis). Low-latency, best-effort. Suited to waiting on completion, waking idle workers, live progress views, and outbound triggers.
- **Durable tail** — the object-storage event log itself. Replayable, ordered, guaranteed. Suited to anything that must not miss an event (audit, metering, backfill), which reconciles against the log rather than trusting the wire.

### The Notifier is subsumed

An earlier sketch proposed a purpose-built "notifier" for cross-node `Wait`. That is unnecessary: `Wait` and worker-wakeup are simply the first two consumers of the event stream. One mechanism serves notification, work-available signaling, and arbitrary downstream consumption.

### Subject-addressed cancel (revises the base RFC's Distributed Cancel)

The base RFC's cancel flow — read the manifest for the owner's address, send a direct RPC to that node, fall back to a sentinel object — is replaced. Routing by stored node address hand-rolls service discovery on top of eventually-stale bucket objects, leaks network topology into domain state, and implies point-to-point worker RPC. Subject addressing removes the need for any of it:

1. **Durable first.** Cancel (arriving via the RPC ingress on any worker) writes a write-once sentinel object `cancel/<run_version>` whose body carries the cancellation cause. Write-once (`If-None-Match: *`) gives first-cancel-wins semantics: a second cancel observes the 412 and reports success — the run is already canceled — rather than an error.
2. **Then accelerate.** The accepting worker publishes `fsm.run.cancel.<run_version>` on the bus.
3. **Owner reacts.** Every worker subscribes to cancel subjects and checks the version against its local running set: the owner cancels the run's context; everyone else ignores the message.
4. **Polling floor.** During each lease heartbeat the owner also checks for sentinels covering the runs it holds — as **one keys-only listing** of the `cancel/` prefix intersected with the owned set, not a GET per run, so the check is a single request per tick regardless of how many runs a node holds (the sentinel body is fetched only on a match, and cancels are rare). With the bus down, cancel latency is bounded by the heartbeat interval — the same bound the base RFC's fallback path already accepted.
5. **Canceled before execution.** A run can be canceled while pending, delayed, or queued — before any run context exists to cancel, and before the run loop that normally records CANCEL/FINISH events has started. Whichever node holds the run (or next claims it) must, on discovering the sentinel, drive the manifest to a terminal canceled state itself — recording the cause, deleting the lock, and writing history — so waiters resolve instead of polling to their deadline. No transitions run and no finalizers fire for a run canceled before execution began: nothing has happened that needs finalizing.

Consequences:

- **`Config.NodeAddr` is removed.** No component needs a routable address for any other component.
- **`owner_node` in the manifest is a node identity** — used for lease fencing and observability — never a network address.
- No worker-to-worker point-to-point RPC exists anywhere in the design, and no network addresses are stored in object storage.

## Part 3: Run-state queries and the memdb boundary

### The current coupling

In the base design, the in-memory index (`go-memdb`) is not a backend detail — it is a `Manager`-level component that both backends write through and the `Manager` reads **directly, bypassing the `Store` interface**, in five places:

| Site | Query |
|------|-------|
|`Manager.Active`|active runs for a resource id|
|`Manager.ActiveChildren`|active children of a parent|
|`Manager.Wait` (checkRun)|watch a run for completion|
|`Manager.WaitByID`|resolve id → version, then watch|
|`adminServer.ListActive`|all active runs on this node|

This encodes an unstated assumption: *there is one authoritative in-process index of every run.* That assumption holds in the single-process model and is **false the moment runs can migrate between nodes.**

### Why it breaks under multi-worker execution

memdb only observes writes from its own process, which produces two failures once a run can be owned by a different node than the one being asked:

1. **It cannot notify across nodes.** A `Wait` for a run this node does not own can never be satisfied by this node's `WatchSet`; the notification has to come from object storage or the event stream regardless.
2. **A stale entry hangs `Wait`.** If a worker holds `V=running` in memdb, loses its lease (GC pause, partition), and another worker completes `V`, the first worker's `checkRun` still finds a *present, non-terminal* entry and parks on a `WatchSet` that will never fire — it does **not** re-consult object storage while an entry is present. The wait blocks until its context deadline even though `V` finished elsewhere. This is a correctness regression that does not exist with a single writer.

This revises — for the multi-worker case only — the base RFC's "Replacing memdb entirely" abandoned-idea reasoning. That reasoning (keep memdb; polling is too slow for in-process waits) is correct for single-process execution and is retained there. It does not carry into multi-worker execution, where memdb cannot serve the cross-node waits that dominate and the event bus, not raw polling, removes the latency objection.

### Resolution: the Store interface owns run-state queries

Push the five reads above behind the `Store` interface so each backend answers appropriately:

- **Bolt backend** answers from memdb, with `WatchSet` for `Wait`. memdb becomes its private implementation detail — invisible above the `Store` boundary.
- **Object backend** answers from object storage: `Active`/`ListActive` from the `locks/` prefix, `WaitByID` and id→version resolution from `locks/` + `index/`, `ActiveChildren` from `children/` + manifests, and `Wait` from the event bus (fast path) with a manifest poll floor (correctness).

memdb thus gains no "multi-worker off switch"; it simply stops being part of the object backend's answer. The decision is **backend-typed, not worker-count-typed** — the object backend answers from object storage whether it runs on one node or fifty, so there is no runtime single/multi mode to detect or misconfigure. A single-node object deployment pays a rare poll-floor cost in place of memdb-instant waits, which the event bus makes negligible (see below).

### The same-node fast path: what is lost, and the deferred replacement

Removing memdb from the object path gives up one thing: the **same-node-owner instant wait** — a worker completing a run cannot notify a co-located waiter without an event-bus round trip or a poll. In the split topology this case is rare: the requesting tier is separate (always cross-node), and even worker-side `run_after` / parent waits are usually on runs some *other* worker owns.

If profiling later shows same-node waits matter, the replacement is **not** memdb but a scoped in-process signal — a `map[runVersion]chan struct{}` that `run()` closes when it finishes a run *this node is actively executing*. It is staleness-free by construction: created at execute-start, fired at execute-end, both within the owning goroutine's lifetime; a lease loss cancels the run, which closes the channel and drops the waiter back to object storage. This is a handful of lines, not an indexed cache with invalidation. It is deliberately deferred until measured.

### Cost note

The concern that dropping memdb means "a GET on every poll" only holds without a notification layer. With the event bus, a `Wait` subscribes and returns on the completion event; polling degrades to a rare insurance floor (with adaptive backoff while the bus is healthy), not a per-interval cost. Removing memdb is therefore not a cost regression once the bus exists — and the bus covers the cross-node majority that memdb never helped. A deployment that stays strictly object-storage-only (no injected bus) achieves the same with an object-native completion marker (a small `done/<version>` object polled in place of the full manifest), which — unlike memdb — works across nodes.

## Interaction with the claim loop

In the distributed topology the Phase 4 claim loop becomes the **primary work-distribution mechanism**, not merely a failover path: workers obtain work by claiming pending runs from object storage. The event stream's "pending" event provides low-latency wakeup so an idle worker claims immediately rather than on its next scan tick; the periodic scan remains the correctness floor.

A broadcast wakeup stampedes by construction: every idle worker hears `fsm.run.pending` and races the claim CAS. That is *safe* — the lease epoch CAS picks exactly one winner — but it costs N lock-scans per wakeup. Workers should therefore apply a small random delay before scanning on a wakeup (jitter), which composes naturally with the claim tick they already run; the winner is simply whoever wakes first.

## Dependency invariant

Object storage remains the only required dependency of the library. The RPC ingress uses the already-present Connect module. The event bus is an injected interface pair (EventPublisher/EventSubscriber) whose default is a no-op. No message broker, queue, or database is introduced. Any pub/sub system a deployment wishes to use for low-latency notification is supplied at the edge as an `EventBus` implementation, exactly as the storage endpoint is supplied as configuration.

## Future work: embedded messaging mesh

An `EventBus` implementation may embed the broker in-process — e.g., an embedded NATS server clustered across the worker fleet — giving deployments live events, wait-unblock, cancel signaling, and claim wakeup with **no separately-operated messaging infrastructure**. The growth path "single process → worker fleet" then requires no infrastructure decision at any point. This is future work, deliberately out of scope for the initial distributed-execution implementation; the `EventBus` boundary is what lets it land later without touching the core.

Guardrails, should it be built:

- **Packaging preserves the dependency invariant.** The mesh ships as a sub-package or nested module implementing `EventBus` — first-class (in-repo, tested, documented) but **not in the core module's dependency graph**. Embedded/Bolt users never pull a broker.
- **Stateless only.** The embedded broker must run without persistence (core messaging; no embedded streaming/storage subsystem). A second durable store inside the workers would violate the source-of-truth invariant. A corollary benefit: stateless clustering has no quorum requirement, so a two-node mesh is valid.
- **Peer discovery is a deployment concern — never object storage.** The mesh seeds its routes from deployment-level configuration: DNS (e.g., a process-group name that resolves to all workers) or a static seed list. Network topology does not belong in the bucket or in domain objects, for the same reasons `NodeAddr` was removed.
- **Failure degrades to the polling floors.** Per the invariants above, a partitioned or down mesh costs latency only. This is the property that makes an in-process mesh acceptable at all.

For small fleets, a broker-less `EventBus` implementation — simple fan-out to DNS-discovered peers — may be all a deployment needs; the interface admits both, and the choice is invisible to the library.

## Open questions

1. **Wait transport.** Server-streaming push vs. long-poll for the RPC `Wait`. Streaming is lower-latency but holds a connection per waiter; long-poll is simpler to scale behind a load balancer.
2. **Event schema stability.** The event payload becomes a public contract once consumers depend on it. Versioning policy and which fields are guaranteed vs. advisory.
3. **Bus backpressure.** On buffer overflow: drop-oldest, drop-newest, or block-with-timeout — but never block the transition. Whether the policy is fixed or configurable.
4. **Ingress vs. execution colocation.** Whether every worker hosts the RPC ingress, or a subset act as ingress "gateways" that persist submissions while the rest execute. Persist-on-receipt plus claim-loop execution makes these independent; the deployment shape is left open.
5. **Result delivery.** How a client retrieves a completed run's response (`W`) — inline in the `Wait` reply, or via `History` after completion.
