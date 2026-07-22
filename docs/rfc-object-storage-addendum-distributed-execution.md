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
|`Cancel(version, cause)`|Signals cancellation of a run.|
|`Runs(id)`|Lists the versions of runs recorded for a resource (see base RFC `index/` prefix).|
|`History(version)`|Returns the archived record for a completed run.|

Key properties:

- **Persist-then-ack durability.** `Start` returns only after the pending run is durably in object storage. A failed `Start` means *not submitted*; the client retries. Retries are idempotent on the run id — a second `Start` for an already-active id returns `AlreadyRunning` with the existing version, which the base RFC's resource lock already enforces. Once acked, object storage is authoritative and all downstream operations (execution, `Wait`, events, the claim loop) proceed as designed.
- **Opaque payloads.** The client sends the request as serialized bytes. The worker, which has the FSM's `R`/`W` types registered for that action, decodes with the existing codec system. The client needs only the generated protobuf definitions — not the Go request/response types, and not the fsm library itself.
- **No new dependency.** The transport is `connectrpc.com/connect`, already a dependency. The ingress is a service the worker optionally hosts; it introduces nothing new to the library's dependency set.
- **Execution placement.** A worker that accepts a `Start` persists the pending run but need not execute it itself. The claim loop (base RFC Phase 4) distributes pending runs across the worker pool; the accepting worker is simply the one that recorded the submission. This keeps ingress capacity and execution capacity independent.

## Part 2: Event Publishing

fsm already writes a durable, ordered, replayable event log — the `events/` prefix, one immutable `StateEvent` per transition, keyed by lexicographically-sortable ULIDs. **The event system exposes and accelerates this existing log; it is not a new store.**

### Interface

```go
// EventSink receives lifecycle events AFTER they are durably persisted.
// Delivery is best-effort and asynchronous; implementations MUST NOT block
// or return errors that affect the caller. The events/ prefix in object
// storage remains the source of truth.
type EventSink interface {
    Publish(Event)
}
```

`Event` carries the run identity (resource type, id, action, version), the event version (ULID, for ordering and dedup), the event kind, and the current state/error. The kinds derive directly from the persisted `StateEvent` types: run started, transition completed, transition errored, canceled, finished. Granularity is per-transition — free, because every transition is already persisted.

### Invariants

These are load-bearing; the event system is easy to get subtly wrong, and the invariants are what preserve reliability and the object-storage-only guarantee.

1. **Publish after the durable write, never before.** A consumer observing a "finished" event must be able to trust that the manifest already reflects it. Publishing before the write would surface phantom events on crash.
2. **Best-effort, non-blocking, correctness-neutral.** The transition has already committed durably before `Publish` is called. A slow or dead sink must never block, delay, or fail a transition. Delivery is fire-and-forget through a bounded buffer with drop-and-log on overflow.
3. **The log is the source of truth; the stream is an accelerator.** `Wait` may subscribe for a fast path but reconciles against the manifest and falls back to polling. A dropped event therefore never hangs a waiter. This mirrors the base RFC's "events are an audit trail, not the recovery source."
4. **No-op default preserves object-storage-only.** The default sink is a no-op; consumers who want the stream can tail the `events/` prefix. A live sink is injected via `Config`. The library never imports a pub/sub client.

### Two delivery tiers

Consumers choose by their reliability need, and neither requires a dependency the library imports:

- **Live stream** — an injected sink (e.g., a pub/sub broker such as NATS or Redis). Low-latency, best-effort. Suited to waiting on completion, waking idle workers, live progress views, and outbound triggers.
- **Durable tail** — the object-storage event log itself. Replayable, ordered, guaranteed. Suited to anything that must not miss an event (audit, metering, backfill), which reconciles against the log rather than trusting the wire.

### The Notifier is subsumed

An earlier sketch proposed a purpose-built "notifier" for cross-node `Wait`. That is unnecessary: `Wait` and worker-wakeup are simply the first two consumers of the event stream. One mechanism serves notification, work-available signaling, and arbitrary downstream consumption.

## Interaction with the claim loop

In the distributed topology the Phase 4 claim loop becomes the **primary work-distribution mechanism**, not merely a failover path: workers obtain work by claiming pending runs from object storage. The event stream's "pending" event provides low-latency wakeup so an idle worker claims immediately rather than on its next scan tick; the periodic scan remains the correctness floor.

## Dependency invariant

Object storage remains the only required dependency of the library. The RPC ingress uses the already-present Connect module. The event sink is an injected interface whose default is a no-op. No message broker, queue, or database is introduced. Any pub/sub system a deployment wishes to use for low-latency notification is supplied at the edge as an `EventSink` implementation, exactly as the storage endpoint is supplied as configuration.

## Open questions

1. **Wait transport.** Server-streaming push vs. long-poll for the RPC `Wait`. Streaming is lower-latency but holds a connection per waiter; long-poll is simpler to scale behind a load balancer.
2. **Event schema stability.** The event payload becomes a public contract once consumers depend on it. Versioning policy and which fields are guaranteed vs. advisory.
3. **Sink backpressure.** On buffer overflow: drop-oldest, drop-newest, or block-with-timeout — but never block the transition. Whether the policy is fixed or configurable.
4. **Ingress vs. execution colocation.** Whether every worker hosts the RPC ingress, or a subset act as ingress "gateways" that persist submissions while the rest execute. Persist-on-receipt plus claim-loop execution makes these independent; the deployment shape is left open.
5. **Result delivery.** How a client retrieves a completed run's response (`W`) — inline in the `Wait` reply, or via `History` after completion.
