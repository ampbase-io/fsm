# fsm

A library for durable, resumable finite-state-machine workflows. Two storage backends sit
behind one `Store` interface: **BoltDB** (embedded, single-process) and **object storage**
(S3-compatible, multi-node). fsm is being evolved into a distributed orchestrator; the object
backend's distributed-execution design is the active work.

## Design docs (authoritative — read before changing the object backend, leases, or the bus)
- `docs/rfc-object-storage-backend.md` — the object backend: manifests, `locks/`, `index/`,
  `events/`, leases.
- `docs/rfc-object-storage-addendum-distributed-execution.md` — RPC ingress, the EventBus, and
  subject-addressed cancel.

## Hard constraints
- **Object storage is the only required dependency.** No broker/queue/DB in `go.mod`. The
  `EventBus` is an injected interface with a no-op default — never import a pub/sub client. RPC
  uses `connectrpc.com/connect` (already a dep).
- **Worker-owns-all topology.** Only worker processes embed the `Manager` and hold bucket
  credentials; the request tier is a thin Connect client. Submission durability lives in the
  ingress — the RPC `Start` persists then acks, synchronously.
- **Manifests are keyed by run version alone** (`runs/<run_version>`). Id/tuple lookups go
  through `locks/` (active) and `index/` (history). Don't re-key.
- **The object-storage event log is the source of truth.** The bus is an accelerator: publish
  only after the durable write, never block a transition, tolerate dropped events. `owner_node`
  is a node identity, never a routable address — there is no worker-to-worker RPC in the design.

## Architecture map
- `store.go` — the `Store` interface. **`Append` is the single mutation path** all run state
  flows through; run-state queries (`ActiveRuns`, `WaitRun`, `Runs`, …) are backend-typed.
  BoltDB impl `boltStore` is memdb-backed and private; object impl is `objectStore`.
- `objstore_store.go` / `objstore_lease.go` / `objstore_cancel.go` / `objstore.go` — the object
  backend: Append + queries + WaitRun; leases and `lease_epoch` fencing; cancel sentinels; the
  S3 client, key helpers, and conditional writes.
- `coordinate.go` — the lease-coordinated background loop (`leaseCoordinator`): heartbeat
  (extend leases, sweep for lost leases and cancels), jittered claim pass, event-driven wakeups.
- `eventbus.go` — `EventPublisher`/`EventSubscriber`/`EventBus`, the protobuf `fsmv1.RunEvent`
  payload, run-addressed subjects, no-op default.
- `manager.go` / `fsm.go` / `builder.go` / `runner.go` / `interceptor.go` — the `Manager` API,
  run lifecycle, the fluent FSM builder, runners (queue/delay/run-after), and transition
  interceptors (retry, cancel, finish).
- `admin.go` — the Connect-RPC admin service, served on a unix socket. Proto sources in
  `proto/fsm/v1/`; generated code in `gen/`.

## Build, test, verify
- **Toolchain: Go 1.27rc1** — the builder API uses generic methods, which need it. Plain `go`.
- **`go vet` is broken repo-wide** — its type-checker rejects generic methods. Don't run it.
  Verify with:
  - `go build ./...`
  - `go test -race ./...` — **always `-race`**; it has caught real concurrency bugs repeatedly.
  - `$(go env GOROOT)/bin/gofmt -l .` — the toolchain's gofmt, not the one on `PATH` (an older
    gofmt false-positives on generic methods).
- **Proto:** regenerate with `go run github.com/bufbuild/buf/cmd/buf@v1.28.1 generate`.
- CI (`.github/workflows/`) runs the same: a `go` job (build + `-race` + gofmt), `buf`, CodeQL.

## Testing conventions
- New behavior tests run against **both backends**: `func TestX(t){ runBackends(t, testX) }`. The
  object backend uses an in-process fake S3 (`fakeS3`); timing tests set lease/poll intervals via
  `newObjectFactoryWith` / `newObjectFactoryWithBus`.
- **Prove a test catches its bug**: temporarily revert the fix with the editor and confirm the
  test fails — never `git checkout <file>`, which discards all uncommitted work in that file.

## Code style
- Follow Dave Cheney's Go guidance: guard clauses / keep-to-the-left, no `if/else` pyramids,
  extract named helpers, initialize once, no derivable or redundant state.
- Retry with `github.com/cenkalti/backoff/v4` — don't hand-roll retry loops.
- The generic plumbing on `Manager` (`Register`, `start`, `finisher`, `resumeOne`) uses Go 1.27
  generic methods; new R/W helpers follow. A helper whose receiver-shaped argument is an
  interface must stay a plain function — interfaces can't carry generic methods.

## Pull requests
- Branch off `main` (or stack on a parent PR's branch); PRs are squash-merged. Run the full
  verify loop before opening. Keep PRs scoped — stacked PRs are fine for a large slice.
