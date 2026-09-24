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
- **Logging is `log/slog` via `Config.Logger`; metrics are OpenTelemetry via
  `Config.MeterProvider`** (the global provider by default). Never import a logging framework or a
  metrics registry. Per-transition and per-wait lines are Debug; a run's start and stop, claims
  and shutdown are Info. A run's identity is one `fsm` slog group (`runAttr`, fsm.go), keyed like
  its span and metric attributes; derive a per-transition logger from the run's base logger
  (`Request.base`), never from the previous transition's — slog's `With` appends, so the group
  would repeat. Pass the ctx to the log call wherever one is in scope.

## Architecture map
- `manager.go` — the `Store` interface, declared with the `Manager` that consumes it (interfaces
  live with their consumer, never beside an implementation). **`Start` and `Append` are the two
  entry points to the single mutation path** all run state flows through (`Start` carries the
  START record; `Append` records every later event); run-state queries (`ActiveRuns`, `WaitRun`,
  `Runs`, …) are backend-typed. `Active` (every incomplete run of one FSM) is on `Store` and
  implemented by both backends; the Manager resumes through it unless the backend is a
  `runClaimer`, whose claim loop hands out only the runs this node may take. A backend may also
  satisfy narrow capability views, each declared at its own call site: `appender`
  (interceptor.go), `runClaimer`, `fencer`, `cancelSweeper` (coordinate.go), `cancelRecorder`
  (manager.go), `nodeIdentified` (fsm.go). BoltDB implements none of the lease-shaped ones, so it
  is excluded structurally rather than by a nil check.
- `store.go` — the BoltDB implementation. `boltStore` is memdb-backed and private; the object
  impl is `objectStore`.
- `objstore_store.go` / `objstore_lease.go` / `objstore_cancel.go` / `objstore.go` — the object
  backend: Append + queries + WaitRun; leases and `lease_epoch` fencing (a run that failed to
  resume keeps a `leaseDeferred` slot, which `reserveClaim` refuses until a timer clears it —
  no second map); cancel sentinels; the S3 client, key helpers, and conditional writes.
- `coordinate.go` — the lease-coordinated background loop (`leaseCoordinator`): heartbeat
  (extend leases, sweep for lost leases and cancels), jittered claim pass, event-driven wakeups.
- `eventbus.go` — `EventPublisher`/`EventSubscriber`/`EventBus`, the protobuf `fsmv1.RunEvent`
  payload, run-addressed subjects, no-op default.
- `manager.go` / `fsm.go` / `builder.go` / `runner.go` / `interceptor.go` — the `Manager` API,
  run lifecycle, the fluent FSM builder, runners (queue/delay/run-after), and transition
  interceptors (retry, cancel, finish).
- `admin.go` — the Connect-RPC admin service, served on a unix socket. Proto sources in
  `proto/fsm/v1/`; generated code in `gen/`.
- `metrics.go` — the OTel `instruments`, built once in `New` from the Meter and threaded like
  the tracer (`Manager`, `retry`, `objectStore`); attribute keys and bucket advice live there.
- `fsmtest/fake` — the in-memory S3 (`fake.S3`, with fault hooks and counters) and EventBus
  (`fake.Bus`); a leaf package that never imports `fsm`. `fsmtest` — the consumer-facing harness
  over it: `Backend`, `RunBackends`, `NewObjectBackend` options, `AsymmetricTimings`,
  `Eventually`.

## Build, test, verify
- **Toolchain: Go 1.27** — the builder API uses generic methods, which need it. Plain `go`.
- Verify with, in the root **and** in `eventbus/nats` (a nested module):
  - `go build ./...`
  - `go vet ./...` — works since Go 1.27.1 (1.27rc1's type-checker rejected generic methods);
    its `slog` analyzer is what makes the loose `"key", value` log form safe.
  - `go test -race ./...` — **always `-race`**; it has caught real concurrency bugs repeatedly.
  - `$(go env GOROOT)/bin/gofmt -l .` — the toolchain's gofmt, not the one on `PATH` (an older
    gofmt false-positives on generic methods).
  - `go mod tidy && git diff --exit-code go.mod go.sum` — CI's tidiness gate, in both modules.
- **Proto:** regenerate with `go run github.com/bufbuild/buf/cmd/buf@v1.28.1 generate`.
- CI (`.github/workflows/`) runs the same: a `go` job (tidy + gofmt + build + vet + `-race`) for
  the root and for `eventbus/nats`, `buf`, CodeQL.

## Testing conventions
- New behavior tests run against **both backends**: `func TestX(t){ runBackends(t, testX) }`. The
  object backend uses the in-process `fake.S3`; timing tests set lease/poll intervals via
  `newObjectBackendWith` / `newObjectBackendWithBus`.
- **The cycle rule:** `fsmtest` imports `fsm`, so in-package tests may import only
  `fsmtest/fake`. The `backend` type, `asymmetricTimings` and `eventually` therefore exist
  twice — once in-package (`backend_test.go`, `lease_test.go`, `eventbus_test.go`) and once
  exported in `fsmtest`; keep the two in step.
- Object-backend managers take the fake's client (`Client: s3.Client()`, static credentials), so
  nothing calls `t.Setenv` except the one test of the `Endpoint`-built client.
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
