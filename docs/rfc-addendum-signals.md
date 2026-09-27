# [RFC Addendum] Signals: Durable, Typed Messages to a Running Run

|           |                                              |
|-----------|----------------------------------------------|
|**Created**|2026-09-26                                    |
|**Status** |Draft                                         |
|**Parent** |[Object Storage Backend](rfc-object-storage-backend.md), [Distributed Execution](rfc-object-storage-addendum-distributed-execution.md)|
|**Target** |ampbase-io/fsm v2                             |

## Overview

fsm has one durable, cluster-visible way to reach a running run: cancel. Cancel ends the run. Many workflows also need to tell a run something without ending it — pause, resume, approve, advance — from any node, and have the run act on it inside a transition that is already executing.

This addendum specifies **signals**: a named, typed message sent to a run version, recorded durably when it is accepted, and read by whichever transition asks for that name. The model is the workflow signal of Cadence and Temporal, adapted to a library with no replay:

- **Signal the run, not a transition.** A signal is addressed to a run version; any transition of that run may read it.
- **Named and typed.** Each name is bound to a Go type when it is declared, and its payload is encoded with a codec probed from that type, as requests and responses are.
- **Buffered per name.** Reading one name never consumes another. A signal nobody has read waits for a transition that reads it.
- **Recorded when accepted.** The signal is written to the run's event log before the send returns.
- **Never ends a run.** Cancel stays its own mechanism.

Signals are opt-in per FSM. An FSM that declares none pays nothing: no goroutine, no listing, no subscription.

## API

A signal is declared once, as a typed handle:

```go
var (
    Pause   = fsm.NewSignal[Command]("pause")
    Advance = fsm.NewSignal[Command]("advance")
)
```

Several names may bind the same type. The codec is probed from `*T`: a `proto.Message` uses protobuf, a type implementing `fsm.Codec` uses itself, anything else JSON. A type no codec can handle fails the build.

An FSM declares the signals it accepts on `End`:

```go
End("done", fsm.WithSignals[Req, Resp](Pause, Advance))
```

Names must be unique within an FSM.

**Sending.** From any node embedding the `Manager`:

```go
id, err := Advance.Send(ctx, m, runVersion, &Command{Reason: "looks good"})
```

`Send` returns the signal's ID, the same ID the receiving transition sees.

A client without a `Manager` uses the `Signal` RPC, which carries the name and the payload as bytes. The worker decodes the bytes with the declared signal's codec before accepting, as `Start` does for requests.

**Receiving.** A transition receives on a channel, so it can `select` over signals beside its own timers and its context:

```go
select {
case p := <-Pause.Receive(req):   // p.Msg is *Command; p.ID and p.SentAt come from fsm
case a := <-Advance.Receive(req):
case <-ticker.C:
case <-ctx.Done():
}
```

`Receive` on a handle the run's FSM did not declare panics with a message naming the signal: a programming error, never a silent channel that blocks forever.

## Delivery semantics

- **At least once.** A signal a transition receives is consumed when that transition's COMPLETE is recorded. If the transition's attempt fails and retries, or the run is taken over before COMPLETE, the transition receives it again. Transitions already run at least once; a signal handler that must act once dedupes on the signal ID, which is stable across redeliveries.
- **Consumption is a set of IDs.** COMPLETE records the IDs of the signals the transition received. A per-name cursor would not do: see ordering.
- **Ordering is best effort.** Signals of one name are offered in ID order among those visible when the owner looks. The ID is a ULID minted by the accepting node, so two signals accepted close together on different nodes can become visible out of ID order. A consumer must never skip a signal because its ID is older than one it has applied; dedupe on the set of applied IDs.
- **Only a receive consumes.** Delivery uses an unbuffered channel per name, so a signal counts as received only when the handler's receive completes. A transition that never reads a name consumes none of its signals by completing.
- **Unread signals wait.** A signal nobody reads waits for a later transition that reads its name. Signals still unread when the run finishes are discarded.
- **Refused at the door.** A run that is finished or unknown refuses with `ErrFsmNotFound`. A name the run's FSM did not declare, or a payload that does not decode as the declared type, refuses with an error, `InvalidArgument` over the RPC. A typed `Send` can only hit these through a programming error, since its handle carries both. A refused signal is never recorded.
- **Addressed to the run.** A signal sent while one transition runs but read after the next one starts reaches the next one. A consumer that cares which step a signal was meant for carries that in the payload.

## Interactions

- **Retries.** Each attempt of a transition starts with every unconsumed signal on offer again, including those a failed attempt received.
- **`RepeatWhile`.** Every iteration records its own COMPLETE, so each consumes what it received. The predicate may receive signals too; they belong to the attempt it decides, so a signal it receives before `RepeatAgain` is not offered again to that iteration's body, and one it receives before `RepeatDone` is consumed by the COMPLETE that finishes the transition.
- **A name no longer accepted.** A stored signal whose name the running definition does not accept, left by an earlier definition, is logged and never offered.
- **Cancel.** Cancel ends the run; signals never do. Unread signals are discarded with the run.
- **Interceptors.** Signal delivery is not a caller-visible interceptor, and a caller's interceptor sees nothing of it. Internally, one step inside the retry puts received signals back on offer at the start of each attempt.
- **The finisher and finalizers** receive no signals.

## Storage

### Object storage

Sending a signal:

1. Read the run's manifest; refuse a missing or terminal run.
2. Resolve the run's FSM from the manifest's type and action, and validate the name and payload against its declaration.
3. Write the SIGNAL event — an immutable, write-once event object under the run's `events/` prefix — carrying the signal's ID, name and payload.
4. Write the pending marker `signals/<run_version>/<signal_id>`, carrying the same signal.
5. Read the manifest again. With no transaction to hold the run live across the writes, a run that finished meanwhile would never deliver the signal, so its event and marker are deleted and the send refuses with `ErrFsmNotFound`.
6. Publish `fsm.run.signal` on the bus, if one is configured.

The sender never writes the manifest: the lease owner remains its only writer, and every manifest write stays fenced by lease epoch.

Delivering:

- A run lists its own `signals/<run_version>/` prefix, keys only, when it starts executing on a node. The owner's coordinate loop lists it again once per heartbeat, and on the bus broadcast, for each run it is executing whose FSM accepts signals. A node executing no such run lists nothing.
- A run's mailbox fetches the bodies of markers it has not seen and offers them to the run's transitions.
- COMPLETE carries the received IDs (`StateEvent.consumed_signals`). The manifest CAS appends them to `RunManifest.consumed_signals`, and the owner then deletes their markers. A marker whose delete fails stays harmless: its ID is on the manifest, so it is never offered again.
- A takeover or restart reads the manifest's consumed IDs with the rest of the run's state, and offers every marker not in that set.
- Markers still pending when the run finishes are inert, since no node executes the run, and retention removes them with the run's other objects. FINISH does not delete them: that would cost a listing on every run's finish for a case that is rare, since consumed markers are already gone.

### BoltDB

BoltDB is single-process. Sending writes the SIGNAL event and a pending entry in a `SIGNALS` bucket in one transaction, which first checks the run is still active, so a send cannot land after the run's FINISH; it then wakes the run's mailbox in-process. The COMPLETE that consumes signals deletes their entries in its own transaction. A restart offers every remaining entry.

## Event bus

`Signal` publishes a `RUN_EVENT_KIND_SIGNAL` event on `fsm.run.signal`, a broadcast every worker hears; the owning node reacts, as with cancel. The event is an accelerator only: the heartbeat listing is the floor, and a dropped event delays delivery by at most one heartbeat. Subject prefixes configured on the bus apply as they do to every run event.

## RPC

The admin service gains:

| Method | Behavior |
|--------|----------|
|`Signal(version, name, payload)`|Validates and durably records the signal, then returns its ID. `NotFound` for a finished or unknown run; `InvalidArgument` for an undeclared name or a payload that does not decode.|

## History visibility

The SIGNAL event is part of the run's durable event log. The current `History` API returns a run's start record and its last event only, so signals — like every intermediate transition event — are not readable through it. Exposing a run's event log is a separate, general addition and is not part of this addendum. A consumer that shows "who signaled what, when" records that in its own audit trail when it sends.

## Rejected alternatives

- **A mailbox addressed to the running transition.** Signals would be lost between transitions or read by the wrong one. Addressing the run and buffering per name is the workflow-signal model.
- **Consume on hand-off.** Deleting a signal when it is handed to the transition loses it if the node dies before the handler acts. Consumption at COMPLETE matches the at-least-once contract transitions already have.
- **An explicit `Ack`.** A per-signal durable write mid-transition would give at-most-once from that point. The consumer's own fenced record can dedupe on the signal ID with no library write; add `Ack` if a consumer needs it without such a record.
- **Cancel as a reserved signal.** Cancel ends a run and a signal never does, and cancel's first-cancel-wins sentinel is the point. They share only the sweep's listing shape.
- **Run-level pause and resume.** A pause usually lands mid-step, where only the transition can honor it; a run-level pause would still need a signal to reach the step.

## Future work

- An `AwaitSignal` helper for a transition that only waits for one signal.
- Signal-with-start: send a signal atomically with a run's START.
- An API that lists a run's event log, which would make signals visible in history.
