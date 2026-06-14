---
type: State Machine
title: GroupBroadcast Queue Model
description: Specifies GroupBroadcast queue ownership, route states, transitions, and completion rules.
status: draft
---

# GroupBroadcast Queue Model

This document refines the high-level `GroupBroadcast` draft in
[`communication_protocol_spec.md`](./communication_protocol_spec.md) into an
implementation-oriented queue and state-machine model.

## 1. Scope

This document specifies:

- the queue entities owned by `GroupBroadcast`
- the per-route state machine
- terminal outcomes and compaction rules
- ownership boundaries between replication, discovery, broadcast, and persistence
- crash-recovery expectations for `Durable` work

This document does not yet specify:

- concrete transport selection or UDP/TCP binding
- wire codecs or relay ack payload formats
- the public replication API shape for submitting broadcast work

Those belong in follow-up tasks.

## 2. Ownership Boundaries

### 2.1 Replication

Replication owns:

- payload semantics
- `delivery_class`
- `message_ref`
- the logical membership and relay configuration for the group
- the decision that one payload should be broadcast at all

Replication does not own:

- per-route retry state
- direct-send scheduling
- relay-store confirmation tracking
- recovery of in-flight `Durable` fan-out work after restart

### 2.2 Discovery Route Publication

Peer announcement and route establishment together feed discovery route
publication. That discovery side owns:

- identity verification
- current reachability classification
- address freshness
- the distinction between `Known`, `Reachable`, and `Stale`

Broadcast consumes verified discovery route output as input. It must not listen
for peer announcements, probe routes, or authenticate peers on its own.

### 2.3 GroupBroadcast

Broadcast owns:

- expanding one accepted envelope into per-route work entries
- queue admission and ordering
- the per-route active state
- retry and backoff scheduling
- relay-store confirmation waiting
- terminal-outcome bookkeeping
- deciding when active work can be compacted

Broadcast is keyed by stable envelope identity. The minimum stable key is:

- `group_id`
- `message_id`
- logical route id

### 2.4 Persistence

Persistence owns:

- storing `Durable` active work so it survives process restart
- storing terminal bookkeeping long enough for dedupe and compaction policy
- replaying stored work back into broadcast at startup

Persistence does not decide state transitions. It records the state chosen by
broadcast.

## 3. Queue Entities

### 3.1 Broadcast Work Item

One accepted `GroupMessageEnvelope` becomes one broadcast work item.

Each work item contains:

- one immutable envelope
- zero or more member routes
- zero or more relay routes
- enqueue timestamp
- expiry deadline or retention policy

The local sender is not a route.

### 3.2 Route Entry

Each route entry is independent and has exactly one of:

- one active state
- one terminal outcome

Member routes and relay routes are never merged. A relay route reaching
`StoredAtRelay` does not complete any member route.

### 3.3 Route Selection

The base model uses:

- one member route for every remote group member
- one relay route for every configured relay

Future policy may prune or specialize that route set, but the queue model does
not depend on the transport choice.

### 3.4 Direct Attempts and Shared Coverage

A route is a logical delivery obligation, not a concrete socket operation.

One direct attempt may cover:

- exactly one route
- several member routes at once

Shared direct attempts are expected for cases such as local-network
broadcast or multicast where one accepted send can satisfy best-effort
delivery for multiple active member routes.

When several routes share one direct attempt:

- every covered route enters `AttemptingDirect`
- one transport-facing success or failure is applied to every covered route
- a successful shared best-effort attempt fulfills the direct obligation for
  every covered member route, so no later per-route individual send is needed

## 4. Active States

### 4.1 `Queued`

The route was accepted into the queue, but no direct attempt has started yet.

Allowed for:

- member routes
- relay routes
- `Durable`
- `BestEffort`

### 4.2 `AttemptingDirect`

Broadcast has started one direct send attempt for this route and is waiting
for the transport-facing outcome.

More than one route may simultaneously reference the same shared direct attempt.
This is especially relevant for best-effort member routes on the local network.

Allowed for:

- member routes
- relay routes
- `Durable`
- `BestEffort`

### 4.3 `AwaitingRelayStore`

The direct send to a relay was accepted locally, but the broadcaster still
needs an explicit relay-store confirmation before the relay route is complete.

Allowed only for:

- relay routes
- `Durable`

This state must never be used for member routes.

### 4.4 `PendingRoute`

The route still requires work, but no direct attempt is currently in flight.
The entry is waiting for either:

- a retry timer
- fresh reachability
- local resource availability

Allowed only for:

- `Durable`

`BestEffort` routes must not remain in `PendingRoute`, because `BestEffort`
does not promise later recovery or replay.

## 5. Terminal Outcomes

### 5.1 `Delivered`

The broadcaster considers the direct fan-out obligation for this route
complete.

For this task, `Delivered` means:

- a direct attempt was accepted by the chosen transport/runtime
- no stronger queue action is still required for that route

Optional `DeliveryReceipt` messages may strengthen confidence and accelerate
compaction, but they do not introduce a separate active state and do not gate
transition to `Delivered`.

### 5.2 `StoredAtRelay`

The relay acknowledged durable storage keyed by the envelope identity and clear
header fields.

This outcome is valid only for relay routes.

### 5.3 `Expired`

The broadcaster will not perform further work for this route.

Typical reasons:

- `BestEffort` route was unreachable when examined
- direct attempt failed and the delivery class does not allow retry
- retry budget or retention deadline was exhausted
- the group or message was superseded by local policy

### 5.4 Cleanup Rule

Terminal outcomes are not retained as active local route state.

When one route reaches `Delivered`, `StoredAtRelay`, or `Expired`:

- the active queue entry for that route is removed
- any retained record becomes separate terminal bookkeeping used for dedupe,
  receipts, diagnostics, or work-item completion
- the route must not continue to participate in active scheduling

## 6. State Invariants

- Every route has exactly one active state or one terminal outcome.
- `BestEffort` routes never survive restart.
- `PendingRoute` is only for `Durable` routes.
- `AwaitingRelayStore` is only for `Durable` relay routes.
- Relay success never implies member delivery success.
- `DeliveryReceipt` is advisory for compaction, not required for correctness.
- Retries for one logical message reuse the same `message_id`.
- Terminal outcomes trigger cleanup of active route state instead of persisting
  as active queue entries.

## 7. Legal Transitions

### 7.1 Enqueue

When a new work item is accepted:

- every route starts in `Queued`
- `Durable` work must be persisted before enqueue success is exposed upward
- `BestEffort` work may stay memory-only

### 7.2 Member Routes

Transitions are evaluated per route. Several member routes may share one direct
attempt, such as a local-network best-effort broadcast. In that case, one
transport-facing success or failure drives the corresponding per-route
transition for every covered route.

| Current            | Event                                                      | Next               | Notes                                              |
| ------------------ | ---------------------------------------------------------- | ------------------ | -------------------------------------------------- |
| `Queued`           | route currently reachable and scheduler starts send        | `AttemptingDirect` | First direct attempt begins.                       |
| `Queued`           | route not currently reachable and class is `Durable`       | `PendingRoute`     | Wait for retry or fresh reachability.              |
| `Queued`           | route not currently reachable and class is `BestEffort`    | `Expired`          | One-shot delivery class does not keep pending work. |
| `AttemptingDirect` | direct handoff accepted                                    | `Delivered`        | Queue obligation ends for this member route.       |
| `AttemptingDirect` | transient failure and class is `Durable`                   | `PendingRoute`     | Retry later with same envelope identity.           |
| `AttemptingDirect` | transient failure and class is `BestEffort`                | `Expired`          | No later retry.                                    |
| `AttemptingDirect` | permanent local failure                                    | `Expired`          | Example: invalid local route configuration.        |
| `PendingRoute`     | reachability becomes fresh enough and scheduler starts send | `AttemptingDirect` | Retry begins.                                      |
| `PendingRoute`     | expiry deadline reached                                    | `Expired`          | Retry window closed.                               |

### 7.3 Relay Routes for `BestEffort`

`BestEffort` relay routes behave like one-shot direct routes:

| Current            | Event                                     | Next               | Notes                                         |
| ------------------ | ----------------------------------------- | ------------------ | --------------------------------------------- |
| `Queued`           | relay reachable and scheduler starts send | `AttemptingDirect` | Direct relay send begins.                     |
| `Queued`           | relay unreachable                         | `Expired`          | No persistence or retry guarantee.            |
| `AttemptingDirect` | direct handoff accepted                   | `Delivered`        | For `BestEffort`, no relay-store wait exists. |
| `AttemptingDirect` | send failure                              | `Expired`          | One-shot semantics.                           |

### 7.4 Relay Routes for `Durable`

`Durable` relay routes have the extra store-confirmation step:

| Current              | Event                                                                | Next                 | Notes                                                           |
| -------------------- | -------------------------------------------------------------------- | -------------------- | --------------------------------------------------------------- |
| `Queued`             | relay currently reachable and scheduler starts send                  | `AttemptingDirect`   | First direct relay attempt begins.                              |
| `Queued`             | relay not currently reachable                                        | `PendingRoute`       | Retry later.                                                    |
| `AttemptingDirect`   | direct handoff accepted                                              | `AwaitingRelayStore` | Transport accepted bytes, but durable obligation is still open. |
| `AttemptingDirect`   | direct attempt failed before acceptance                              | `PendingRoute`       | Retry later.                                                    |
| `AwaitingRelayStore` | matching relay-store confirmation received                           | `StoredAtRelay`      | Durable relay obligation complete.                              |
| `AwaitingRelayStore` | timeout, disconnect, or ambiguous transport loss before confirmation | `PendingRoute`       | Replay conservatively; storage was not proven.                  |
| `PendingRoute`       | relay becomes reachable and scheduler starts send                    | `AttemptingDirect`   | Retry begins.                                                   |
| `PendingRoute`       | expiry deadline reached                                              | `Expired`            | Retry window closed.                                            |

## 8. Overall Work-Item Completion

One broadcast work item remains active while any route remains in an active
state.

A work item becomes compactable only once every route is terminal.

This implies:

- a durable relay copy does not automatically clear pending member routes
- member retry can continue after one or more relays already reached
  `StoredAtRelay`
- overall message completion is the conjunction of route completion, not a
  separate shadow state machine

## 9. Crash Recovery Rules

### 9.1 `BestEffort`

`BestEffort` work is dropped on restart. Recovery is not required.

### 9.2 `Durable`

`Durable` work must be replayable after restart.

Conservative rehydration rules:

- persisted `Queued` stays `Queued`
- persisted `PendingRoute` stays `PendingRoute`
- persisted `AttemptingDirect` rehydrates as `PendingRoute`
- persisted `AwaitingRelayStore` rehydrates as `PendingRoute`
- retained terminal bookkeeping stays terminal

The key rule is to prefer safe replay over assuming a send or relay-store step
completed before the crash.

## 10. Internal vs External Facts

The following are semantic or externally meaningful:

- enqueue accepted or rejected
- per-route terminal outcome
- optional delivery receipt observation
- work item fully complete and compactable

The following are internal bookkeeping:

- retry counters
- backoff deadlines
- chosen transport/session/socket
- transport-local ids such as `TransmissionId`
- last failure cause
- scheduler ordering between equally eligible routes

Later tasks may expose some of that data for diagnostics, but it is not part of
the broadcast contract itself.

## 11. Consequences for Follow-Up Tasks

This state model deliberately leaves four follow-up questions open:

- how direct attempts map onto UDP versus TCP
- what exact relay-store confirmation message shape is used
- which Rust types and runtime traits should carry the model
- how much of this route model should be shared with the single-recipient
  durable-delivery mailbox protocol

Those are handled by the transport-mapping, domain-types, runtime-boundary,
and mailbox-protocol tasks created under `flotsync-yd6`.
