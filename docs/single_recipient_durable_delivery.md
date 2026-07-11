---
type: Protocol
title: Single-Recipient Durable Delivery
description: Specifies sender, recipient, and relay rules for single-recipient delivery with crash-survival expectations.
status: draft
---

# Single-Recipient Durable Delivery

This document defines a top-level protocol abstraction for durable delivery to
one specific recipient outside normal `GroupBroadcast` fan-out.

Its primary use is bootstrap traffic such as:

- migration proposals
- new-group establishment
- recipient-specific key packages

The key requirement is that delivery must work before a shared group key exists.

## 1. Scope

This document specifies:

- sender-side delivery semantics for one recipient-addressed durable message
- the relay mailbox contract assumed by the protocol
- recipient mailbox check-in, fetch, and ack flow
- how this channel relates to `GroupBroadcast` and `Replication`

This document does not yet specify:

- relay storage internals or mailbox indexing implementation details
- transport binding onto UDP or TCP
- exact Rust types or public API changes

## 2. Relationship to Other Sub-Protocols

### 2.1 Discovery Route Publication

Peer announcement and route establishment together feed discovery route
publication. That discovery side owns:

- whether the recipient is currently reachable directly
- how relays are reached
- identity verification for peers and relays

Single-recipient delivery consumes verified route information. It does not
listen for peer announcements, probe routes, or authenticate peers on its own.

### 2.2 GroupBroadcast

This protocol is not a special case of `GroupBroadcast`.

The major differences are:

- it has exactly one intended recipient
- it is not group-scoped
- it must work before a shared group key exists
- relay mailbox storage can satisfy the sender's durability obligation

It does, however, intentionally reuse the same route-state vocabulary where
possible so later runtime and type work can share machinery.

### 2.3 Replication

Replication owns:

- the logical meaning of the payload
- whether a payload should be sent via this protocol or via `GroupBroadcast`
- invitation acceptance and rejection policy
- accepted-but-not-active group activation and initial snapshot resolution

This protocol owns:

- direct delivery attempts
- relay mailbox storage attempts
- recipient mailbox fetch and ack semantics

## 3. Sender-Side Delivery Model

### 3.1 Work Item Shape

One accepted recipient-addressed envelope becomes one delivery work item.

Each work item contains:

- one immutable envelope
- one recipient route
- zero or more relay mailbox routes
- zero or more concrete delivery attempts across those routes
- enqueue timestamp
- expiry deadline or retention policy
- recipient-ack status

The recipient route and relay routes are logical scheduling lanes. The concrete
send/store operations against them are attempts.

### 3.2 Reused Attempt States

Concrete sender-side attempts reuse the same active-state vocabulary as
[`group_broadcast_queue_model.md`](./group_broadcast_queue_model.md):

- `Queued`
- `AttemptingDirect`
- `AwaitingRelayStore`
- `PendingRoute`

Concrete sender-side attempts also reuse the same terminal outcomes:

- `Delivered`
- `StoredAtRelay`
- `Expired`

Interpretation is specialized as follows:

- a direct recipient attempt reaching `Delivered` means one direct delivery
  attempt was accepted
- a relay mailbox attempt reaching `StoredAtRelay` means one relay durably
  accepted the envelope into the recipient's mailbox

These are attempt outcomes, not overall completion.

### 3.3 Completion Rule

One sender-side work item is complete only when the sender observes a valid
`RecipientAck` signed by the intended recipient for the original `message_id`.

Neither of the following is sufficient by itself:

- one accepted direct delivery attempt
- one stored relay mailbox copy

They are important evidence and may reduce delivery risk, but they do not prove
that the recipient durably accepted the message.

### 3.4 Aggressive Retry Rule

Until `RecipientAck` is observed, the sender should remain aggressive:

- continue direct delivery attempts when the recipient appears reachable
- establish and maintain relay mailbox copies when possible
- retry after ambiguous loss, disconnect, or stale reachability

This reflects the intended use of the protocol:

- these messages are high-value
- delays can make later application more difficult
- sender-side durability is not enough; the goal is recipient acceptance

### 3.5 Durable-Only Semantics

This protocol is always durable. There is no `BestEffort` mode.

The sender should attempt:

- direct recipient delivery when the recipient is reachable
- relay mailbox storage when direct delivery is unavailable or not yet proven

Giving up without either a direct delivery or one stored mailbox copy means the
sender failed even to establish a plausible delivery path. Overall completion
still requires `RecipientAck`.

## 4. Envelope Model

### 4.1 `DirectMessageEnvelope`

Purpose:
Carry one opaque, recipient-addressed payload for either direct delivery or
relay mailbox storage.

Must convey:

- cleartext header:
    - sender identity
    - recipient identity
    - `message_ref`, for example:
        - `MigrationProposal(migration_id)`
        - `GroupInvitation(group_id)`
        - `BootstrapKeyPackage(group_id)`
    - `message_id`
    - sender signature metadata
- encrypted payload bytes

Notes:

- the payload is encrypted/authenticated for the intended recipient
- the same envelope is used for both direct recipient delivery and relay
  mailbox storage
- relays index by cleartext header fields but cannot decrypt the payload
- the sender signs the envelope so the recipient can verify who authored the
  bootstrap material before acting on it

### 4.2 `RecipientAck`

Purpose:
Let the recipient confirm durable local acceptance of one recipient-addressed
message.

Must convey:

- original `message_id`
- recipient identity
- optional `message_ref`
- recipient signature over the ack fields

Notes:

- this is the only completion signal that satisfies the sender-side work item
- it may itself be delivered directly or via relay mailbox using this same
  protocol
- for replication bootstrap payloads, recipient acknowledgement means the
  proposal or invitation was accepted into local activation state; it does not
  necessarily mean the target group is active yet

## 5. Relay Mailbox Contract

This protocol assumes that configured relays can provide recipient-addressed
mailbox storage.

The externally visible relay contract is:

- store opaque envelopes under one recipient identity
- acknowledge successful storage to the sender
- return stored envelopes only to the intended recipient
- retain stored envelopes until recipient ack or retention expiry
- redeliver unacked envelopes on later recipient fetches

This document intentionally does not define how the relay implements that
contract internally.

### 5.1 Relay Store Confirmation

A relay-store confirmation proves only:

- the relay accepted the envelope into the recipient's mailbox

It does not prove:

- the recipient fetched the envelope
- the recipient accepted or applied the payload
- the sender's delivery obligation is complete

### 5.2 Retention and Redelivery

Mailbox delivery is at-least-once:

- a fetched but unacked envelope may be returned again later
- the recipient must deduplicate by `message_id`
- relays may expire old mailbox entries according to policy

## 6. Recipient Mailbox Flow

Recipient mailbox retrieval is driven by the recipient, not by relay push.

### 6.1 `MailboxFetch`

Purpose:
Check in with one relay mailbox and request pending envelopes for one recipient.

Must convey:

- recipient identity
- optional cursor or batch limit
- freshness material
- proof that the requester controls the recipient identity key

This request acts as mailbox check-in and fetch trigger in one step.

### 6.2 `MailboxBatch`

Purpose:
Return one batch of pending mailbox items for the requesting recipient.

Must convey:

- zero or more stored `DirectMessageEnvelope` values
- relay-local ack handles or stable message ids sufficient for later ack
- optional continuation marker when more items remain

An empty batch means the relay currently has no deliverable mailbox items for
that recipient.

### 6.3 `MailboxAck`

Purpose:
Tell the relay that the recipient durably accepted specific mailbox items and
they may be deleted or compacted.

Must convey:

- recipient identity
- acknowledged message ids or relay-issued ack handles
- freshness/authentication material bound to the recipient identity

The recipient should send `MailboxAck` only after the fetched item has been
durably accepted locally.

`MailboxAck` is for relay cleanup only. It does not replace `RecipientAck`.

## 7. Recipient-Side Rules

On mailbox retrieval, the recipient must:

- authenticate the relay response at the transport/session level as usual
- deduplicate envelopes by `message_id`
- ignore envelopes not addressed to its identity
- verify the sender signature on each envelope before acting on the payload
- treat fetched envelopes as opaque input to higher-level bootstrap or
  replication logic
- ack envelopes only after durable local acceptance

After durable local acceptance, the recipient should:

- send `MailboxAck` to the relay for mailbox cleanup
- send `RecipientAck` to the original sender, directly when possible and via
  mailbox delivery when necessary

Direct delivery and mailbox retrieval share the same `message_id` namespace for
deduplication. If the same message arrives directly and via mailbox, processing
it twice must be a no-op at the higher layer.

## 8. Crash Recovery Expectations

Sender-side active work follows the same conservative durable-recovery rule as
`GroupBroadcast`:

- `Queued` rehydrates as `Queued`
- `PendingRoute` rehydrates as `PendingRoute`
- `AttemptingDirect` rehydrates as `PendingRoute`
- `AwaitingRelayStore` rehydrates as `PendingRoute`

Recipient-side mailbox retrieval is also conservative:

- fetched but unacked items may be fetched again after restart
- idempotence is therefore required at the recipient

## 9. Integration Points

### 9.1 Bootstrap and Invitation

This protocol is the default delivery mechanism for recipient-addressed
bootstrap material before a shared group key exists.

Typical examples:

- first invitation into a new replication group
- migration proposal for an existing old-group member
- migration-derived group invitation for a recipient not yet in the new group
- one recipient's encrypted share of new group key material

### 9.2 Transition to Group Protocols

Once the recipient accepts, resolves any required initial snapshot, and
activates the group:

- group-scoped traffic moves to `GroupBroadcast`
- state synchronization uses `Replication`

This protocol remains available for future recipient-addressed bootstrap or
out-of-band control messages, but it is not the normal path for steady-state
group replication.

## 10. Open Questions Deferred to Later Tasks

- how mailbox fetch and ack bind onto concrete transport sessions
- whether relay ack should use stable message ids, relay-issued handles, or both
- whether some envelope/domain types should be shared with `GroupBroadcast`
- how mailbox relay configuration is surfaced in runtime and public APIs
