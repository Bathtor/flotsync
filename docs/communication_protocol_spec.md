---
type: Protocol
title: FlotSync Communication Protocol
description: Defines the high-level Flotsync replication communication model and its sub-protocol boundaries.
status: draft
---

# FlotSync Communication Protocol (High-Level Spec, Draft)

## 1. Scope

This document defines the replication communication model as distinct sub-protocols:

1. `PeerAnnouncement`
2. `RouteEstablishment`
3. `SingleRecipientDurableDelivery`
4. `GroupBroadcast`
5. `Replication`

The goal is to keep reachability tracking, recipient-addressed durable delivery,
group fan-out/storage, and replication semantics separate.

## 2. Terminology

- **Replication Group**: fixed membership set of devices sharing one replication key and schema epoch.
- **Member**: one device identity within a replication group.
- **Relay**: store-and-forward peer for encrypted messages.
- **UpdateId**: `(version, producer_index)`. `UpdateId(0, 0)` is reserved as
  the synthetic initial-state origin and is not a real replicated update.
- **ReadVV**: VV captured by an update producer at creation time.
- **VV**: one version per group member.
- **Snapshot**: full materialized state at a group/version reference.
- **SnapshotRef**: `(group_id, VV)` reference to one group state. Multiple
  refs may describe the same logical snapshot, for example an old group at a
  migration cut and a new group at its zero vector.
- **InitialSnapshot**: initial dataset state required before activating a
  joined or migrated group. It may be empty, inline, or described by metadata
  for later retrieval.
- **Migration**: creation of a new replication group from a state cut of an old group.
- **MigrationId**: `(old_group_id, new_group_id)`.

## 3. System Model and Assumptions

- Network is asynchronous (drop/delay/reorder/duplicate).
- Nodes can be offline for long periods.
- Active groups retain full update history on members and relays.
- Relays cannot decode updates or synthesize snapshots from updates.
- Membership/schema changes only happen via migration.
- Normal sync is fully automatic and deterministic.

## 4. Global Invariants

- **G1 Producer monotonicity**: Each producer's updates are strictly increasing in version.
- **G2 Idempotence**: Replay of an already-applied update is a no-op.
- **G3 Global causality gate**: Every update carries `ReadVV`. A receiver may apply the update only when `LocalVV >= ReadVV`.
- **G4 Legal order**: A legal order is any order that never violates I3 and applies each update at most once.
- **G5 New-group creator index**: The creator/proposer of a newly created group
  instance must occupy member index 0 in that group's canonical member order.
  Bootstrap or migration material whose authenticated sender/proposer is not
  member index 0 is invalid for that new group instance.
- **G6 Convergence**: If two members apply the same update set in any legal order, they converge.
- **G7 Tombstone safety**: Deletes use tombstones so replay/reconstruction remain correct.
- **G8 Ack promise**: When node `n` sends `Ack(A)`, it promises future updates produced by `n` will not produce versions below `max(A)`, that is an `Ack(A)` is a no-op update for all versions between `A[n]` and `max(A)`.

## 5. Sub-Protocol A: PeerAnnouncement

Implementation detail:
See [`route_establishment.md`](./route_establishment.md)
for how peer-announcement observations feed the concrete route establishment
protocol currently targeted by the replicated-checklist first-release slice.

### 5.1 Purpose and Ownership

This sub-protocol only answers:

- which Flotsync-speaking runtime instances are advertising candidate endpoints
- which endpoint routes those instances claim are worth probing

It does not authenticate member identity, prove reachability, or publish
replication routes.

### 5.2 Current Message Class

#### `Peer`

Purpose:
Plaintext peer announcement for one Flotsync-speaking instance and its advertised
listening endpoints.

It must not carry member ids or group ids.

## 6. Sub-Protocol B: RouteEstablishment

Implementation detail:
See [`route_establishment.md`](./route_establishment.md)
for the concrete route establishment protocol currently targeted by the
replicated-checklist first-release slice.

### 6.1 Purpose and Ownership

This sub-protocol only answers:

- which peers are currently reachable
- how they can be reached (transport/address tuple)
- whether reachability is fresh enough to attempt delivery

It also authenticates that a reachable route belongs to the claimed member
identity before exposing the route to replication.

Route establishment consumes decoded peer-announcement observations, but it is
not the peer-announcement protocol and does not own the peer-announcement UDP
port.

Route establishment uses the runtime endpoint port, the same endpoint later used
by replication transport. The setup layer that initiated the endpoint bind
reports each new endpoint binding to route establishment. Route establishment
observes closure itself from the shared UDP indication stream.

There is no central endpoint demultiplexer. Components classify their own
messages from the shared UDP indications and ignore well-formed endpoint traffic
for other sub-protocols.

### 6.2 State Model (per remote peer endpoint)

- `Unknown`: No active record; normally represented by absence from tracker state.
- `Known`: A plaintext peer announcement was observed, but the endpoint has not been verified.
- `Reachable`: A receiver-driven introduction probe verified at least one signed introduction claim.
- `Stale`: A previously reachable endpoint expired, timed out, or failed refresh.

### 6.3 Current Route Establishment Message Classes

#### `IntroductionRequest`

Purpose:
Receiver-driven route probe with a fresh nonce.

The request is plaintext. The nonce is signed by the responder in the
`Introduction` claims.

#### `Introduction`

Purpose:
Return signed member/group claims for the probed route.

Each introduction claim is signed by the claimed member identity and may list
several group ids for that member. Route establishment checks verified claims
against the shared group membership snapshot and publishes only claims that
intersect local group membership.

### 6.4 Identity Verification Rules

- Every member identity has a public/private signing keypair.
- For group-scoped discovery, trusted public keys come from local provisioning
  or stored replication group security material.
- `Peer` and `IntroductionRequest` are plaintext hints/probes.
- Each `Introduction` claim signature must verify against the claimed member
  identity key over the encoded claim bytes exactly as received.
- Signature failure means the route is not published to replication and any
  previously reachable route for the affected member may become `Stale`.

## 7. Sub-Protocol C: SingleRecipientDurableDelivery

Implementation detail:
See [`single_recipient_durable_delivery.md`](./single_recipient_durable_delivery.md)
for the concrete sender-side route model and relay mailbox flow that refine this
section.

### 7.1 Purpose and Ownership

This sub-protocol delivers one opaque message durably to one specific
recipient.

It is used when:

- no shared group key exists yet
- the payload is recipient-addressed rather than group-scoped
- relays may need to hold mailbox copies until the recipient checks in

It does not interpret payload semantics.
It does enforce durable-delivery behavior.

### 7.2 Sender-Side Route Model

- one recipient route for the intended recipient
- zero or more relay mailbox routes
- zero or more concrete delivery attempts across those routes
- concrete attempts reuse `Queued`, `AttemptingDirect`,
  `AwaitingRelayStore`, `PendingRoute`, `Delivered`, `StoredAtRelay`, and
  `Expired`

Unlike `GroupBroadcast`, accepted direct delivery or relay mailbox storage does
not complete the work item. The sender-side work item completes only when a
valid `RecipientAck` from the intended recipient is observed.

### 7.3 Message Classes

#### `DirectMessageEnvelope`

Purpose:
Carry one recipient-addressed payload for either direct delivery or relay
mailbox storage.

Must convey:

- cleartext header:
    - sender identity
    - recipient identity
    - `message_ref`
    - `message_id`
    - sender signature metadata
- encrypted payload bytes

Notes:

- the same envelope is used for both direct delivery and relay mailbox storage
- the payload is encrypted/authenticated for the recipient
- relays index by cleartext header fields but cannot decrypt the payload
- the sender signs the envelope so the recipient can verify bootstrap trust

#### `RecipientAck`

Purpose:
Tell the original sender that one recipient-addressed message was processed and
does not require retry.

Must convey:

- original `message_id`
- recipient identity
- optional `message_ref`
- recipient signature over the ack fields

Notes:

- invitation and proposal payloads do not require retry after terminal
  rejection, stored listener mediation, or committed activation work
- it does not imply that a listener accepted the work or that the target group
  is already active
- failures before processing reaches one of those outcomes intentionally
  withhold the acknowledgement so reliable delivery may retry

- this is the completion signal for sender-side delivery
- it may itself be delivered directly or through relay mailboxes

#### `MailboxFetch`

Purpose:
Let the recipient check in with one relay and request pending mailbox items.

Must convey:

- recipient identity
- optional cursor or batch limit
- freshness material
- proof that the requester controls the recipient identity key

#### `MailboxBatch`

Purpose:
Return stored mailbox items for one recipient.

Must convey:

- zero or more stored `DirectMessageEnvelope` values
- message ids or relay-issued ack handles
- optional continuation marker when more items remain

#### `MailboxAck`

Purpose:
Tell the relay that specific mailbox items were durably accepted by the
recipient and may be deleted or compacted.

Must convey:

- recipient identity
- acknowledged message ids or ack handles
- freshness/authentication material

Notes:

- `MailboxAck` is for relay cleanup only
- it does not replace `RecipientAck`

### 7.4 Delivery Semantics

- durable only; there is no `BestEffort` mode
- sender should attempt direct delivery and relay mailbox storage before giving
  up
- relay-store confirmation proves mailbox acceptance, not recipient processing
- direct-delivery acceptance proves only that one direct attempt was accepted
- sender remains aggressive until `RecipientAck` is observed
- recipient mailbox retrieval is at-least-once until `MailboxAck`

## 8. Sub-Protocol D: GroupBroadcast

Implementation detail:
See [`group_broadcast_queue_model.md`](./group_broadcast_queue_model.md) for the
concrete queue/state-machine and ownership model that refines this section.

### 8.1 Purpose and Ownership

This sub-protocol fans messages to:

- all currently reachable group members
- configured relays

For delivery classes that allow retry, it also keeps pending delivery work for
currently unreachable routes.

It does not interpret replication payload semantics.
It does enforce delivery-class behavior (`Durable` vs `BestEffort`).

### 8.2 State Model (per group message, per route)

- `Queued`: accepted for fan-out.
- `AttemptingDirect`: direct delivery in progress.
- `AwaitingRelayStore`: durable message still needs relay persistence confirmation.
- `PendingRoute`: route currently unavailable; retry pending.
- `StoredAtRelay`, `Delivered`, and `Expired` are terminal bookkeeping outcomes,
  not explicit active states; reaching one cleans up the active local route
  state.

### 8.3 Message Classes

#### `GroupMessageEnvelope`

Purpose:
Carry a group-scoped message payload for fan-out and optional relay persistence.

Must convey:

- cleartext header:
    - group id
    - sender identity
    - delivery class: `Durable` or `BestEffort`
    - `message_ref` (for relay indexing/retrieval), e.g.:
        - `GroupInit(migration_or_group_id)`
        - `Update(version, producer_index)`
        - `GroupClose(group_id)`
    - envelope `message_id` (dedupe/retry bookkeeping)
- encrypted payload bytes

Notes:

- `GroupMessageEnvelope` is used for both peer fan-out and relay storage.
- There is no separate `RelayStore` message type.

#### `DeliveryReceipt` (optional)

Purpose:
Signal successful route delivery for queue compaction.

Must convey:

- message id
- route identity

### 8.4 Delivery Class Semantics

- `Durable`:
    - for messages that carry important group state and should remain retrievable
    - broadcaster should attempt direct delivery and relay persistence before giving up
    - relay stores/indexes by cleartext header fields including `message_ref`
    - unreachable routes may remain pending for later retry
- `BestEffort`:
    - for transient request-like messages (for example `NeedRange`)
    - broadcast once, no persistence guarantee required
    - unreachable or failed routes expire instead of remaining pending

## 9. Sub-Protocol E: Replication

### 9.1 Purpose and Ownership

This sub-protocol handles:

- producing and applying updates
- VV tracking
- pending updates blocked by causality
- migration initiation/handling

It assumes recipient-addressed bootstrap delivery is handled by sub-protocol C
and group-scoped delivery is handled by sub-protocol D.

### 9.2 State Model (per local node, per replication group)

Application access follows a persisted lifecycle:

- `Open`: application reads, writes, summaries, membership changes, and
  listener row events are enabled.
- `ReadOnly`: an accepted migration target and immutable `final_versions` cut
  are recorded while target activation completes. Application reads,
  summaries, and listener row events remain enabled; writes and further
  membership changes are rejected.
- `Closed`: application access and listener row events are disabled. The group
  still applies and serves replication updates needed to reproduce the accepted
  cut.

Catch-up and causality-blocked work are operational states within this
lifecycle. For `ReadOnly` and `Closed` groups, summaries, range requests, and
accepted updates are bounded by `final_versions`. Read tokens omit both states
because neither can produce further application writes.

### 9.3 Message Classes

#### `Summary`

Purpose:
Advertise current group progress frontier.

Must convey:

- group id
- sender `hasVV`

#### `NeedRange`

Purpose:
Request catch-up from current state to desired state.

Must convey:

- group id
- requester `hasVV`
- requester `needsVV`
- correlation id

Notes:

- responder chooses efficient fulfillment (`UpdateBatch` or `Snapshot` + trailing `UpdateBatch`)
- `NeedRange` is typically sent as a `BestEffort` `GroupMessageEnvelope`
- `NeedRange` can be fulfilled by any node or relay that can provide the requested data

#### `Update`

Purpose:
Broadcast one new locally produced update.

Must convey:

- group id
- `UpdateId(version, producer_index)`
- `ReadVV`
- update payload

Receiver must:

- apply only when `LocalVV >= ReadVV`
- otherwise buffer and continue catch-up

#### `UpdateBatch`

Purpose:
Send historical updates (usually as `NeedRange` response).

Must convey:

- group id
- correlation id
- one or more updates

#### `Snapshot`

Purpose:
Send full state at a group/version reference as a catch-up optimization or
activation input.

Must convey:

- snapshot ref:
    - group id
    - VV
- snapshot payload

Notes:

- relays may forward stored snapshots
- relays cannot synthesize snapshots from update history
- far-behind members may fetch a snapshot instead of replaying many deltas
- large snapshot transfer must be chunked by the replication layer so transport
  reassembly does not need to hold a complete snapshot in memory

#### `InitialSnapshot`

Purpose:
Describe the initial state required before a group invitation or migration can
become active locally.

Must convey one of:

- `Empty`: the initial dataset state is empty
- `Inline`: the initial snapshot payload is carried directly
- `Metadata`: the initial snapshot exists elsewhere and must be resolved before
  activation

`Metadata` must convey:

- primary snapshot ref
- zero or more equivalent snapshot refs
- optional record count

Notes:

- `Empty` does not imply that the group is newly created
- `Inline` carries projected row values, not CRDT state. Recipients embed
  deterministic initial CRDT state from those values using
  `UpdateId(0, 0)` as the synthetic origin. `UpdateId(0, 0)` must not appear in
  ordinary update logs.
- in a migration, existing old-group members may receive an old-group/cut ref
  while newly added members receive a new-group/zero-vector ref
- equivalent refs let runtimes deduplicate local snapshot state even when
  transfer encryption or recipient perspective differs
- digest, encoded size, and table count are intentionally deferred until their
  canonical semantics are designed

#### `Ack`

Purpose:
Advertise applied progress and commit non-regression promise.

Must convey:

- group id
- `ackVV`

Notes:

- steady state: usually ack each `Update`
- catch-up: coarse ack (for example once synced) is acceptable

#### `GroupInvitation`

Purpose:
Invite a recipient to activate a new group when the recipient cannot rely on an
old-group authority context.

Must convey:

- target group id
- creation or migration source context
- proposed canonical membership
- group schema
- empty, inline, or metadata initial snapshot
- recipient-protected target-group member keys, cipher suite, and group key
- optional application-facing group name and message

Notes:

- the reliable invitation payload carries its matching private group setup
- migration-derived invitations are scoped to the target group and are sent to
  newly added members
- private setup remains outside listener-visible invitation values

#### `MigrationProposal`

Purpose:
Propose/initiate migration to new group.

Must convey:

- migration id `(old_group_id, new_group_id)`
- old group id
- new group id
- migration cut VV in old group
- new group membership
- new group schema
- recipient-protected target-group member keys, cipher suite, and group key
- initial snapshot

Notes:

- typically carried via `SingleRecipientDurableDelivery` when the recipients do
  not yet share the new group key
- recipients should verify sender signatures before accepting the bootstrap
  material
- the proposer of the new group instance must be member index 0 in the proposed
  new-group member order
- `MigrationProposal` is signed/scoped to the old group because the old group is
  the authority context for superseding its state
- newly added members that are not in the old group receive a `GroupInvitation`
  with migration source context rather than a listener-visible
  `MigrationProposal`
- the proposal and its private group setup are one self-contained reliable
  payload; private setup remains outside listener-visible proposal values
- the recipient-addressed envelope protects both inline snapshot values and
  target-group setup while they are in transit

#### `GroupClose`

Purpose:
Request closure of an existing group without creating a replacement group.

Must convey:

- group id
- close mode/policy marker

Notes:

- this standalone group-close message is specified for a later runtime slice
- `GroupClose` is signed/scoped to the group being closed
- it is a lifecycle signal, not remote command authority
- local policy decides whether to close writes, keep read-only access, or only
  observe the signal
- unauthorised or unsupported close signals are ignored

## 10. Cross-Protocol Flows

### 10.1 Bootstrap / Invitation

1. higher-layer logic prepares a self-contained recipient-addressed migration
   proposal or group invitation with matching private target-group setup.
2. `SingleRecipientDurableDelivery`: attempt direct delivery and relay mailbox
   storage.
3. recipient checks in with configured relay mailboxes using `MailboxFetch`.
4. relay returns pending envelopes in `MailboxBatch`.
5. recipient verifies the sender signature and payload, processes the message
   to an outcome that does not require retry, and sends `MailboxAck` for relay
   cleanup.
6. recipient sends `RecipientAck` back to the original sender, directly when
   possible and through relay mailboxes when necessary.
7. automatic or listener acceptance activates Empty and Inline snapshots;
   Metadata snapshots remain unsupported by the current runtime.

### 10.2 Initial Sync

1. `PeerAnnouncement` observes candidate endpoints.
2. `RouteEstablishment` verifies reachable peer routes.
3. `Replication`: exchange `Summary`; lagging node sends `NeedRange(hasVV, needsVV)`.
4. `GroupBroadcast`: deliver envelopes to peers/relays (`NeedRange` usually `BestEffort`; state-bearing responses typically `Durable`).
5. `Replication`: fulfil with `UpdateBatch` or `Snapshot` + trailing updates.
6. `Replication`: apply under G3, emit `Ack`.

### 10.3 Reconnect/Resume

Same wire behavior as initial sync: VV is the cursor.

### 10.4 Concurrent Bidirectional Sync

1. both nodes emit `Update` for local writes.
2. both nodes may issue `NeedRange`.
3. broadcast handles fan-out/retry independently from replication logic.
4. each node advances local VV as causality permits.

### 10.5 Causality-Blocked Replay

1. receive update with unsatisfied `ReadVV`.
2. enter `CausalityBlocked` for that group.
3. request missing range via `NeedRange`.
4. apply buffered update once dependencies arrive.

### 10.6 Migration

1. `Replication`: emit old-group-scoped `MigrationProposal` messages for
   existing members and migration-sourced `GroupInvitation` messages for newly
   added members.
2. `SingleRecipientDurableDelivery`: deliver per-recipient invitation packages
   directly or through relay mailboxes until `RecipientAck`.
3. receivers validate the self-contained setup and classify the work according
   to local policy. Concurrent undecided proposals for the same old group are
   exposed to the listener together so one candidate can be selected. Metadata
   snapshots are automatically rejected in the current implementation because
   snapshot fetching is unavailable.
4. automatic rejection removes inactive material and pending work. Listener
   mediation atomically stores inactive material plus pending decision state.
   Automatic acceptance atomically stores inactive material plus accepted
   activation state.
5. once the message has been processed and does not require retry, the receiver
   completes the reliable-delivery acknowledgement. Listener mediation does not
   wait for the eventual listener response before acknowledging delivery.
6. accepting one proposal atomically records the selected target and cut, moves
   the old group to `ReadOnly`, removes competing proposals for that old group,
   and stores accepted activation work. Exact replays of the selected proposal
   are idempotent; proposals for a different target or cut are terminally
   rejected and acknowledged without setup validation or listener notification.
7. pending listener decisions are re-surfaced after restart. Accepted activation
   work resumes after restart without asking the listener again.
8. Empty snapshots activate without rows. Inline projected rows are embedded as
   deterministic initial state, committed with target-group activation, and
   emitted to the listener after commit.
9. target activation and the old group's transition from `ReadOnly` to `Closed`
   commit together. A closed old group continues bounded catch-up without
   exposing further rows to the application.
10. `GroupBroadcast`: once the new group exists, fan-out group-scoped traffic
   within that group as normal.
11. rejecting or ignoring migration can intentionally split old/new group
   activity.

## 11. Error Handling and Compatibility

- Unknown message kinds are ignored.
- No mandatory negative response is required.
- Integrity/authentication failures cause drop.
- Temporary feature mismatch can be tolerated by ignoring unsupported messages.

## 12. Security / Trust Assumptions

- Group payloads are end-to-end encrypted/authenticated.
- Recipient-addressed bootstrap payloads are encrypted/authenticated for the
  intended recipient.
- Relays are untrusted for plaintext.
- Migration key rollout must exclude removed members.
- Migration acceptance is a user/policy safety boundary.

## 13. Wire-Format Notes (Deferred)

- Keep a common outer envelope for all sub-protocol messages.
- Include message id, sender id, group id or recipient id (as applicable),
  payload kind, and correlation id where relevant.
- Reuse existing protobuf datamodel payload formats for update/snapshot bodies where possible.
