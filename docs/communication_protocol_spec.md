# FlotSync Communication Protocol (High-Level Spec, Draft)

## 1. Scope

This document defines the replication communication model as four distinct sub-protocols:

1. `PeerDiscovery+Tracking`
2. `SingleRecipientDurableDelivery`
3. `GroupBroadcast`
4. `Replication`

The goal is to keep reachability tracking, recipient-addressed durable delivery,
group fan-out/storage, and replication semantics separate.

## 2. Terminology

- **Replication Group**: fixed membership set of devices sharing one replication key and schema epoch.
- **Member**: one device identity within a replication group.
- **Relay**: store-and-forward peer for encrypted messages.
- **UpdateId**: `(version, producer_index)`.
- **ReadVV**: VV captured by an update producer at creation time.
- **VV**: one version per group member.
- **Snapshot**: full materialized state at a VV cut.
- **Migration**: creation of a new replication group from a state cut of an old group.

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
- **G5 Convergence**: If two members apply the same update set in any legal order, they converge.
- **G6 Tombstone safety**: Deletes use tombstones so replay/reconstruction remain correct.
- **G7 Ack promise**: When node `n` sends `Ack(A)`, it promises future updates produced by `n` will not produce versions below `max(A)`, that is an `Ack(A)` is a no-op update for all versions between `A[n]` and `max(A)`.

## 5. Sub-Protocol A: PeerDiscovery+Tracking

Implementation detail:
See [`custom_udp_peer_discovery.md`](./custom_udp_peer_discovery.md)
for the concrete custom UDP discovery protocol currently targeted by the
replicated-checklist first-release slice.

### 5.1 Purpose and Ownership

This sub-protocol only answers:

- which peers are currently reachable
- how they can be reached (transport/address tuple)
- whether reachability is fresh enough to attempt delivery

It also authenticates that a reachable route belongs to the claimed member
identity before exposing the route to replication.

### 5.2 State Model (per remote peer endpoint)

- `Unknown`: No active record; normally represented by absence from tracker state.
- `Known`: A plaintext beacon was observed, but the endpoint has not been verified.
- `Reachable`: A receiver-driven introduction probe verified at least one signed introduction claim.
- `Stale`: A previously reachable endpoint expired, timed out, or failed refresh.

### 5.3 Current Custom UDP Message Classes

#### `Peer`

Purpose:
Plaintext beacon for one Flotsync-speaking instance and its advertised
listening endpoints.

It must not carry member ids or group ids.

#### `IntroductionRequest`

Purpose:
Receiver-driven route probe with a fresh nonce.

The request is plaintext. The nonce is signed by the responder in the
`Introduction` claims.

#### `Introduction`

Purpose:
Return signed member/group claims for the probed route.

Each introduction claim is signed by the claimed member identity and may list
several group ids for that member. Only claims that intersect local group
membership are published as replication routes.

### 5.4 Identity Verification Rules

- Every member identity has a public/private signing keypair.
- For group-scoped discovery, trusted public keys come from local provisioning
  or stored replication group security material.
- `Peer` and `IntroductionRequest` are plaintext hints/probes.
- Each `Introduction` claim signature must verify against the claimed member
  identity key over the encoded claim bytes exactly as received.
- Signature failure means the route is not published to replication and any
  previously reachable route for the affected member may become `Stale`.

## 6. Sub-Protocol B: SingleRecipientDurableDelivery

Implementation detail:
See [`single_recipient_durable_delivery.md`](./single_recipient_durable_delivery.md)
for the concrete sender-side route model and relay mailbox flow that refine this
section.

### 6.1 Purpose and Ownership

This sub-protocol delivers one opaque message durably to one specific
recipient.

It is used when:

- no shared group key exists yet
- the payload is recipient-addressed rather than group-scoped
- relays may need to hold mailbox copies until the recipient checks in

It does not interpret payload semantics.
It does enforce durable-delivery behavior.

### 6.2 Sender-Side Route Model

- one recipient route for the intended recipient
- zero or more relay mailbox routes
- zero or more concrete delivery attempts across those routes
- concrete attempts reuse `Queued`, `AttemptingDirect`,
  `AwaitingRelayStore`, `PendingRoute`, `Delivered`, `StoredAtRelay`, and
  `Expired`

Unlike `GroupBroadcast`, accepted direct delivery or relay mailbox storage does
not complete the work item. The sender-side work item completes only when a
valid `RecipientAck` from the intended recipient is observed.

### 6.3 Message Classes

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
Tell the original sender that the recipient durably accepted one
recipient-addressed message.

Must convey:

- original `message_id`
- recipient identity
- optional `message_ref`
- recipient signature over the ack fields

Notes:

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

### 6.4 Delivery Semantics

- durable only; there is no `BestEffort` mode
- sender should attempt direct delivery and relay mailbox storage before giving
  up
- relay-store confirmation proves mailbox acceptance, not recipient processing
- direct-delivery acceptance proves only that one direct attempt was accepted
- sender remains aggressive until `RecipientAck` is observed
- recipient mailbox retrieval is at-least-once until `MailboxAck`

## 7. Sub-Protocol C: GroupBroadcast

Implementation detail:
See [`group_broadcast_queue_model.md`](./group_broadcast_queue_model.md) for the
concrete queue/state-machine and ownership model that refines this section.

### 7.1 Purpose and Ownership

This sub-protocol fans messages to:

- all currently reachable group members
- configured relays

For delivery classes that allow retry, it also keeps pending delivery work for
currently unreachable routes.

It does not interpret replication payload semantics.
It does enforce delivery-class behavior (`Durable` vs `BestEffort`).

### 7.2 State Model (per group message, per route)

- `Queued`: accepted for fan-out.
- `AttemptingDirect`: direct delivery in progress.
- `AwaitingRelayStore`: durable message still needs relay persistence confirmation.
- `PendingRoute`: route currently unavailable; retry pending.
- `StoredAtRelay`, `Delivered`, and `Expired` are terminal bookkeeping outcomes,
  not explicit active states; reaching one cleans up the active local route
  state.

### 7.3 Message Classes

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
        - `GroupClose(migration_id)`
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

### 7.4 Delivery Class Semantics

- `Durable`:
    - for messages that carry important group state and should remain retrievable
    - broadcaster should attempt direct delivery and relay persistence before giving up
    - relay stores/indexes by cleartext header fields including `message_ref`
    - unreachable routes may remain pending for later retry
- `BestEffort`:
    - for transient request-like messages (for example `NeedRange`)
    - broadcast once, no persistence guarantee required
    - unreachable or failed routes expire instead of remaining pending

## 8. Sub-Protocol D: Replication

### 8.1 Purpose and Ownership

This sub-protocol handles:

- producing and applying updates
- VV tracking
- pending updates blocked by causality
- migration initiation/handling

It assumes recipient-addressed bootstrap delivery is handled by sub-protocol B
and group-scoped delivery is handled by sub-protocol C.

### 8.2 State Model (per local node, per replication group)

- `Active`: normal update production/application.
- `CatchUp`: local node is behind known frontier and is requesting data.
- `CausalityBlocked`: buffered updates exist with unsatisfied `ReadVV`.
- `Migrating`: migration has been proposed/accepted and new-group transition is in progress.
- `Closed`: old group locally closed for writes by policy.

### 8.3 Message Classes

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
Send full state at a VV cut as catch-up optimization or migration seed.

Must convey:

- group id (or new group id in migration context)
- `snapshotVV`
- snapshot payload

Notes:

- relays may forward stored snapshots
- relays cannot synthesize snapshots from update history

#### `Ack`

Purpose:
Advertise applied progress and commit non-regression promise.

Must convey:

- group id
- `ackVV`

Notes:

- steady state: usually ack each `Update`
- catch-up: coarse ack (for example once synced) is acceptable

#### `MigrationInit`

Purpose:
Propose/initiate migration to new group.

Must convey:

- migration id
- old group id
- new group id
- migration cut VV in old group
- new group membership
- new shared key material (encrypted per recipient)
- initial snapshot for new group

Notes:

- typically carried via `SingleRecipientDurableDelivery` when the recipients do
  not yet share the new group key
- recipients should verify sender signatures before accepting the bootstrap
  material

#### `GroupClose`

Purpose:
Signal old-group lifecycle policy after migration.

Must convey:

- migration id
- old group id
- close mode/policy marker

## 9. Cross-Protocol Flows

### 9.1 Bootstrap / Invitation

1. higher-layer logic prepares recipient-addressed bootstrap material (for
   example a migration invitation or key package).
2. `SingleRecipientDurableDelivery`: attempt direct delivery and relay mailbox
   storage.
3. recipient checks in with configured relay mailboxes using `MailboxFetch`.
4. relay returns pending envelopes in `MailboxBatch`.
5. recipient verifies the sender signature, durably accepts relevant messages,
   and sends `MailboxAck` for relay cleanup.
6. recipient sends `RecipientAck` back to the original sender, directly when
   possible and through relay mailboxes when necessary.
7. once the invitation or bootstrap material is accepted, the recipient can
   join the new group.

### 9.2 Initial Sync

1. `PeerDiscovery+Tracking`: determine reachable peers/relays.
2. `Replication`: exchange `Summary`; lagging node sends `NeedRange(hasVV, needsVV)`.
3. `GroupBroadcast`: deliver envelopes to peers/relays (`NeedRange` usually `BestEffort`; state-bearing responses typically `Durable`).
4. `Replication`: fulfill with `UpdateBatch` or `Snapshot` + trailing updates.
5. `Replication`: apply under G3, emit `Ack`.

### 9.3 Reconnect/Resume

Same wire behavior as initial sync: VV is the cursor.

### 9.4 Concurrent Bidirectional Sync

1. both nodes emit `Update` for local writes.
2. both nodes may issue `NeedRange`.
3. broadcast handles fan-out/retry independently from replication logic.
4. each node advances local VV as causality permits.

### 9.5 Causality-Blocked Replay

1. receive update with unsatisfied `ReadVV`.
2. enter `CausalityBlocked` for that group.
3. request missing range via `NeedRange`.
4. apply buffered update once dependencies arrive.

### 9.6 Migration

1. `Replication`: emit `MigrationInit` or equivalent recipient-addressed
   invitation material.
2. `SingleRecipientDurableDelivery`: deliver per-recipient invitation packages
   directly or through relay mailboxes until `RecipientAck`.
3. receivers accept or ignore migration (policy/user mediated).
4. accepting receivers join new group and start sync there.
5. `GroupBroadcast`: once the new group exists, fan-out group-scoped traffic
   within that group as normal.
6. optional `GroupClose` updates old-group write/read policy.
7. ignoring migration can intentionally split old/new group activity.

## 10. Error Handling and Compatibility

- Unknown message kinds are ignored.
- No mandatory negative response is required.
- Integrity/authentication failures cause drop.
- Temporary feature mismatch can be tolerated by ignoring unsupported messages.

## 11. Security / Trust Assumptions

- Group payloads are end-to-end encrypted/authenticated.
- Recipient-addressed bootstrap payloads are encrypted/authenticated for the
  intended recipient.
- Relays are untrusted for plaintext.
- Migration key rollout must exclude removed members.
- Migration acceptance is a user/policy safety boundary.

## 12. Wire-Format Notes (Deferred)

- Keep a common outer envelope for all sub-protocol messages.
- Include message id, sender id, group id or recipient id (as applicable),
  payload kind, and correlation id where relevant.
- Reuse existing protobuf datamodel payload formats for update/snapshot bodies where possible.
