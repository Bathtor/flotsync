# FlotSync Communication Protocol (High-Level Spec, Draft)

## 1. Scope

This document defines the replication communication model as three distinct sub-protocols:

1. `PeerDiscovery+Tracking`
2. `GroupBroadcast`
3. `Replication`

The goal is to keep reachability tracking, message fan-out/storage, and replication semantics separate.

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

### 5.1 Purpose and Ownership

This sub-protocol only answers:

- which peers are currently reachable
- how they can be reached (transport/address tuple)
- whether reachability is fresh enough to attempt delivery

It also authenticates that a reachable peer is actually the claimed identity.

### 5.2 State Model (per remote peer endpoint)

- `Unknown`: No information yet. (Can be implicit by simply missing from the state).
- `Known`: Identity/Address known. May not be reachable. (Default state for relays, which have fixed addresses).
- `Reachable`: Identity recently confirmed reachable on stored address.
- `Stale`: Identity not reached within TTL on stored address.

### 5.3 Message Classes

#### `Hello`

Purpose:
Announce identity, reachable addresses/transports, and capability hints.

Must convey:

- peer identity
- one or more reachable addresses
- optional capabilities/version hints
- freshness nonce
- signature by peer private key over the signed fields

#### `CheckIn`

Purpose:
Actively test peer reachability.

Must convey:

- attempt id
- sender return address
- freshness nonce
- signature by peer private key over the signed fields

#### `StillHere`

Purpose:
Answer `CheckIn`.

Must convey:

- `CheckIn` attempt id
- echoed challenge material (or bound response nonce)
- signature by peer private key over the signed fields

### 5.4 Identity Verification Rules

- Every identity has a public/private keypair.
- For group-scoped discovery, public keys come from the replication-group creation message.
- `Hello`, `CheckIn`, and `StillHere` signatures must verify against the claimed identity key.
- Signature failure means the peer is treated as unauthenticated (`Unknown` or `Stale`) and not upgraded to `Reachable`.

## 6. Sub-Protocol B: GroupBroadcast

### 6.1 Purpose and Ownership

This sub-protocol fans messages to:

- all currently reachable group members
- configured relays

It also keeps pending delivery work for currently unreachable members.

It does not interpret replication payload semantics.
It does enforce delivery-class behavior (`Durable` vs `BestEffort`).

### 6.2 State Model (per group message, per target)

- `Queued`: accepted for fan-out.
- `AttemptingDirect`: direct delivery in progress.
- `AwaitingRelayStore`: durable message still needs relay persistence confirmation.
- `PendingTarget`: target currently unreachable; retry pending.
- `StoredAtRelay`, `Delivered`, and `Expired` are terminal bookkeeping outcomes, not explicit active states.

### 6.3 Message Classes

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
Signal successful target delivery for queue compaction.

Must convey:

- message id
- target identity

### 6.4 Delivery Class Semantics

- `Durable`:
    - for messages that carry important group state and should remain retrievable
    - broadcaster should attempt direct delivery and relay persistence before giving up
    - relay stores/indexes by cleartext header fields including `message_ref`
- `BestEffort`:
    - for transient request-like messages (for example `NeedRange`)
    - broadcast once, no persistence guarantee required

## 7. Sub-Protocol C: Replication

### 7.1 Purpose and Ownership

This sub-protocol handles:

- producing and applying updates
- VV tracking
- pending updates blocked by causality
- migration initiation/handling

It assumes transport/broadcast delivery is handled by sub-protocol B.

### 7.2 State Model (per local node, per replication group)

- `Active`: normal update production/application.
- `CatchUp`: local node is behind known frontier and is requesting data.
- `CausalityBlocked`: buffered updates exist with unsatisfied `ReadVV`.
- `Migrating`: migration has been proposed/accepted and new-group transition is in progress.
- `Closed`: old group locally closed for writes by policy.

### 7.3 Message Classes

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

#### `GroupClose`

Purpose:
Signal old-group lifecycle policy after migration.

Must convey:

- migration id
- old group id
- close mode/policy marker

## 8. Cross-Protocol Flows

### 8.1 Initial Sync

1. `PeerDiscovery+Tracking`: determine reachable peers/relays.
2. `Replication`: exchange `Summary`; lagging node sends `NeedRange(hasVV, needsVV)`.
3. `GroupBroadcast`: deliver envelopes to peers/relays (`NeedRange` usually `BestEffort`; state-bearing responses typically `Durable`).
4. `Replication`: fulfill with `UpdateBatch` or `Snapshot` + trailing updates.
5. `Replication`: apply under G3, emit `Ack`.

### 8.2 Reconnect/Resume

Same wire behavior as initial sync: VV is the cursor.

### 8.3 Concurrent Bidirectional Sync

1. both nodes emit `Update` for local writes.
2. both nodes may issue `NeedRange`.
3. broadcast handles fan-out/retry independently from replication logic.
4. each node advances local VV as causality permits.

### 8.4 Causality-Blocked Replay

1. receive update with unsatisfied `ReadVV`.
2. enter `CausalityBlocked` for that group.
3. request missing range via `NeedRange`.
4. apply buffered update once dependencies arrive.

### 8.5 Migration

1. `Replication`: emit `MigrationInit`.
2. `GroupBroadcast`: fan-out to reachable members and relays; keep pending for unavailable members.
3. receivers accept or ignore migration (policy/user mediated).
4. accepting receivers join new group and start sync there.
5. optional `GroupClose` updates old-group write/read policy.
6. ignoring migration can intentionally split old/new group activity.

## 9. Error Handling and Compatibility

- Unknown message kinds are ignored.
- No mandatory negative response is required.
- Integrity/authentication failures cause drop.
- Temporary feature mismatch can be tolerated by ignoring unsupported messages.

## 10. Security / Trust Assumptions

- Group payloads are end-to-end encrypted/authenticated.
- Relays are untrusted for plaintext.
- Migration key rollout must exclude removed members.
- Migration acceptance is a user/policy safety boundary.

## 11. Wire-Format Notes (Deferred)

- Keep a common outer envelope for all sub-protocol messages.
- Include message id, sender id, group id (if scoped), payload kind, and correlation id where relevant.
- Reuse existing protobuf datamodel payload formats for update/snapshot bodies where possible.
