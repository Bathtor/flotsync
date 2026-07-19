---
type: State Machine
title: Inline Migration Policy Flow
description: Defines validation, policy, persistence, activation, and restart behaviour for inline membership migration work.
status: implemented
applies_to:
  - flotsync_replication
source_of_truth:
  - ../flotsync_replication/src/api/mod.rs
  - ../flotsync_replication/src/runtime/component.rs
  - ../flotsync_replication/src/runtime/pending_group.rs
  - ../flotsync_replication/src/store/sqlite.rs
depends_on:
  - communication_protocol_spec.md
  - single_recipient_durable_delivery.md
tracked_by:
  - flotsync-git-pow.5
  - flotsync-git-03f
  - flotsync-git-lg9
---

# Inline Migration Policy Flow

## Scope

This document describes the implemented Empty/Inline membership-migration
slice. It covers both listener-visible work kinds:

- `MigrationProposal` for a recipient that belongs to the old and proposed groups
- migration-sourced `GroupInvitation` for a recipient newly added to the group

Creation invitations use the same pending-decision and activation machinery.
The [communication protocol](communication_protocol_spec.md) owns message
semantics; this document records the local runtime state machine.

## Non-Goals

The current flow does not:

- fetch or reconstruct Metadata-only initial snapshots
- classify member devices below top-level `MemberIdentity` values
- notify a member that the proposed group removed its local identity
- emit or apply the standalone `GroupClose` operation

Standalone closure without a replacement group is tracked by
`flotsync-git-lg9`. Migration requires the old group to become read-only and
then inactive as the target group activates. The current implementation does
not yet perform that transition; `flotsync-git-03f` tracks the gap.
Application-facing deactivation exposure remains with `flotsync-git-pvp` and
`flotsync-git-os1`.

## Inputs and Ownership

One recipient-protected reliable payload contains:

- listener-safe invitation or proposal fields
- proposed canonical membership and group schema
- Empty, Inline, or Metadata initial snapshot state
- private target-group member keys, cipher suite, and group key

Private setup is validated and stored by the runtime but removed before work is
surfaced to the application listener. The SQLite store owns inactive material,
pending work, active markers, and crash recovery. Only an active marker makes a
group externally visible.

## Validation

Inbound handling validates these conditions before policy selection:

1. the reliable envelope authority group matches the message
2. the authenticated sender is proposed member index zero
3. proposed membership is valid and includes the local member
4. setup membership and listener-safe proposed membership match
5. member keys, local index, and schema match any stored target-group material
6. replayed setup decrypts to the same target-group key as stored material

Cheap stored-definition comparisons run before setup decryption. A matching
already-active target group is an idempotent replay and needs no pending work.

## Policy Classification

Invitation policy selects one configured value:

| Invitation source | Policy field |
| --- | --- |
| Creation | `GroupInvitationPolicy::creation` |
| Migration | `GroupInvitationPolicy::migration_added_member` |

Migration-proposal policy starts with `GroupMigrationPolicy::epoch_change`,
then combines applicable top-level member changes:

| Membership difference | Additional policy field |
| --- | --- |
| At least one proposed member is absent from the old group | `member_added` |
| At least one old member is absent from the proposed group | `member_removed` |

`PolicyDecision` order is `AutoAccept < AskListener < AutoReject`; combining
decisions therefore selects the most restrictive result. Device-level fields
and `local_member_removed` are reserved for later classification flows.

Metadata snapshots override invitation or proposal policy with automatic
rejection because this runtime cannot fetch their rows.

## States and Transitions

The store has one target-group-keyed pending-work row. Its lifecycle state is
either `AwaitingDecision` or `AcceptedActivation`.

| Current state | Trigger | Atomic store result | Next action |
| --- | --- | --- | --- |
| Validated inbound work | `AutoReject` | Remove pending work and inactive material | Complete processing as rejected |
| Validated inbound work | `AskListener` | Store exact inactive material and `AwaitingDecision` | Emit one listener event |
| Validated inbound work | `AutoAccept` | Store exact inactive material and `AcceptedActivation` | Activate without listener mediation |
| `AwaitingDecision` | Listener rejects | Remove pending work and inactive material | Complete response |
| `AwaitingDecision` | Listener accepts Empty/Inline | Transition the same row to `AcceptedActivation` | Activate |
| `AwaitingDecision` | Listener accepts Metadata | Remove pending work and inactive material | Return unsupported-snapshot error |
| `AcceptedActivation` | Activation commits | Insert active marker, embed rows, remove pending work | Install live membership and notify rows |

Conflicting work for an occupied target group is rejected. Exact replay is
idempotent, including the transition from the same decision payload to accepted
activation.

## Activation

`InitialSnapshot::Empty` activates the target group without row writes.

`InitialSnapshot::Inline` contains projected row values. The runtime embeds
deterministic CRDT state using `UpdateId(0, 0)`, writes one dataset patch at a
time in the activation transaction, and inserts the active-group marker in that
same transaction. Listener row changes are emitted only after commit.

The activation listener `ReadToken` contains progress for every active group,
not only the newly activated target group. Until `flotsync-git-03f` is
implemented, target-group activation does not yet apply the required old-group
read-only and inactive transitions.

## Restart Behaviour

At startup:

- every `AwaitingDecision` row is emitted to the listener again
- every `AcceptedActivation` row resumes internally without another listener decision
- inactive material without pending accepted work remains hidden
- listener failure leaves `AwaitingDecision` stored so later startup can retry
- activation failure after accepted-state commit leaves `AcceptedActivation`
  available for startup retry

## Reliable-Delivery Completion

The runtime completes the reliable-delivery processed promise after one outcome
that means the message was processed and does not require retry:

- terminal policy rejection and cleanup
- committed listener-mediated work plus successful listener notification
- committed automatic activation plus successful activation side effects

This acknowledgement stops sender retries for work awaiting a listener. It does
not claim that the listener accepted the proposal or that the target group is
already active. Earlier failures intentionally leave the promise incomplete so
redelivery can retry idempotently.

## Validation Coverage

Checked-in tests cover:

- policy ordering, defaults, and top-level membership-delta derivation
- pending decision replay and listener-response persistence
- automatic-acceptance recovery after a post-commit failure
- invitation and proposal accepted-activation resume
- Metadata acceptance rejection without target-group activation
- inactive-material isolation and target-group pending-work uniqueness
- inline row embedding, listener changes, and global read tokens
- proposal delivery to continuing members and migration invitations to added members
