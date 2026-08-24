---
type: API Contract
title: Group Replacement Row Lineage
description: Defines listener-visible row correspondence, divergence, and accepted-cut evidence across group replacements.
status: implemented
applies_to:
  - flotsync_replication
source_of_truth:
  - ../flotsync_replication/src/api/changes.rs
  - ../flotsync_replication/src/api/groups.rs
  - ../flotsync_replication/src/runtime/component/group_replacement_provider.rs
  - ../flotsync_replication/src/runtime/component/mod.rs
depends_on:
  - inline_migration_policy_flow.md
tracked_by:
  - flotsync-9fs
  - flotsync-v76
  - flotsync-fk0
---

# Group Replacement Row Lineage

## Scope

This contract defines how a `ReplicationEvent::DataChanged` event exposes one
accepted replacement of an old replication group by its successor. It covers
row correspondence, projected value differences, and conservative evidence
about whether predecessor state belongs to the accepted migration cut.

Changes within one existing group use `DataChangeLineage::Update` and retain
`PreviousRow::NotCompared`. Their upserts have no previous-value differences
because there is no old-to-new group comparison.

## Identity Model

Row identity remains group-scoped. A predecessor and successor with the same
dataset id and row key are corresponding occurrences, but their `RowId` values
remain distinct because their group ids differ. The runtime reports that
correspondence; it does not merge the identities or resolve application state
between them.

Datasets correspond by the same `DatasetId`. Renaming a dataset while retaining
its records is outside this contract. Schema-evolution assistance, including
materialised defaults and removed-column reporting, is deferred to a later
slice.

## Event Boundary

`DataChangeLineage::GroupReplacement { migration_id }` identifies one complete
old-to-new application-view transition. The row provider may return multiple
batches, but all batches belong to that one transition. An empty transition is
still emitted.

Applications should consume and record the complete provider before merging
the event's `ReadToken`. Dropping the provider abandons the remaining portion
of the transition. The token describes the active successor view after the
activation commit; it does not preserve the predecessor as an active group.

## Row Operations

For each dataset and row key, visibility in the predecessor and successor
determines the emitted operation:

| Visible predecessor | Visible successor | Operation | `previous` |
| --- | --- | --- | --- |
| yes | yes | Upsert the successor `RowId` and complete value | `Present` with the predecessor `RowId` and evidence |
| yes | no | Delete the predecessor `RowId` | `Present` with the predecessor `RowId` and evidence |
| no | yes | Upsert the successor `RowId` and complete value | `Absent`, distinguishing no stored occurrence from a tombstone |
| no | no | No operation | Not emitted |

A newly added member does not host the predecessor group. Its visible
successor rows are emitted as upserts with `PreviousRow::Unavailable`. This
means Flotsync could not inspect the old group at all: the application cannot
infer whether a corresponding row previously existed or what its value was.

`PreviousRow::Absent(PreviousRowAbsence::NotStored)` means no predecessor
record was found after Flotsync inspected a locally hosted old group. This is
stronger than `Unavailable`: Flotsync knows the inspected old-group store has
neither a visible occurrence nor a retained deletion for the key.
`PreviousRowAbsence::Tombstoned` means the occurrence did exist and its latest
stored state is deletion; it preserves the predecessor `RowId` and evidence
for that creation and deletion.

## Projected Value Comparison

When both occurrences are visible, Flotsync materialises their
application-facing row values and compares them once. The upsert's
`previous_value_differences` is `Some`, with field differences sorted by
canonical field name. An empty collection means the projected values are
identical.

The current comparison reports `RowFieldDifference::ValueChanged` for fields
whose projected values differ. Applications can rely on this exact comparison
result rather than repeating it with potentially different equality rules.

When no visible predecessor can be compared, `previous_value_differences` is
`None`. The accompanying `PreviousRow` says whether this is an update within
one group, an inspected absence, or unavailable old-group state.

## Accepted-Cut Evidence

`PreviousRowEvidence` reports three independent facts conservatively. Each is
an `Option`; `None` means retained information cannot establish the answer:

- `creator` identifies whether the local application member or another member
  created the old-group occurrence
- `creation` says whether the accepted old-group state used to create the
  successor included creation of that occurrence
- `last_state` says whether that accepted state included the occurrence's
  latest value or, for a tombstone, its deletion

`Included` means the successor creator's accepted old-group state contained
the creation or latest state. `NotIncluded` means the work is known in the
local old-group store but is newer than that accepted state. It is therefore
not known to have reached the member which created the successor, and the
application may need to reapply or reconcile it. Missing provenance,
incompatible vector widths, and concurrent frontiers produce `None` rather
than an inferred answer. The synthetic initial-state origin has no known
member creator, while its version-zero creation is included in every accepted
cut.

## Ownership and Failure Model

Storage supplies row-key-aligned stored transitions and retained provenance.
The replication layer owns application visibility, materialisation, comparison,
and evidence classification. Store implementations may optimise the aligned
scan without changing these listener semantics.

The provider owns a pinned read transaction until it is exhausted or dropped.
Successful exhaustion explicitly releases the transaction; dropping it uses
the store transaction's release-on-drop behaviour. Provider-open and listener
failures happen after group activation commits. This slice does not introduce
an application-event outbox for replaying such failures.

## Application Responsibility

The metadata provides evidence for application-specific reconciliation; it is
not an automatic conflict resolution. An application may remap dirty local
state onto the successor, warn that a local change may be lost, or present a
conflict for user resolution.

The replicated checklist currently performs only mechanical API conformance.
Its dirty-state reconciliation policy and user-visible conflict handling are
tracked separately by `flotsync-fk0`.
