//! Application-specific row reconciliation across checklist group replacements.

use super::{
    ChecklistEvent,
    ChecklistItem,
    ChecklistItemAssociation,
    ChecklistItemField,
    ChecklistItemId,
    ChecklistRowChange,
    ChecklistWorkingSet,
    ChecklistWorkingSetError,
    DirtyReplacementSuccessorSnafu,
    DirtyRowKind,
    FIELD_EDIT_COUNT,
    InvalidReplacementChangeSnafu,
    UnexpectedReplacementRowGroupSnafu,
    push_display_item,
};
use flotsync_core::GroupId;
use flotsync_data_types::RowValueRead;
use flotsync_replication::{
    AcceptedCutRelation,
    MigrationId,
    PreviousRow,
    PreviousRowAbsence,
    PreviousRowCreator,
    PreviousRowEvidence,
    RowChange,
    RowChangeKind,
    RowFieldDifference,
    RowId,
};
use snafu::ensure;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::SystemTime,
};

/// Result of committing one completely resolved group replacement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ChecklistReplacementOutcome {
    /// Number of resolutions retained as dirty successor-group work.
    pub dirty_resolution_count: usize,
}

/// One ambiguous local-versus-remote row decision shown by the REPL wizard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ChecklistReconciliation {
    /// Old group-scoped identity, including for a locally inserted occurrence.
    pub old_item_id: ChecklistItemId,
    /// Successor group-scoped identity used by either resolution.
    pub new_item_id: ChecklistItemId,
    /// Locally visible value, or absence for a local deletion/tombstone.
    pub local: Option<ChecklistItem>,
    /// Successor value, or absence when the replacement removed the row.
    pub remote: Option<ChecklistItem>,
    /// Canonical framework-reported differences, or `None` when no comparison was available.
    pub differing_fields: Option<Box<[RowFieldDifference]>>,
    /// Available evidence about the corresponding old-group occurrence.
    pub evidence: Option<PreviousRowEvidence>,
}

/// Staged replacement whose ambiguous rows must be resolved before commit.
pub(super) struct ChecklistReplacementPlan {
    /// Group transition represented by the staged event.
    migration_id: MigrationId,
    /// Isolated state mutated until every ambiguous row is resolved.
    state: ReplacementWorkingState,
    /// Ambiguous rows in deterministic successor-identity order.
    reconciliations: Vec<ChecklistReconciliation>,
    /// Raw application event recorded when the complete plan commits.
    event_changes: Vec<ChecklistRowChange>,
    /// Prefix of `reconciliations` already applied to `state`.
    resolved_count: usize,
}

impl ChecklistReplacementPlan {
    /// Group transition represented by this staged plan.
    pub const fn migration_id(&self) -> MigrationId {
        self.migration_id
    }

    /// Ambiguous decisions in deterministic successor-identity order.
    pub fn reconciliations(&self) -> &[ChecklistReconciliation] {
        &self.reconciliations
    }

    /// Apply one selected row value to the next ambiguous decision.
    ///
    /// `selected` is `None` when the chosen side represents row absence.
    ///
    /// # Panics
    ///
    /// Panics if every reconciliation has already been resolved.
    pub fn resolve_next(&mut self, selected: Option<ChecklistItem>) {
        let reconciliation = self
            .reconciliations
            .get(self.resolved_count)
            .expect("replacement plan must retain an unresolved reconciliation");
        let value = if selected == reconciliation.remote {
            ReplacementValue::Clean(selected)
        } else {
            ReplacementValue::Dirty(selected)
        };
        apply_selected_value(
            &mut self.state,
            ReplacementSelection {
                old_item_id: reconciliation.old_item_id,
                new_item_id: reconciliation.new_item_id,
                remote: reconciliation.remote.as_ref(),
                value,
            },
        );
        self.resolved_count += 1;
    }

    /// Commit the fully resolved plan to the application working set.
    ///
    /// # Panics
    ///
    /// Panics if not every staged reconciliation was resolved.
    pub fn commit(self, working_set: &mut ChecklistWorkingSet) -> ChecklistReplacementOutcome {
        assert_eq!(
            self.resolved_count,
            self.reconciliations.len(),
            "every replacement reconciliation must be resolved before commit"
        );
        working_set.rows = self.state.rows;
        working_set.display_order = self.state.display_order;
        working_set.dirty_rows = self.state.dirty_rows;
        let event = ChecklistEvent {
            timestamp: SystemTime::now(),
            changes: self.event_changes,
        };
        working_set.event_history.push(event);
        let dirty_resolution_count = working_set
            .dirty_rows
            .keys()
            .filter(|item_id| {
                item_id.association
                    == ChecklistItemAssociation::Group(self.migration_id.new_group_id)
            })
            .count();
        ChecklistReplacementOutcome {
            dirty_resolution_count,
        }
    }
}

impl ChecklistWorkingSet {
    /// Stage one complete group replacement without mutating visible or dirty state.
    ///
    /// # Errors
    ///
    /// Returns an error when row data cannot be decoded or the replacement rows
    /// are inconsistent with `migration_id`.
    pub(super) fn prepare_group_replacement(
        &self,
        migration_id: MigrationId,
        changes: Vec<RowChange>,
    ) -> Result<ChecklistReplacementPlan, ChecklistWorkingSetError> {
        if let Some(&item_id) = self.dirty_rows.keys().find(|item_id| {
            item_id.association == ChecklistItemAssociation::Group(migration_id.new_group_id)
        }) {
            return DirtyReplacementSuccessorSnafu { item_id }.fail();
        }
        let mut state = ReplacementWorkingState::from_working_set(self);
        let mut reconciliations = Vec::new();
        let mut event_changes = Vec::with_capacity(changes.len());
        let mut processed_old_ids = HashSet::new();

        for change in changes {
            let transition = self.decode_replacement_transition(migration_id, change)?;
            event_changes.push(transition.event_change.clone());
            if let Some(old_item_id) = transition.old_item_id {
                processed_old_ids.insert(old_item_id);
            }
            reconcile_transition(
                self,
                &mut state,
                &mut reconciliations,
                migration_id,
                transition,
            );
        }

        reconcile_unreported_dirty_rows(
            self,
            &mut state,
            &mut reconciliations,
            migration_id,
            &processed_old_ids,
        );
        reconciliations.sort_by_key(|reconciliation| reconciliation.new_item_id);
        Ok(ChecklistReplacementPlan {
            migration_id,
            state,
            reconciliations,
            event_changes,
            resolved_count: 0,
        })
    }

    /// Dispatch one framework change by predecessor and operation shape.
    fn decode_replacement_transition(
        &self,
        migration_id: MigrationId,
        change: RowChange,
    ) -> Result<ReplacementTransition, ChecklistWorkingSetError> {
        match (change.previous, change.change) {
            (
                PreviousRow::Present { row_id, evidence },
                RowChangeKind::Upsert {
                    row_id: successor_row_id,
                    row,
                    previous_value_differences,
                },
            ) => self.decode_present_upsert(
                migration_id,
                &row_id,
                evidence,
                ReplacementUpsertInput {
                    row_id: successor_row_id,
                    row,
                    differing_fields: previous_value_differences,
                },
            ),
            (
                PreviousRow::Present { row_id, evidence },
                RowChangeKind::Delete { row_id: deleted_id },
            ) => self.decode_present_delete(migration_id, &row_id, &deleted_id, evidence),
            (
                PreviousRow::Absent(absence),
                RowChangeKind::Upsert {
                    row_id,
                    row,
                    previous_value_differences,
                },
            ) => self.decode_absent_upsert(
                migration_id,
                absence,
                ReplacementUpsertInput {
                    row_id,
                    row,
                    differing_fields: previous_value_differences,
                },
            ),
            (
                PreviousRow::Unavailable,
                RowChangeKind::Upsert {
                    row_id,
                    row,
                    previous_value_differences,
                },
            ) => self.decode_unavailable_upsert(
                migration_id,
                ReplacementUpsertInput {
                    row_id,
                    row,
                    differing_fields: previous_value_differences,
                },
            ),
            (PreviousRow::NotCompared, _) => {
                Err(ChecklistWorkingSetError::InvalidReplacementChange {
                    reason: "replacement row was not compared with an old group",
                })
            }
            (_, RowChangeKind::Delete { .. }) => {
                Err(ChecklistWorkingSetError::InvalidReplacementChange {
                    reason: "replacement delete did not identify a visible old-group row",
                })
            }
        }
    }

    /// Decode a visible old row becoming a visible successor row.
    fn decode_present_upsert(
        &self,
        migration_id: MigrationId,
        old_row_id: &RowId,
        evidence: PreviousRowEvidence,
        successor: ReplacementUpsertInput,
    ) -> Result<ReplacementTransition, ChecklistWorkingSetError> {
        self.validate_replacement_row_id(old_row_id, migration_id.old_group_id, "old")?;
        self.validate_replacement_row_id(
            &successor.row_id,
            migration_id.new_group_id,
            "successor",
        )?;
        ensure!(
            old_row_id.same_dataset_row(&successor.row_id),
            InvalidReplacementChangeSnafu {
                reason: "old and successor row identities did not use the same dataset and row key",
            }
        );
        let item = ChecklistItem::from_row(successor.row_id.row_key, successor.row.as_ref())?;
        let new_item_id =
            ChecklistItemId::group(migration_id.new_group_id, successor.row_id.row_key);
        Ok(ReplacementTransition {
            old_item_id: Some(ChecklistItemId::group(
                migration_id.old_group_id,
                old_row_id.row_key,
            )),
            new_item_id,
            remote: Some(item.clone()),
            previous: ReplacementPrevious::Present(evidence),
            differing_fields: successor.differing_fields,
            event_change: ChecklistRowChange::Upsert {
                item_id: new_item_id,
                item,
            },
        })
    }

    /// Decode a visible old row which the replacement removed.
    fn decode_present_delete(
        &self,
        migration_id: MigrationId,
        old_row_id: &RowId,
        deleted_row_id: &RowId,
        evidence: PreviousRowEvidence,
    ) -> Result<ReplacementTransition, ChecklistWorkingSetError> {
        self.validate_replacement_row_id(old_row_id, migration_id.old_group_id, "old")?;
        self.validate_replacement_row_id(deleted_row_id, migration_id.old_group_id, "deleted")?;
        ensure!(
            old_row_id.same_dataset_row(deleted_row_id),
            InvalidReplacementChangeSnafu {
                reason: "previous and deleted row identities did not use the same dataset and row key",
            }
        );
        let old_item_id = ChecklistItemId::group(migration_id.old_group_id, deleted_row_id.row_key);
        Ok(ReplacementTransition {
            old_item_id: Some(old_item_id),
            new_item_id: ChecklistItemId::group(migration_id.new_group_id, deleted_row_id.row_key),
            remote: None,
            previous: ReplacementPrevious::Present(evidence),
            differing_fields: Some(Box::default()),
            event_change: ChecklistRowChange::Delete {
                item_id: old_item_id,
            },
        })
    }

    /// Decode a successor row whose predecessor was absent or tombstoned.
    fn decode_absent_upsert(
        &self,
        migration_id: MigrationId,
        absence: PreviousRowAbsence,
        successor: ReplacementUpsertInput,
    ) -> Result<ReplacementTransition, ChecklistWorkingSetError> {
        self.validate_replacement_row_id(
            &successor.row_id,
            migration_id.new_group_id,
            "successor",
        )?;
        let item = ChecklistItem::from_row(successor.row_id.row_key, successor.row.as_ref())?;
        let (old_item_id, previous) = match absence {
            PreviousRowAbsence::NotStored => (
                Some(ChecklistItemId::group(
                    migration_id.old_group_id,
                    successor.row_id.row_key,
                )),
                ReplacementPrevious::NotStored,
            ),
            PreviousRowAbsence::Tombstoned { row_id, evidence } => {
                self.validate_replacement_row_id(&row_id, migration_id.old_group_id, "tombstoned")?;
                ensure!(
                    row_id.same_dataset_row(&successor.row_id),
                    InvalidReplacementChangeSnafu {
                        reason: "tombstoned and successor row identities did not use the same dataset and row key",
                    }
                );
                (
                    Some(ChecklistItemId::group(
                        migration_id.old_group_id,
                        row_id.row_key,
                    )),
                    ReplacementPrevious::Tombstoned(evidence),
                )
            }
        };
        let new_item_id =
            ChecklistItemId::group(migration_id.new_group_id, successor.row_id.row_key);
        Ok(ReplacementTransition {
            old_item_id,
            new_item_id,
            remote: Some(item.clone()),
            previous,
            differing_fields: successor.differing_fields,
            event_change: ChecklistRowChange::Upsert {
                item_id: new_item_id,
                item,
            },
        })
    }

    /// Decode a successor row whose predecessor group was unavailable locally.
    fn decode_unavailable_upsert(
        &self,
        migration_id: MigrationId,
        successor: ReplacementUpsertInput,
    ) -> Result<ReplacementTransition, ChecklistWorkingSetError> {
        self.validate_replacement_row_id(
            &successor.row_id,
            migration_id.new_group_id,
            "successor",
        )?;
        let item = ChecklistItem::from_row(successor.row_id.row_key, successor.row.as_ref())?;
        let new_item_id =
            ChecklistItemId::group(migration_id.new_group_id, successor.row_id.row_key);
        Ok(ReplacementTransition {
            old_item_id: None,
            new_item_id,
            remote: Some(item.clone()),
            previous: ReplacementPrevious::Unavailable,
            differing_fields: successor.differing_fields,
            event_change: ChecklistRowChange::Upsert {
                item_id: new_item_id,
                item,
            },
        })
    }

    /// Validate a replacement row against the checklist dataset and its migration-side group.
    ///
    /// The row must belong to the checklist dataset and to `expected_group_id`. A mismatch returns
    /// either the ordinary checklist dataset error or `UnexpectedReplacementRowGroup`, whose
    /// `role` identifies the old, deleted, tombstoned, or successor position being checked.
    fn validate_replacement_row_id(
        &self,
        row_id: &RowId,
        expected_group_id: GroupId,
        role: &'static str,
    ) -> Result<(), ChecklistWorkingSetError> {
        self.validate_row_dataset(row_id)?;
        ensure!(
            row_id.group_id == expected_group_id,
            UnexpectedReplacementRowGroupSnafu {
                row: row_id.clone(),
                expected_group_id,
                role,
            }
        );
        Ok(())
    }
}

/// Mutable working-set fields cloned while a replacement is staged.
struct ReplacementWorkingState {
    /// Visible checklist values keyed by their group-scoped identities.
    rows: HashMap<ChecklistItemId, ChecklistItem>,
    /// Stable display order for visible identities.
    display_order: Vec<ChecklistItemId>,
    /// Local publication work retained alongside the staged values.
    dirty_rows: HashMap<ChecklistItemId, DirtyRowKind>,
}

impl ReplacementWorkingState {
    /// Clone only state which a replacement is allowed to change.
    fn from_working_set(working_set: &ChecklistWorkingSet) -> Self {
        Self {
            rows: working_set.rows.clone(),
            display_order: working_set.display_order.clone(),
            dirty_rows: working_set.dirty_rows.clone(),
        }
    }
}

/// Decoded application-facing transition for one row key.
struct ReplacementTransition {
    /// Old-group identity when the framework supplied or implied one.
    old_item_id: Option<ChecklistItemId>,
    /// Successor-group identity used by the application view.
    new_item_id: ChecklistItemId,
    /// Complete successor value, or absence when the replacement removed it.
    remote: Option<ChecklistItem>,
    /// Shape and evidence of the predecessor occurrence.
    previous: ReplacementPrevious,
    /// Canonical comparison data retained without copying field names.
    differing_fields: Option<Box<[RowFieldDifference]>>,
    /// Raw checklist event recorded when the complete replacement commits.
    event_change: ChecklistRowChange,
}

/// Decoded fields from one framework replacement upsert.
struct ReplacementUpsertInput {
    /// Successor group-scoped row identity.
    row_id: RowId,
    /// Complete framework-delivered successor value.
    row: Arc<dyn RowValueRead + Send + Sync>,
    /// Optional comparison with the corresponding predecessor value.
    differing_fields: Option<Box<[RowFieldDifference]>>,
}

/// Old-group evidence shape for one decoded replacement row.
enum ReplacementPrevious {
    /// A visible predecessor row with inclusion evidence.
    Present(PreviousRowEvidence),
    /// The inspected predecessor group stored no occurrence for the row key.
    NotStored,
    /// The inspected predecessor group retained a deletion with inclusion evidence.
    Tombstoned(PreviousRowEvidence),
    /// The predecessor group was unavailable to the framework.
    Unavailable,
}

impl ReplacementPrevious {
    /// Return occurrence evidence when the old group retained one.
    const fn evidence(&self) -> Option<PreviousRowEvidence> {
        match self {
            Self::Present(evidence) | Self::Tombstoned(evidence) => Some(*evidence),
            Self::NotStored | Self::Unavailable => None,
        }
    }
}

/// Complete application state needed to decide one replacement row.
struct ReplacementRowContext {
    /// Old-group identity removed when the resolution is applied.
    old_item_id: ChecklistItemId,
    /// Successor-group identity receiving the selected value.
    new_item_id: ChecklistItemId,
    /// Locally visible value before the replacement, if present.
    local: Option<ChecklistItem>,
    /// Framework-delivered successor value, if present.
    remote: Option<ChecklistItem>,
    /// Shape and evidence of the predecessor occurrence.
    previous: ReplacementPrevious,
    /// Optional canonical comparison between predecessor and successor values.
    differing_fields: Option<Box<[RowFieldDifference]>>,
}

/// Policy result for one replacement row.
enum ReplacementDisposition {
    /// Apply a value without retaining publication work.
    Clean(Option<ChecklistItem>),
    /// Apply a value and retain it for later publication.
    Dirty(Option<ChecklistItem>),
    /// Ask the user to choose between the available candidates.
    Reconcile,
}

/// Whether a selected value is clean or must be published later.
enum ReplacementValue {
    Clean(Option<ChecklistItem>),
    Dirty(Option<ChecklistItem>),
}

/// Named inputs for replacing one old identity with its selected successor value.
struct ReplacementSelection<'a> {
    /// Old-group identity removed by the application.
    old_item_id: ChecklistItemId,
    /// Successor-group identity receiving the selected state.
    new_item_id: ChecklistItemId,
    /// Framework successor value used as the dirty-update base.
    remote: Option<&'a ChecklistItem>,
    /// Selected state and whether it needs later publication.
    value: ReplacementValue,
}

/// Reconcile one decoded transition into automatic state or a wizard decision.
fn reconcile_transition(
    working_set: &ChecklistWorkingSet,
    state: &mut ReplacementWorkingState,
    reconciliations: &mut Vec<ChecklistReconciliation>,
    migration_id: MigrationId,
    transition: ReplacementTransition,
) {
    let old_item_id = transition.old_item_id.unwrap_or_else(|| {
        ChecklistItemId::group(migration_id.old_group_id, transition.new_item_id.row_key)
    });
    let dirty = working_set.dirty_rows.get(&old_item_id);
    let context = ReplacementRowContext {
        old_item_id,
        new_item_id: transition.new_item_id,
        local: working_set.rows.get(&old_item_id).cloned(),
        remote: transition.remote,
        previous: transition.previous,
        differing_fields: transition.differing_fields,
    };
    let disposition = match dirty {
        Some(DirtyRowKind::Insert) => reconcile_local_value(&context, None),
        Some(DirtyRowKind::Update { original }) => reconcile_local_value(&context, Some(original)),
        Some(DirtyRowKind::Delete) if context.remote.is_none() => {
            ReplacementDisposition::Clean(None)
        }
        Some(DirtyRowKind::Delete) => ReplacementDisposition::Reconcile,
        None => reconcile_clean_transition(&context),
    };
    finish_reconciliation(state, reconciliations, context, disposition);
}

/// Reconcile a locally inserted or updated value with the successor result.
fn reconcile_local_value(
    context: &ReplacementRowContext,
    original: Option<&ChecklistItem>,
) -> ReplacementDisposition {
    if let Some(local) = context.local.as_ref() {
        if context.remote.as_ref() == Some(local) {
            ReplacementDisposition::Clean(context.remote.clone())
        } else if let Some(rebased) = rebase_local_value(context, local, original) {
            if context.remote.as_ref() == Some(&rebased) {
                ReplacementDisposition::Clean(Some(rebased))
            } else {
                ReplacementDisposition::Dirty(Some(rebased))
            }
        } else if context.remote.is_none() && original.is_none() {
            ReplacementDisposition::Dirty(context.local.clone())
        } else {
            ReplacementDisposition::Reconcile
        }
    } else {
        ReplacementDisposition::Reconcile
    }
}

/// Apply policy for a row without unsynchronised local dirty state.
fn reconcile_clean_transition(context: &ReplacementRowContext) -> ReplacementDisposition {
    if context.local == context.remote {
        return ReplacementDisposition::Clean(context.remote.clone());
    }
    match &context.previous {
        ReplacementPrevious::Present(evidence) => match (&context.local, &context.remote) {
            (Some(_), Some(remote))
                if evidence.last_state == Some(AcceptedCutRelation::Included) =>
            {
                ReplacementDisposition::Clean(Some(remote.clone()))
            }
            (Some(_), None)
                if evidence.creation == Some(AcceptedCutRelation::Included)
                    && evidence.last_state == Some(AcceptedCutRelation::Included) =>
            {
                ReplacementDisposition::Clean(None)
            }
            (Some(local), None)
                if evidence.creator == Some(PreviousRowCreator::Local)
                    && evidence.creation == Some(AcceptedCutRelation::NotIncluded) =>
            {
                ReplacementDisposition::Dirty(Some(local.clone()))
            }
            _ => ReplacementDisposition::Reconcile,
        },
        ReplacementPrevious::NotStored | ReplacementPrevious::Unavailable => {
            if context.local.is_none() {
                ReplacementDisposition::Clean(context.remote.clone())
            } else {
                ReplacementDisposition::Reconcile
            }
        }
        ReplacementPrevious::Tombstoned(evidence) => {
            if context.local.is_none() && evidence.last_state == Some(AcceptedCutRelation::Included)
            {
                ReplacementDisposition::Clean(context.remote.clone())
            } else {
                ReplacementDisposition::Reconcile
            }
        }
    }
}

/// Rebase disjoint local fields onto the successor when comparison evidence permits it.
fn rebase_local_value(
    context: &ReplacementRowContext,
    local: &ChecklistItem,
    original: Option<&ChecklistItem>,
) -> Option<ChecklistItem> {
    let (Some(original), Some(remote), Some(differing_fields)) = (
        original,
        context.remote.as_ref(),
        context.differing_fields.as_ref(),
    ) else {
        return None;
    };
    let local_changes = local.changes_since(original);
    let has_remote_fields = differing_fields
        .iter()
        .any(|difference| difference.field_name() != FIELD_EDIT_COUNT);
    let overlaps = differing_fields.iter().any(|difference| {
        let field_name = difference.field_name();
        field_name != FIELD_EDIT_COUNT
            && ChecklistItemField::from_schema_name(field_name)
                .is_some_and(|field| local_changes.contains(field))
    });
    let safe_rebase = !has_remote_fields
        || context.previous.evidence().is_some_and(|evidence| {
            evidence.last_state == Some(AcceptedCutRelation::Included) && !overlaps
        });
    if safe_rebase {
        let mut rebased = remote.clone();
        rebased.apply_changes_from(local, local_changes);
        Some(rebased)
    } else {
        None
    }
}

/// Apply an automatic result or retain the row for interactive reconciliation.
fn finish_reconciliation(
    state: &mut ReplacementWorkingState,
    reconciliations: &mut Vec<ChecklistReconciliation>,
    context: ReplacementRowContext,
    disposition: ReplacementDisposition,
) {
    match disposition {
        ReplacementDisposition::Clean(selected) => apply_selected_value(
            state,
            ReplacementSelection {
                old_item_id: context.old_item_id,
                new_item_id: context.new_item_id,
                remote: context.remote.as_ref(),
                value: ReplacementValue::Clean(selected),
            },
        ),
        ReplacementDisposition::Dirty(selected) => apply_selected_value(
            state,
            ReplacementSelection {
                old_item_id: context.old_item_id,
                new_item_id: context.new_item_id,
                remote: context.remote.as_ref(),
                value: ReplacementValue::Dirty(selected),
            },
        ),
        ReplacementDisposition::Reconcile => reconciliations.push(ChecklistReconciliation {
            old_item_id: context.old_item_id,
            new_item_id: context.new_item_id,
            local: context.local,
            remote: context.remote,
            differing_fields: context.differing_fields,
            evidence: context.previous.evidence(),
        }),
    }
}

/// Carry dirty old-group rows which were not represented by a stored transition.
fn reconcile_unreported_dirty_rows(
    working_set: &ChecklistWorkingSet,
    state: &mut ReplacementWorkingState,
    reconciliations: &mut Vec<ChecklistReconciliation>,
    migration_id: MigrationId,
    processed_old_ids: &HashSet<ChecklistItemId>,
) {
    for (&old_item_id, dirty) in &working_set.dirty_rows {
        if old_item_id.association != ChecklistItemAssociation::Group(migration_id.old_group_id)
            || processed_old_ids.contains(&old_item_id)
        {
            continue;
        }
        let new_item_id = ChecklistItemId::group(migration_id.new_group_id, old_item_id.row_key);
        let local = working_set.rows.get(&old_item_id).cloned();
        match dirty {
            DirtyRowKind::Insert => {
                apply_selected_value(
                    state,
                    ReplacementSelection {
                        old_item_id,
                        new_item_id,
                        remote: None,
                        value: ReplacementValue::Dirty(local),
                    },
                );
            }
            DirtyRowKind::Delete => {
                apply_selected_value(
                    state,
                    ReplacementSelection {
                        old_item_id,
                        new_item_id,
                        remote: None,
                        value: ReplacementValue::Clean(None),
                    },
                );
            }
            DirtyRowKind::Update { .. } => reconciliations.push(ChecklistReconciliation {
                old_item_id,
                new_item_id,
                local,
                remote: None,
                differing_fields: None,
                evidence: None,
            }),
        }
    }
}

/// Replace the old identity with one selected successor value and dirty state.
fn apply_selected_value(state: &mut ReplacementWorkingState, selection: ReplacementSelection<'_>) {
    state.rows.remove(&selection.old_item_id);
    state.rows.remove(&selection.new_item_id);
    state
        .display_order
        .retain(|item_id| *item_id != selection.old_item_id && *item_id != selection.new_item_id);
    state.dirty_rows.remove(&selection.old_item_id);
    state.dirty_rows.remove(&selection.new_item_id);

    match selection.value {
        ReplacementValue::Clean(Some(selected)) => {
            state.rows.insert(selection.new_item_id, selected);
            push_display_item(&mut state.display_order, selection.new_item_id);
        }
        ReplacementValue::Dirty(Some(selected)) => {
            state.rows.insert(selection.new_item_id, selected);
            push_display_item(&mut state.display_order, selection.new_item_id);
            let dirty_kind =
                selection
                    .remote
                    .map_or(DirtyRowKind::Insert, |remote| DirtyRowKind::Update {
                        original: remote.clone(),
                    });
            state.dirty_rows.insert(selection.new_item_id, dirty_kind);
        }
        ReplacementValue::Dirty(None) if selection.remote.is_some() => {
            state
                .dirty_rows
                .insert(selection.new_item_id, DirtyRowKind::Delete);
        }
        ReplacementValue::Clean(None) | ReplacementValue::Dirty(None) => {
            // With neither a selected nor remote row, absence needs no visible or dirty state.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replicated_checklist::{
        ChecklistStatus,
        FIELD_NOTE,
        FIELD_PRIORITY,
        FIELD_TEXT,
        checklist_dataset_id,
    };
    use flotsync_replication::{PreviousRow, RowKey, RowValues};
    use std::{borrow::Cow, collections::BTreeSet, sync::Arc};
    use uuid::Uuid;

    /// Fixed old-to-new group transition used by reconciliation tests.
    fn migration_id() -> MigrationId {
        MigrationId {
            old_group_id: GroupId(Uuid::from_u128(40)),
            new_group_id: GroupId(Uuid::from_u128(41)),
        }
    }

    /// Build a complete checklist item with an explicit edit count.
    fn item(text: &str, note: &str, edit_count: u64) -> ChecklistItem {
        ChecklistItem {
            text: text.to_owned(),
            note: note.to_owned(),
            tags: BTreeSet::new(),
            status: ChecklistStatus::Open,
            priority: 0,
            edit_count,
        }
    }

    /// Encode one checklist item as a complete listener row.
    fn row_values(item: &ChecklistItem) -> Arc<RowValues> {
        Arc::new(RowValues::from_fields_unchecked(
            item.to_row_values_patch().fields,
        ))
    }

    /// Build one corresponding visible-row upsert.
    fn corresponding_upsert(
        row_key: RowKey,
        remote: &ChecklistItem,
        evidence: PreviousRowEvidence,
        differences: &[&'static str],
    ) -> RowChange {
        let migration_id = migration_id();
        RowChange {
            previous: PreviousRow::Present {
                row_id: RowId::new(migration_id.old_group_id, checklist_dataset_id(), row_key),
                evidence,
            },
            change: RowChangeKind::Upsert {
                row_id: RowId::new(migration_id.new_group_id, checklist_dataset_id(), row_key),
                row: row_values(remote),
                previous_value_differences: Some(
                    differences
                        .iter()
                        .map(|field_name| RowFieldDifference::ValueChanged {
                            field_name: Cow::Borrowed(field_name),
                        })
                        .collect(),
                ),
            },
        }
    }

    /// Build one old-group delete for a previously visible row.
    fn corresponding_delete(row_key: RowKey, evidence: PreviousRowEvidence) -> RowChange {
        let migration_id = migration_id();
        let row_id = RowId::new(migration_id.old_group_id, checklist_dataset_id(), row_key);
        RowChange {
            previous: PreviousRow::Present {
                row_id: row_id.clone(),
                evidence,
            },
            change: RowChangeKind::Delete { row_id },
        }
    }

    /// Build one successor upsert for an absent or unavailable old row.
    fn introduced_upsert(
        row_key: RowKey,
        remote: &ChecklistItem,
        previous: PreviousRow,
    ) -> RowChange {
        RowChange {
            previous,
            change: RowChangeKind::Upsert {
                row_id: RowId::new(migration_id().new_group_id, checklist_dataset_id(), row_key),
                row: row_values(remote),
                previous_value_differences: None,
            },
        }
    }

    /// Evidence establishing that the accepted cut included the complete old row.
    fn included_evidence() -> PreviousRowEvidence {
        PreviousRowEvidence {
            creator: Some(PreviousRowCreator::Local),
            creation: Some(AcceptedCutRelation::Included),
            last_state: Some(AcceptedCutRelation::Included),
        }
    }

    /// Load one clean old-group item into a working set.
    fn clean_old_item(
        working_set: &mut ChecklistWorkingSet,
        row_key: RowKey,
        value: ChecklistItem,
    ) -> ChecklistItemId {
        let item_id = ChecklistItemId::group(migration_id().old_group_id, row_key);
        working_set
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id,
                item: value,
            }])
            .expect("clean test row should enqueue");
        assert_eq!(working_set.drain_queued_events(), 1);
        item_id
    }

    #[test]
    fn identical_clean_row_remaps_to_successor_without_a_decision() {
        let row_key = RowKey(Uuid::from_u128(1));
        let value = item("same", "", 1);
        let mut working_set = ChecklistWorkingSet::new();
        let old_item_id = clean_old_item(&mut working_set, row_key, value.clone());

        let plan = working_set
            .prepare_group_replacement(
                migration_id(),
                vec![corresponding_upsert(
                    row_key,
                    &value,
                    included_evidence(),
                    &[],
                )],
            )
            .expect("identical replacement should stage");
        assert!(plan.reconciliations().is_empty());
        let outcome = plan.commit(&mut working_set);

        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        assert!(working_set.item(old_item_id).is_none());
        assert_eq!(working_set.item(new_item_id), Some(&value));
        assert_eq!(outcome.dirty_resolution_count, 0);
    }

    #[test]
    fn disjoint_dirty_update_rebases_onto_included_remote_fields() {
        let row_key = RowKey(Uuid::from_u128(2));
        let base = item("base", "", 1);
        let mut remote = item("base", "remote note", 2);
        remote.priority = 4;
        let mut working_set = ChecklistWorkingSet::new();
        let old_item_id = clean_old_item(&mut working_set, row_key, base);
        working_set
            .rename_item(old_item_id, "local title")
            .expect("local title edit should apply");

        let plan = working_set
            .prepare_group_replacement(
                migration_id(),
                vec![corresponding_upsert(
                    row_key,
                    &remote,
                    included_evidence(),
                    &[FIELD_NOTE, FIELD_PRIORITY, FIELD_EDIT_COUNT],
                )],
            )
            .expect("disjoint replacement should stage");
        assert!(plan.reconciliations().is_empty());
        let outcome = plan.commit(&mut working_set);

        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        let merged = working_set
            .item(new_item_id)
            .expect("successor row should exist");
        assert_eq!(merged.text, "local title");
        assert_eq!(merged.note, "remote note");
        assert_eq!(merged.priority, 4);
        assert_eq!(outcome.dirty_resolution_count, 1);
    }

    #[test]
    fn dirty_insert_collision_uses_the_same_local_remote_decision() {
        let row_key = RowKey(Uuid::from_u128(3));
        let old_item_id = ChecklistItemId::group(migration_id().old_group_id, row_key);
        let local = item("local insert", "", 1);
        let remote = item("remote insert", "", 1);
        let mut working_set = ChecklistWorkingSet::new();
        working_set.add_item_with_id(old_item_id, local.text.clone());
        let new_row_id = RowId::new(migration_id().new_group_id, checklist_dataset_id(), row_key);
        let change = RowChange {
            previous: PreviousRow::Absent(PreviousRowAbsence::NotStored),
            change: RowChangeKind::Upsert {
                row_id: new_row_id,
                row: row_values(&remote),
                previous_value_differences: None,
            },
        };

        let mut plan = working_set
            .prepare_group_replacement(migration_id(), vec![change])
            .expect("insert collision should stage");
        assert_eq!(plan.reconciliations().len(), 1);
        assert_eq!(plan.reconciliations()[0].local, Some(local.clone()));
        assert_eq!(plan.reconciliations()[0].remote, Some(remote));
        plan.resolve_next(Some(local.clone()));
        let outcome = plan.commit(&mut working_set);

        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        assert_eq!(working_set.item(new_item_id), Some(&local));
        assert_eq!(outcome.dirty_resolution_count, 1);
    }

    #[test]
    fn omitted_old_state_requires_resolution_and_can_accept_remote() {
        let row_key = RowKey(Uuid::from_u128(4));
        let local = item("old local", "", 2);
        let remote = item("successor", "", 1);
        let mut working_set = ChecklistWorkingSet::new();
        clean_old_item(&mut working_set, row_key, local.clone());
        let evidence = PreviousRowEvidence {
            creator: Some(PreviousRowCreator::Local),
            creation: Some(AcceptedCutRelation::Included),
            last_state: Some(AcceptedCutRelation::NotIncluded),
        };

        let mut plan = working_set
            .prepare_group_replacement(
                migration_id(),
                vec![corresponding_upsert(
                    row_key,
                    &remote,
                    evidence,
                    &[FIELD_TEXT, FIELD_EDIT_COUNT],
                )],
            )
            .expect("omitted state should stage");
        assert_eq!(plan.reconciliations().len(), 1);
        plan.resolve_next(Some(remote.clone()));
        let outcome = plan.commit(&mut working_set);

        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        assert_eq!(working_set.item(new_item_id), Some(&remote));
        assert_eq!(outcome.dirty_resolution_count, 0);
    }

    #[test]
    fn unreported_dirty_insert_moves_to_successor_and_remains_dirty() {
        let row_key = RowKey(Uuid::from_u128(5));
        let old_item_id = ChecklistItemId::group(migration_id().old_group_id, row_key);
        let mut working_set = ChecklistWorkingSet::new();
        working_set.add_item_with_id(old_item_id, "not yet published");

        let plan = working_set
            .prepare_group_replacement(migration_id(), Vec::new())
            .expect("empty replacement should carry local insert");
        assert!(plan.reconciliations().is_empty());
        let outcome = plan.commit(&mut working_set);

        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        assert!(working_set.item(old_item_id).is_none());
        assert_eq!(
            working_set.item(new_item_id).map(|item| item.text.as_str()),
            Some("not yet published")
        );
        assert_eq!(outcome.dirty_resolution_count, 1);
    }

    #[test]
    fn included_clean_divergence_accepts_the_successor() {
        let row_key = RowKey(Uuid::from_u128(6));
        let local = item("old", "", 1);
        let remote = item("successor", "", 2);
        let mut working_set = ChecklistWorkingSet::new();
        clean_old_item(&mut working_set, row_key, local);

        let plan = working_set
            .prepare_group_replacement(
                migration_id(),
                vec![corresponding_upsert(
                    row_key,
                    &remote,
                    included_evidence(),
                    &[FIELD_TEXT, FIELD_EDIT_COUNT],
                )],
            )
            .expect("included successor should stage");
        assert!(plan.reconciliations().is_empty());
        let outcome = plan.commit(&mut working_set);

        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        assert_eq!(working_set.item(new_item_id), Some(&remote));
        assert_eq!(outcome.dirty_resolution_count, 0);
    }

    #[test]
    fn included_clean_old_only_row_is_removed() {
        let row_key = RowKey(Uuid::from_u128(7));
        let mut working_set = ChecklistWorkingSet::new();
        let old_item_id = clean_old_item(&mut working_set, row_key, item("removed", "", 1));

        let plan = working_set
            .prepare_group_replacement(
                migration_id(),
                vec![corresponding_delete(row_key, included_evidence())],
            )
            .expect("included removal should stage");
        assert!(plan.reconciliations().is_empty());
        plan.commit(&mut working_set);

        assert!(working_set.item(old_item_id).is_none());
        assert!(working_set.dirty_group_ids().is_empty());
    }

    #[test]
    fn omitted_local_creation_is_reinserted_in_the_successor() {
        let row_key = RowKey(Uuid::from_u128(8));
        let local = item("late local row", "", 1);
        let mut working_set = ChecklistWorkingSet::new();
        clean_old_item(&mut working_set, row_key, local.clone());
        let evidence = PreviousRowEvidence {
            creator: Some(PreviousRowCreator::Local),
            creation: Some(AcceptedCutRelation::NotIncluded),
            last_state: Some(AcceptedCutRelation::NotIncluded),
        };

        let plan = working_set
            .prepare_group_replacement(
                migration_id(),
                vec![corresponding_delete(row_key, evidence)],
            )
            .expect("omitted local creation should stage");
        assert!(plan.reconciliations().is_empty());
        let outcome = plan.commit(&mut working_set);

        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        assert_eq!(working_set.item(new_item_id), Some(&local));
        assert_eq!(outcome.dirty_resolution_count, 1);
    }

    #[test]
    fn omitted_tombstone_requires_a_local_absence_decision() {
        let row_key = RowKey(Uuid::from_u128(9));
        let remote = item("restored remotely", "", 1);
        let evidence = PreviousRowEvidence {
            creator: Some(PreviousRowCreator::Other),
            creation: Some(AcceptedCutRelation::Included),
            last_state: Some(AcceptedCutRelation::NotIncluded),
        };
        let change = introduced_upsert(
            row_key,
            &remote,
            PreviousRow::Absent(PreviousRowAbsence::Tombstoned {
                row_id: RowId::new(migration_id().old_group_id, checklist_dataset_id(), row_key),
                evidence,
            }),
        );
        let mut working_set = ChecklistWorkingSet::new();

        let mut plan = working_set
            .prepare_group_replacement(migration_id(), vec![change])
            .expect("omitted tombstone should stage");
        assert_eq!(plan.reconciliations().len(), 1);
        assert_eq!(plan.reconciliations()[0].local, None);
        plan.resolve_next(None);
        let outcome = plan.commit(&mut working_set);

        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        assert!(working_set.item(new_item_id).is_none());
        assert!(matches!(
            working_set.dirty_rows.get(&new_item_id),
            Some(DirtyRowKind::Delete)
        ));
        assert_eq!(outcome.dirty_resolution_count, 1);
    }

    #[test]
    fn overlapping_dirty_update_requires_a_decision() {
        let row_key = RowKey(Uuid::from_u128(10));
        let base = item("base", "", 1);
        let remote = item("remote text", "", 2);
        let mut working_set = ChecklistWorkingSet::new();
        let old_item_id = clean_old_item(&mut working_set, row_key, base);
        working_set
            .rename_item(old_item_id, "local text")
            .expect("local text edit should apply");

        let plan = working_set
            .prepare_group_replacement(
                migration_id(),
                vec![corresponding_upsert(
                    row_key,
                    &remote,
                    included_evidence(),
                    &[FIELD_TEXT, FIELD_EDIT_COUNT],
                )],
            )
            .expect("overlapping update should stage");

        assert_eq!(plan.reconciliations().len(), 1);
        assert_eq!(
            plan.reconciliations()[0]
                .local
                .as_ref()
                .map(|item| item.text.as_str()),
            Some("local text")
        );
    }

    #[test]
    fn dirty_delete_uses_absence_candidate_against_a_successor_row() {
        let row_key = RowKey(Uuid::from_u128(11));
        let remote = item("remote", "", 2);
        let mut working_set = ChecklistWorkingSet::new();
        let old_item_id = clean_old_item(&mut working_set, row_key, item("base", "", 1));
        working_set
            .delete_item(old_item_id)
            .expect("local deletion should stage");

        let mut plan = working_set
            .prepare_group_replacement(
                migration_id(),
                vec![corresponding_upsert(
                    row_key,
                    &remote,
                    included_evidence(),
                    &[FIELD_TEXT, FIELD_EDIT_COUNT],
                )],
            )
            .expect("dirty deletion should stage");
        assert_eq!(plan.reconciliations().len(), 1);
        assert_eq!(plan.reconciliations()[0].local, None);
        assert_eq!(plan.reconciliations()[0].remote, Some(remote));
        plan.resolve_next(None);
        let outcome = plan.commit(&mut working_set);

        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        assert!(working_set.item(new_item_id).is_none());
        assert!(matches!(
            working_set.dirty_rows.get(&new_item_id),
            Some(DirtyRowKind::Delete)
        ));
        assert_eq!(outcome.dirty_resolution_count, 1);
    }

    #[test]
    fn dirty_delete_is_satisfied_by_an_absent_successor_row() {
        let row_key = RowKey(Uuid::from_u128(12));
        let mut working_set = ChecklistWorkingSet::new();
        let old_item_id = clean_old_item(&mut working_set, row_key, item("base", "", 1));
        working_set
            .delete_item(old_item_id)
            .expect("local deletion should stage");

        let plan = working_set
            .prepare_group_replacement(
                migration_id(),
                vec![corresponding_delete(row_key, included_evidence())],
            )
            .expect("satisfied deletion should stage");
        assert!(plan.reconciliations().is_empty());
        let outcome = plan.commit(&mut working_set);

        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        assert!(working_set.item(new_item_id).is_none());
        assert!(!working_set.dirty_rows.contains_key(&new_item_id));
        assert_eq!(outcome.dirty_resolution_count, 0);
    }

    #[test]
    fn missing_comparison_does_not_enable_empty_difference_rebase() {
        let row_key = RowKey(Uuid::from_u128(15));
        let base = item("base", "", 1);
        let remote = item("remote", "", 2);
        let mut working_set = ChecklistWorkingSet::new();
        let old_item_id = clean_old_item(&mut working_set, row_key, base);
        working_set
            .rename_item(old_item_id, "local")
            .expect("local text edit should apply");
        let change = RowChange {
            previous: PreviousRow::Present {
                row_id: RowId::new(migration_id().old_group_id, checklist_dataset_id(), row_key),
                evidence: included_evidence(),
            },
            change: RowChangeKind::Upsert {
                row_id: RowId::new(migration_id().new_group_id, checklist_dataset_id(), row_key),
                row: row_values(&remote),
                previous_value_differences: None,
            },
        };

        let plan = working_set
            .prepare_group_replacement(migration_id(), vec![change])
            .expect("missing comparison should remain resolvable");

        assert_eq!(plan.reconciliations().len(), 1);
        assert!(plan.reconciliations()[0].differing_fields.is_none());
    }

    #[test]
    fn dirty_successor_state_is_rejected_before_replacement_staging() {
        let row_key = RowKey(Uuid::from_u128(16));
        let new_item_id = ChecklistItemId::group(migration_id().new_group_id, row_key);
        let mut working_set = ChecklistWorkingSet::new();
        working_set.add_item_with_id(new_item_id, "premature successor edit");

        assert!(matches!(
            working_set.prepare_group_replacement(migration_id(), Vec::new()),
            Err(ChecklistWorkingSetError::DirtyReplacementSuccessor { item_id })
                if item_id == new_item_id
        ));
    }

    #[test]
    fn unavailable_successor_row_is_accepted_and_can_be_replaced_again() {
        let row_key = RowKey(Uuid::from_u128(13));
        let remote = item("joined successor", "", 1);
        let mut working_set = ChecklistWorkingSet::new();
        let first_plan = working_set
            .prepare_group_replacement(
                migration_id(),
                vec![introduced_upsert(
                    row_key,
                    &remote,
                    PreviousRow::Unavailable,
                )],
            )
            .expect("unavailable predecessor should stage");
        assert!(first_plan.reconciliations().is_empty());
        first_plan.commit(&mut working_set);

        let second_group = GroupId(Uuid::from_u128(42));
        let second_migration = MigrationId {
            old_group_id: migration_id().new_group_id,
            new_group_id: second_group,
        };
        let second_change = RowChange {
            previous: PreviousRow::Present {
                row_id: RowId::new(
                    second_migration.old_group_id,
                    checklist_dataset_id(),
                    row_key,
                ),
                evidence: included_evidence(),
            },
            change: RowChangeKind::Upsert {
                row_id: RowId::new(second_group, checklist_dataset_id(), row_key),
                row: row_values(&remote),
                previous_value_differences: Some(Box::default()),
            },
        };
        let second_plan = working_set
            .prepare_group_replacement(second_migration, vec![second_change])
            .expect("later replacement should use current state");
        assert!(second_plan.reconciliations().is_empty());
        second_plan.commit(&mut working_set);

        assert_eq!(
            working_set.item(ChecklistItemId::group(second_group, row_key)),
            Some(&remote)
        );
    }
}
