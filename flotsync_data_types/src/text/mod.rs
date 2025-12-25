use crate::InternalSnafu;
use flotsync_utils::{debugging::DebugFormatting, require};
use snafu::prelude::*;
use std::{collections::BTreeSet, fmt, ops::RangeBounds};
use unicode_segmentation::{Graphemes, UnicodeSegmentation};

mod linear_data;
use linear_data::IdGeneratorWithZeroIndex;
pub use linear_data::{
    Composite,
    DataOperation,
    IdWithIndex,
    IdWithIndexRange,
    LinearData,
    LinkIds,
    NodeIdRange,
    NodeIds,
    VecCoalescedLinearData,
    VecCoalescedLinearDataIter,
    VecLinearData,
};
mod linear_string;
pub use linear_string::{LinearString, LinearStringIter, NodeIdRangeString};
mod grapheme_string;
use grapheme_string::GraphemeString;

use crate::InternalError;

/// Simple diffs on plain old strings.
mod text_diff;

#[derive(Debug, Snafu)]
pub enum ApplyError<Id>
where
    Id: fmt::Display,
{
    #[snafu(display("Some operations failed to apply:\n{remaining_diff}"))]
    ApplicationFailed {
        remaining_diff: LinearStringDiff<Id>,
    },
    #[snafu(transparent)]
    Internal { source: InternalError },
}

/// A set of changes that can be applied to a [[LinearString]].
#[derive(Debug)]
pub struct LinearStringDiff<Id> {
    operations: Vec<DataOperation<IdWithIndex<Id>, String>>,
}
impl<Id> LinearStringDiff<Id>
where
    Id: Clone + fmt::Debug + fmt::Display + PartialEq + Eq + PartialOrd + Ord + 'static,
{
    /// Apply all the changes in this diff to `target`.
    pub fn apply_to(self, target: &mut LinearString<Id>) -> Result<(), ApplyError<Id>> {
        let mut iter = self.operations.into_iter();

        for op in iter.by_ref() {
            if let Err(op) = target.apply_operation(op) {
                let (lower, _) = iter.size_hint();
                let mut remaining = Vec::with_capacity(lower + 1);
                remaining.push(op);
                remaining.extend(iter);

                let remaining_diff = LinearStringDiff {
                    operations: remaining,
                };
                return ApplicationFailedSnafu { remaining_diff }.fail();
            }
        }

        Ok(())
    }

    /// Return all ids that are being newly introduced by applying this diff.
    pub fn new_ids(&self) -> BTreeSet<Id> {
        let mut ids = BTreeSet::new();
        for op in self.operations.iter() {
            match op {
                DataOperation::Insert { id, .. } => {
                    ids.insert(id.id.clone());
                }
                DataOperation::Delete { .. } => (), // ignore
            }
        }
        ids
    }

    /// Returns `true` iff this diff is empty, i.e. a no-op.
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Returns how many individual operations there in this diff.
    pub fn num_operations(&self) -> usize {
        self.operations.len()
    }

    /// Returns how many of the operations in this diff are inserts.
    pub fn num_insert_operations(&self) -> usize {
        self.operations
            .iter()
            .filter(|op| matches!(op, DataOperation::Insert { .. }))
            .count()
    }

    /// Returns how many of the operations in this diff are deletes.
    pub fn num_delete_operations(&self) -> usize {
        self.operations
            .iter()
            .filter(|op| matches!(op, DataOperation::Delete { .. }))
            .count()
    }

    /// Returns an iterator over the values that are to be inserted.
    pub fn values_inserted(&self) -> impl Iterator<Item = &str> {
        self.operations.iter().flat_map(|op| match op {
            DataOperation::Insert { value, .. } => Some(value.as_str()),
            DataOperation::Delete { .. } => None,
        })
    }
}
impl<Id> fmt::Display for LinearStringDiff<Id>
where
    Id: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for op in self.operations.iter() {
            match op {
                DataOperation::Insert {
                    id,
                    pred,
                    succ,
                    value,
                } => {
                    writeln!(f, "@@ {pred} @@ {id}+++{value} @@ {succ} @@")?;
                }
                DataOperation::Delete { start, end } => {
                    let last = end.as_ref().unwrap_or(start);
                    writeln!(f, "@@ {start} @@ --- @@ {last} @@")?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Snafu)]
pub enum DiffError {
    #[snafu(display("The id generator did not produce sufficient ids to complete the diff."))]
    IdsExhausted,
    #[snafu(transparent)]
    Internal { source: InternalError },
}

/// Compute the operations that need to be applied to `base` such that its string output is the
/// same as `changed`.
///
/// This uses a text diffing algorithm internally to produce a small set of text changes and then
/// translate this into equivalent linear data operations using the `id_generator` to produce
/// new node ids as required.
/// (If the insertions are small enough it will try to use a single id only,
/// but larger insertions could require more, so callers need to handle lazily producing additional
/// ids.)
pub fn linear_diff<Id>(
    base: &LinearString<Id>,
    changed: &str,
    id_generator: &mut impl Iterator<Item = Id>,
) -> Result<LinearStringDiff<Id>, DiffError>
where
    Id: Clone + fmt::Debug + PartialEq + 'static,
{
    let mut id_with_index_generator = IdGeneratorWithZeroIndex::new(id_generator);
    let current_text = base.to_string();
    let basic_diff = text_diff::diff(&current_text, changed);
    // TODO: For debugging. Remove later.
    // println!(
    //     "Text Diff:\n{}\n{}",
    //     TextChangePrettyPrint {
    //         from: &current_text,
    //         changes: &basic_diff
    //     },
    //     basic_diff.iter().map(|diff| diff.to_string()).join("\n")
    // );

    // Convert the TextChange to DataOperations over `base`.
    let mut operations: Vec<DataOperation<IdWithIndex<Id>, String>> =
        Vec::with_capacity(basic_diff.len());
    let mut active_insert_op_id: Option<IdWithIndex<Id>> = None;
    for change in basic_diff {
        match change {
            text_diff::TextChange::Insert { at, value } => {
                let node_insert_ids = if base.is_empty() {
                    require!(
                        at == 0,
                        InternalSnafu {
                            context: format!(
                                "When base is empty inserts must be at position 0, but was {at}: {value}"
                            ),
                        }
                        .build()
                        .into()
                    );
                    base.ids_after_head()
                } else {
                    match base.ids_at_pos(at) {
                        // TextChange always outputs the position where the first inserted character
                        // should be after the insertion happened.
                        Some(node_ids) => node_ids.before(),
                        None => {
                            // If it wants to insert at the end, it will return the length of the
                            // string as position (which is consistent with above).
                            ensure!(
                                at == base.len(),
                                InternalSnafu {
                                    context: format!("Insert position {at} did not exist in base."),
                                }
                            );
                            base.ids_before_end()
                        }
                    }
                };
                let mut value_graphemes = GraphemeString::new(value);
                let mut previous_op_end_id: Option<IdWithIndex<Id>> = None;
                while !value_graphemes.is_empty() {
                    // Set up ids for this operation.
                    let op_id = if let Some(id) = active_insert_op_id.take() {
                        id
                    } else {
                        id_with_index_generator.next().context(IdsExhaustedSnafu)?
                    };
                    require!(
                        op_id.addressable_len() > 0,
                        InternalSnafu {
                            context: "There should have been some space left in the current id."
                                .to_owned()
                        }
                        .build()
                        .into()
                    );
                    let current_value = value_graphemes.take(op_id.addressable_len());

                    let ids = if let Some(id) = previous_op_end_id.take() {
                        // Update the predessor to the last id we inserted,
                        // since by the time this operation executes,
                        // that will be present in the target and the next operation should be
                        // deterministically added after it and not accidentally sorted before it,
                        // if it has different id (we don't know the sorting).
                        LinkIds {
                            predecessor: id,
                            ..node_insert_ids.clone()
                        }
                    } else {
                        node_insert_ids.clone()
                    };
                    let current_value_len = current_value.len();

                    // Create operation.
                    let op = ids.insert_operation(op_id.clone(), current_value.unwrap());
                    operations.push(op);

                    // Prepare upcoming ids.
                    if current_value_len == op_id.addressable_len() {
                        previous_op_end_id = Some(op_id.with_max_index());
                        // Just leave active_insert_op_id as None
                        // (which is after the take above anyway),
                        // so that we only pull a new id from the generator,
                        // if we really need another one.
                    } else {
                        let current_len: u16 = current_value_len.try_into().map_err(|e| InternalSnafu {
                            context: format!("Failed to convert value length to u16, even though it was fit into addressable_len before: {e}")
                        }.build())?;
                        let diff_to_last_index =
                            current_len.checked_sub(1).with_context(|| InternalSnafu {
                                context: "current_len was 0 for a non-empty GraphemeString"
                                    .to_owned(),
                            })?;
                        previous_op_end_id = Some(&op_id + diff_to_last_index);
                        active_insert_op_id = Some(op_id + current_len);
                    };
                }
            }
            text_diff::TextChange::Delete { at, len } => {
                let range_end = at + len;
                let ids = base
                    .ids_in_range(at..range_end)
                    .with_context(|| InternalSnafu {
                        context: format!("Delete range [{at}, {range_end}) did not exist in base."),
                    })?;
                operations.extend(ids.delete_operations());
            }
        }
    }

    Ok(LinearStringDiff { operations })
}

#[cfg(test)]
mod tests {
    use crate::text::{
        LinearString,
        linear_diff,
        linear_string::tests::TestIdGenerator,
        text_diff::tests::{SMALL_CHANGE_TEST_GROUPS, TEXT_A, TEXT_B},
    };
    use itertools::Itertools;

    #[test]
    fn test_with_empty_string() {
        let mut id_generator = TestIdGenerator::new();

        let mut linear = LinearString::new(&mut id_generator);

        let empty_diff = linear_diff(&linear, "", &mut id_generator).unwrap();
        assert!(empty_diff.is_empty());
        assert_eq!(empty_diff.num_operations(), 0);
        assert_eq!(empty_diff.num_delete_operations(), 0);
        assert_eq!(empty_diff.num_insert_operations(), 0);
        assert_eq!(empty_diff.values_inserted().count(), 0);
        assert!(empty_diff.new_ids().is_empty());
        assert_eq!(empty_diff.to_string(), "");

        empty_diff.apply_to(&mut linear).unwrap();
        assert_eq!(linear.to_string(), "");

        let single_word_insert = linear_diff(&linear, "hello", &mut id_generator).unwrap();
        assert!(!single_word_insert.is_empty());
        assert_eq!(single_word_insert.num_operations(), 1);
        assert_eq!(single_word_insert.num_delete_operations(), 0);
        assert_eq!(single_word_insert.num_insert_operations(), 1);
        assert_eq!(single_word_insert.values_inserted().count(), 1);
        assert_eq!(single_word_insert.new_ids().len(), 1);
        assert_eq!(
            single_word_insert.to_string(),
            "@@ 0:0 @@ 1:0+++hello @@ 0:1 @@\n"
        );

        single_word_insert.apply_to(&mut linear).unwrap();
        assert_eq!(linear.to_string(), "hello");

        // Try to diff with identical string.
        let empty_diff_again = linear_diff(&linear, "hello", &mut id_generator).unwrap();
        assert!(empty_diff_again.is_empty());

        empty_diff_again.apply_to(&mut linear).unwrap();
        assert_eq!(linear.to_string(), "hello");

        // Diff back to empty string.
        let single_delete_diff = linear_diff(&linear, "", &mut id_generator).unwrap();
        assert!(!single_delete_diff.is_empty());
        assert_eq!(single_delete_diff.num_operations(), 1);
        assert_eq!(single_delete_diff.num_delete_operations(), 1);
        assert_eq!(single_delete_diff.num_insert_operations(), 0);
        assert_eq!(single_delete_diff.values_inserted().count(), 0);
        assert!(single_delete_diff.new_ids().is_empty());
        assert_eq!(single_delete_diff.to_string(), "@@ 1:0 @@ --- @@ 1:4 @@\n");

        single_delete_diff.apply_to(&mut linear).unwrap();
        assert_eq!(linear.to_string(), "");
    }

    #[test]
    fn diff_and_apply_small_changes() {
        // Do all possible transitions within each group.
        for (row, group) in SMALL_CHANGE_TEST_GROUPS.iter().enumerate() {
            for perm in group.iter().enumerate().permutations(2) {
                let (from_index, from) = perm[0];
                let (to_index, to) = perm[1];
                let mut id_generator = TestIdGenerator::new();
                let linear_from = LinearString::with_value(&mut id_generator, (*from).to_owned());
                check_diff_and_apply(
                    &linear_from,
                    to,
                    &format!(
                        "Patching with input:\n    {row}:{from_index}: \"{from}\"\n -> {row}:{to_index}: \"{to}\""
                    ),
                );
            }
        }
    }

    #[test]
    fn diff_and_apply_distant_changes() {
        // Do some changes across groups.
        for perm in SMALL_CHANGE_TEST_GROUPS.iter().enumerate().permutations(2) {
            let (from_row, from_group) = perm[0];
            let (to_row, to_group) = perm[1];
            // I don't feel it's worth testing all possible combinations here. I'll just go from the initial value of one group to the final of another.
            let from_index = 0;
            let to_index = 2;
            let from = from_group[from_index];
            let to = to_group[to_index];
            let mut id_generator = TestIdGenerator::new();
            let linear_from = LinearString::with_value(&mut id_generator, (*from).to_owned());
            check_diff_and_apply(
                &linear_from,
                to,
                &format!(
                    "Patching with input:\n    {from_row}:{from_index}: \"{from}\"\n -> {to_row}:{to_index}: \"{to}\""
                ),
            );
        }
    }

    #[test]
    fn diff_and_apply_larger_changes() {
        let mut id_generator = TestIdGenerator::new();
        let linear_text_a = LinearString::with_value(&mut id_generator, TEXT_A.to_owned());

        check_diff_and_apply(
            &linear_text_a,
            TEXT_B,
            &format!("Patching with large input:\n\"\n{TEXT_A}\n\"\n ->\n\"\n{TEXT_B}\n\""),
        );

        // And reverse.
        let linear_text_b = LinearString::with_value(&mut id_generator, TEXT_B.to_owned());
        check_diff_and_apply(
            &linear_text_b,
            TEXT_A,
            &format!("Patching with large input:\n\"\n{TEXT_B}\n\"\n ->\n\"\n{TEXT_A}\n\""),
        );
    }

    fn check_diff_and_apply(from: &LinearString<u32>, to: &str, error_context: &str) {
        let mut id_generator = TestIdGenerator::without_ids(from.iter_ids().cloned());

        // println!("Producing diff between\n '{}'\n and\n '{}'", from, to);
        let diff = linear_diff(from, to, &mut id_generator).expect(error_context);
        assert!(
            !diff.is_empty(),
            "Diff should not be empty.\n  Context: {error_context}"
        );
        // println!("Applying diff:\n{}", diff);
        let mut target = from.clone();
        diff.apply_to(&mut target).expect(error_context);
        from.check_integrity();
        assert_eq!(target.to_string(), to, "{error_context}");
    }

    #[test]
    fn test_sequence_of_changes() {
        let mut id_generator = TestIdGenerator::new();
        let mut linear_from = LinearString::new(&mut id_generator);

        for to in SMALL_CHANGE_TEST_GROUPS.iter().flatten() {
            // let from = linear_from.to_string();
            //println!("Producing diff between\n '{}'\n and\n '{}'", from, to);
            let diff = linear_diff(&linear_from, to, &mut id_generator).unwrap();
            //println!("Applying diff:\n{}\n to {}", diff, linear_from.debug_fmt());
            diff.apply_to(&mut linear_from).unwrap();
            linear_from.check_integrity();
            assert_eq!(linear_from.to_string(), *to);
        }
    }
}
