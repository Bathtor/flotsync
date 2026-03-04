use crate::{
    InternalSnafu,
    linear_data::{
        Composite,
        DataOperation,
        IdGeneratorWithZeroIndex,
        IdWithIndex,
        LinearData,
        VecCoalescedLinearDataIter,
    },
};
use flotsync_utils::{debugging::DebugFormatting, require};
use snafu::prelude::*;
use std::{collections::BTreeSet, fmt, hash::Hash, ops::RangeBounds};
use unicode_segmentation::{Graphemes, UnicodeSegmentation};

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
#[derive(Clone, Debug, PartialEq)]
pub struct LinearStringDiff<Id> {
    operations: Vec<DataOperation<IdWithIndex<Id>, String>>,
}
impl<Id> LinearStringDiff<Id>
where
    Id: Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
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
            // #[cfg(test)]
            // target.validate_integrity().unwrap();
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

    pub(crate) fn into_operations(self) -> Vec<DataOperation<IdWithIndex<Id>, String>> {
        self.operations
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
    #[snafu(display("A single insert would require indices > u32::MAX."))]
    IndexExhausted,
    #[snafu(transparent)]
    Internal { source: InternalError },
}

/// Compute the operations that need to be applied to `base` such that its string output is the
/// same as `changed`.
///
/// This uses a text diffing algorithm internally to produce a small set of text changes and then
/// translate this into equivalent linear data operations using the `id_generator` to produce
/// new node ids as required.
/// Each inserted diff fragment uses a single major id and fails if its addressed indices would
/// exceed `u32::MAX`.
pub fn linear_diff<Id>(
    base: &LinearString<Id>,
    changed: &str,
    id_generator: &mut impl Iterator<Item = Id>,
) -> Result<LinearStringDiff<Id>, DiffError>
where
    Id: Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    let mut id_with_index_generator = IdGeneratorWithZeroIndex::new(id_generator);
    let current_text = base.to_string();
    let basic_diff = text_diff::diff(&current_text, changed);

    // Convert the TextChange to DataOperations over `base`.
    let mut operations: Vec<DataOperation<IdWithIndex<Id>, String>> =
        Vec::with_capacity(basic_diff.len());
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
                let value_graphemes = GraphemeString::new(value);
                let op_id = id_with_index_generator.next().context(IdsExhaustedSnafu)?;
                require!(
                    op_id.can_address(value_graphemes.len()),
                    IndexExhaustedSnafu.build()
                );
                operations.push(node_insert_ids.insert_operation(op_id, value_graphemes.unwrap()));
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
    use crate::{
        linear_data::tests::TestIdGenerator,
        text::{
            LinearString,
            LinearStringDiff,
            linear_diff,
            text_diff::tests::{SMALL_CHANGE_TEST_GROUPS, TEXT_A, TEXT_B},
        },
    };
    use flotsync_utils::{debugging::DebugFormatting, option_when, svec16, testing::SVec16};
    use itertools::Itertools;

    #[test]
    fn test_with_empty_string() {
        let mut id_generator = TestIdGenerator::new();

        let mut linear = LinearString::new(id_generator.next().unwrap());

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
                let linear_from =
                    LinearString::with_value((*from).to_owned(), id_generator.next().unwrap());
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
            let linear_from =
                LinearString::with_value((*from).to_owned(), id_generator.next().unwrap());
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
        let linear_text_a =
            LinearString::with_value(TEXT_A.to_owned(), id_generator.next().unwrap());

        check_diff_and_apply(
            &linear_text_a,
            TEXT_B,
            &format!("Patching with large input:\n\"\n{TEXT_A}\n\"\n ->\n\"\n{TEXT_B}\n\""),
        );

        // And reverse.
        let linear_text_b =
            LinearString::with_value(TEXT_B.to_owned(), id_generator.next().unwrap());
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
        from.validate_integrity().unwrap();
        assert_eq!(target.to_string(), to, "{error_context}");
    }

    #[test]
    fn test_sequence_of_changes() {
        let mut id_generator = TestIdGenerator::new();
        let mut linear_from = LinearString::new(id_generator.next().unwrap());

        for to in SMALL_CHANGE_TEST_GROUPS.iter().flatten() {
            // let from = linear_from.to_string();
            //println!("Producing diff between\n '{}'\n and\n '{}'", from, to);
            let diff = linear_diff(&linear_from, to, &mut id_generator).unwrap();
            //println!("Applying diff:\n{}\n to {}", diff, linear_from.debug_fmt());
            diff.apply_to(&mut linear_from).unwrap();
            linear_from.validate_integrity().unwrap();
            assert_eq!(linear_from.to_string(), *to);
        }
    }

    #[test]
    fn test_single_step_convergence() {
        // For each group, check that no matter in which order we apply the two diffs,
        // they produce the same result (doesn't matter what it is, as long as its the same).
        for group in SMALL_CHANGE_TEST_GROUPS.iter() {
            // println!("### \n### Checking Group: {group:?}\n###");
            let mut id_generator = TestIdGenerator::new();
            let base = LinearString::with_value(group[0].to_owned(), id_generator.next().unwrap());

            let diff_to_first = linear_diff(&base, group[1], &mut id_generator).unwrap();
            assert!(!diff_to_first.is_empty());
            let diff_to_second = linear_diff(&base, group[2], &mut id_generator).unwrap();
            assert!(!diff_to_second.is_empty());

            let first_then_second = {
                let mut linear = base.clone();
                diff_to_first.clone().apply_to(&mut linear).unwrap();
                linear.validate_integrity().unwrap();
                assert_eq!(linear.to_string(), group[1]);
                diff_to_second.clone().apply_to(&mut linear).unwrap();
                linear.validate_integrity().unwrap();
                linear
            };

            // Second then first.
            let second_then_first = {
                let mut linear = base.clone();
                diff_to_second.apply_to(&mut linear).unwrap();
                linear.validate_integrity().unwrap();
                assert_eq!(linear.to_string(), group[2]);
                diff_to_first.apply_to(&mut linear).unwrap();
                linear.validate_integrity().unwrap();
                linear
            };
            assert_eq!(first_then_second.to_string(), second_then_first.to_string());
            // They should also match structurally.
            assert_eq!(
                first_then_second,
                second_then_first,
                "first_then_second:\n{}\nsecond_then_first:\n{}",
                first_then_second.debug_fmt(),
                second_then_first.debug_fmt()
            );
        }
    }

    #[test]
    fn test_multi_step_convergence() {
        // Treat the groups with 3 entries each as 3 indepdent writers.
        // Have each of them make changes to its own string, recording the diffs in order.
        // At the end, apply the other 2 diffs to each result in both orders.
        // Ensure all results are the identical.
        let mut id_generator = TestIdGenerator::new();
        // Begin and end nodes need to have the same ids, of course.
        let shared_base = LinearString::new(id_generator.next().unwrap());
        struct Writer {
            id: usize,
            linear: LinearString<u32>,
            ops: Vec<LinearStringDiff<u32>>,
        }
        let mut writers = Vec::from_fn(3, |id| Writer {
            id,
            linear: shared_base.clone(),
            ops: Vec::with_capacity(SMALL_CHANGE_TEST_GROUPS.len()),
        });
        for group in SMALL_CHANGE_TEST_GROUPS.iter() {
            for (writer_index, writer) in writers.iter_mut().enumerate() {
                let op =
                    linear_diff(&writer.linear, group[writer_index], &mut id_generator).unwrap();
                writer.ops.push(op.clone());
                op.apply_to(&mut writer.linear).unwrap();
                writer.linear.validate_integrity().unwrap();
                assert_eq!(writer.linear.to_string(), group[writer_index]);
            }
        }
        // let mut permutation_results: Vec<LinearString<u32>> = Vec::with_capacity(6);
        let mut previous_result: Option<LinearString<u32>> = None;
        for perm in writers.iter().permutations(3) {
            let permutation_str = perm.iter().map(|w| w.id).join(", ");
            //println!("###\n### Checking writer order: {}\n###", permutation_str);
            let mut linear = perm[0].linear.clone();
            //println!("### Base is '{}':\n {}", linear, linear.debug_fmt());
            for writer in &perm[1..] {
                for op in writer.ops.iter() {
                    //println!("### Applying op\n{}\nto\n {}", op, linear.debug_fmt());
                    op.clone().apply_to(&mut linear).unwrap();
                    linear.validate_integrity().unwrap();
                    //println!("### Got '{}':\n {}", linear, linear.debug_fmt());
                }
            }
            if let Some(ref last_result) = previous_result {
                assert_eq!(
                    last_result.to_string(),
                    linear.to_string(),
                    "Result strings did not match for permutation: {}",
                    permutation_str
                );
                assert_eq!(
                    last_result, &linear,
                    "Results did not match for permutation: {}",
                    permutation_str
                );
            }
            // permutation_results.push(linear);
            previous_result = Some(linear);
        }
        // println!(
        //     "Permutation Results:\n{}",
        //     permutation_results.iter().join("\n")
        // );
        // for ((i1, r1), (i2, r2)) in permutation_results.iter().enumerate().tuple_windows() {
        //     assert_eq!(
        //         r1.to_string(),
        //         r2.to_string(),
        //         "Result strings did not match for permutations: {i1} and {i2}"
        //     );
        //     assert_eq!(
        //         r1, r2,
        //         "Results did not match for permutations: {i1} and {i2}",
        //     );
        // }
    }

    #[test]
    fn test_multi_step_repro() {
        let mut id_generator = TestIdGenerator::new();
        // Begin and end nodes need to have the same ids, of course.
        let shared_base = LinearString::new(id_generator.next().unwrap());

        // Writer A: "" -> "a" -> "Za" (second step references the id created in the first step).
        let mut a = shared_base.clone();
        let a1 = linear_diff(&a, "a", &mut id_generator).unwrap();
        //println!("a1:{}", a1);
        a1.clone().apply_to(&mut a).unwrap();
        a.validate_integrity().unwrap();
        let a2 = linear_diff(&a, "Za", &mut id_generator).unwrap();
        //println!("a2:{}", a2);
        a2.clone().apply_to(&mut a).unwrap();
        a.validate_integrity().unwrap();

        // Writer B: "" -> "ab" -> "aXb" (forces an insertion inside a multi-grapheme node).
        let mut b = shared_base.clone();
        let b1 = linear_diff(&b, "ab", &mut id_generator).unwrap();
        //println!("b1:{}", b1);
        b1.clone().apply_to(&mut b).unwrap();
        b.validate_integrity().unwrap();
        let b2 = linear_diff(&b, "aXb", &mut id_generator).unwrap();
        //println!("b2:{}", b2);
        b2.clone().apply_to(&mut b).unwrap();
        b.validate_integrity().unwrap();

        // Apply A then B (respecting each writer's internal ordering).
        //println!("### Applying a1, a2, b1, b2");
        let mut l1 = shared_base.clone();
        a1.clone().apply_to(&mut l1).unwrap();
        l1.validate_integrity().unwrap();
        a2.clone().apply_to(&mut l1).unwrap();
        l1.validate_integrity().unwrap();
        b1.clone().apply_to(&mut l1).unwrap();
        l1.validate_integrity().unwrap();
        b2.clone().apply_to(&mut l1).unwrap();
        l1.validate_integrity().unwrap();

        // Apply B then A (still respects each writer's internal ordering).
        //println!("### Applying b1, b2, a1, a2");
        let mut l2 = shared_base.clone();
        b1.apply_to(&mut l2).unwrap();
        l2.validate_integrity().unwrap();
        b2.apply_to(&mut l2).unwrap();
        l2.validate_integrity().unwrap();
        a1.apply_to(&mut l2).unwrap();
        l2.validate_integrity().unwrap();
        a2.apply_to(&mut l2).unwrap();
        l2.validate_integrity().unwrap();

        assert_eq!(
            l1.to_string(),
            l2.to_string(),
            "Repro: result strings diverged\n\nOrder a1, a2, b1, b2:\n{}\n\nOrder b1, b2, a1, a2:\n{}\n",
            l1.debug_fmt(),
            l2.debug_fmt()
        );
        assert_eq!(
            l1,
            l2,
            "Repro: structures diverged\n\nOrder a1, a2, b1, b2:\n{}\n\nOrder b1, b2, a1, a2:\n{}\n",
            l1.debug_fmt(),
            l2.debug_fmt()
        );
    }

    #[derive(Debug, Clone, Copy)]
    struct WriterSync {
        writer: usize,
        sync_from: SVec16<usize>,
    }

    struct SyncScenario {
        sync_points: SVec16<(usize, WriterSync)>,
    }
    impl SyncScenario {
        pub fn actions_at(&self, index: usize) -> SVec16<WriterSync> {
            self.sync_points
                .iter()
                .filter_map(|(i, a)| option_when!(i == index, a))
                .collect()
        }
    }

    // Micro-manage formatting here a bit to keep the scenarios tighter.
    #[rustfmt::skip]
    static SYNC_SCENARIOS: [SyncScenario; 4] = {
        // Make the entries a bit shorter.
        type Sync = WriterSync;
        [SyncScenario {
            sync_points: svec16![
                (3, Sync { writer: 0, sync_from: svec16![1] }),
                (5, Sync { writer: 0, sync_from: svec16![2] }),
            ],
        },
        // Trickier scenarios.
        SyncScenario {
            sync_points: svec16![
                (3, Sync { writer: 0, sync_from: svec16![1] }),
                (5, Sync { writer: 2, sync_from: svec16![1] }),
                (8, Sync { writer: 1, sync_from: svec16![0, 2]}),
            ],
        },
        SyncScenario {
            sync_points: svec16![
                (3, Sync { writer: 1, sync_from: svec16![0, 2] }),
                (5, Sync { writer: 2, sync_from: svec16![0, 1] }),
                (8, Sync { writer: 0, sync_from: svec16![1, 2]}),
            ],
        },
        SyncScenario {
            sync_points: svec16![
                (4, Sync { writer: 1, sync_from: svec16![0, 2] }),
                (5, Sync { writer: 2, sync_from: svec16![0, 1] }),
                (6, Sync { writer: 0, sync_from: svec16![1, 2]}),
            ],
        }]
    };

    #[test]
    fn test_multi_step_convergence_with_sync() {
        // Treat the groups with 3 entries each as 3 indepdent writers.
        // Have each of them make changes to its own string, recording the diffs in order.
        // At predefined sync points, apply the diffs, then continue building new diffs from there.
        // In the end (final sync), all writers must have converged to the same result.
        let mut id_generator = TestIdGenerator::new();
        // Begin and end nodes need to have the same ids, of course.
        let shared_base = LinearString::new(id_generator.next().unwrap());
        struct Writer {
            id: usize,
            linear: LinearString<u32>,
            ops: Vec<LinearStringDiff<u32>>,
            next_sync_for: [usize; 3],
        }
        let mut writers: [Writer; 3] = std::array::from_fn(|id| Writer {
            id,
            linear: shared_base.clone(),
            ops: Vec::with_capacity(SMALL_CHANGE_TEST_GROUPS.len()),
            next_sync_for: [0; 3],
        });

        fn apply_diffs(
            diffs: &[LinearStringDiff<u32>],
            linear: &mut LinearString<u32>,
        ) -> Result<(), ()> {
            for op in diffs.iter() {
                // println!("##### Applying op\n{}\nto\n {}", op, linear.debug_fmt());
                op.clone().apply_to(linear).map_err(|_| ())?;
                linear.validate_integrity().unwrap();
                // println!("##### Got '{}':\n {}", linear, linear.debug_fmt());
            }
            Ok(())
        }

        'scenario_loop: for scenario in SYNC_SCENARIOS.iter() {
            //println!("##########\n### Scenario #{scenario_index} ###\n#########");
            for (group_index, group) in SMALL_CHANGE_TEST_GROUPS.iter().enumerate() {
                //println!("### Starting write #{group_index} ###");
                let actions = scenario.actions_at(group_index);
                for writer_index in 0..writers.len() {
                    //println!("#### Handling writer #{writer_index}");
                    if let Some(sync) = actions.iter().find(|s| s.writer == writer_index) {
                        for other_writer_index in sync.sync_from.iter() {
                            assert_ne!(writer_index, other_writer_index);
                            let next_sync_index =
                                writers[writer_index].next_sync_for[other_writer_index];

                            // println!(
                            //     "Syncing state from writer #{other_writer_index} starting at {next_sync_index}"
                            // );
                            let [writer, other_writer] = &mut writers
                                .get_disjoint_mut([writer_index, other_writer_index])
                                .unwrap();
                            let res = apply_diffs(
                                &other_writer.ops[next_sync_index..],
                                &mut writer.linear,
                            );
                            if res.is_err() {
                                // This can happen due to missing dependencies.
                                // println!(
                                //     "###\n### Skipping the rest of the scenario, because some operation was not possible.###\n###"
                                // );
                                continue 'scenario_loop;
                            }
                            writers[writer_index].next_sync_for[other_writer_index] =
                                other_writer.ops.len();
                        }
                    }
                    let writer = &mut writers[writer_index];
                    let op = linear_diff(&writer.linear, group[writer_index], &mut id_generator)
                        .unwrap();
                    // println!("Created diff:\n{op}");
                    writer.ops.push(op.clone());
                    op.apply_to(&mut writer.linear).unwrap();
                    writer.linear.validate_integrity().unwrap();
                    assert_eq!(writer.linear.to_string(), group[writer_index]);
                }
                // println!("### Finished write #{group_index} ###");
            }
            // println!("###\n### Beginning permutation replay ###\n###");
            // for (writer_index, writer) in writers.iter().enumerate() {
            //     println!(
            //         "Writer {} has synced {:?} with state: {}",
            //         writer_index, writer.next_sync_for, writer.linear
            //     );
            // }
            let mut previous_result: Option<LinearString<u32>> = None;
            'permutation_loop: for perm in writers.iter().enumerate().permutations(3) {
                let permutation_str = perm.iter().map(|(_, w)| w.id).join(", ");
                //println!("###\n### Checking writer order: {}\n###", permutation_str);
                let (_, base_writer) = perm[0];
                let mut linear = base_writer.linear.clone();
                // println!(
                //     "### Base is '{}':\n {}\nSync: {:?}",
                //     linear,
                //     linear.debug_fmt(),
                //     base_writer.next_sync_for
                // );
                for (writer_index, writer) in &perm[1..] {
                    let next_sync_for_writer = base_writer.next_sync_for[*writer_index];

                    // println!(
                    //     "#### Applying diffs for writer {writer_index} starting at {next_sync_for_writer}"
                    // );
                    let res = apply_diffs(&writer.ops[next_sync_for_writer..], &mut linear);
                    if res.is_err() {
                        // This can happen due to missing dependencies.
                        // println!(
                        //     "###\n### Skipping the rest of the permutation, because some operation was not possible.\n###"
                        // );
                        continue 'permutation_loop;
                    }
                }
                if let Some(ref last_result) = previous_result {
                    assert_eq!(
                        last_result.to_string(),
                        linear.to_string(),
                        "Result strings did not match for permutation: {}",
                        permutation_str
                    );
                    // They might structurally differ in delete splits.
                    // assert_eq!(
                    //     last_result,
                    //     &linear,
                    //     "Results did not match for permutation: {}\n  {}\n  {}",
                    //     permutation_str,
                    //     last_result.debug_fmt(),
                    //     linear.debug_fmt()
                    // );
                }
                // permutation_results.push(linear);
                previous_result = Some(linear);
            }
            // println!("##########\n### Completed Scenario #{scenario_index} ###\n#########");
        }
    }
}
