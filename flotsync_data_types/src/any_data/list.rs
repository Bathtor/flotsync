use crate::linear_data::{
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
use crate::snapshot::{SnapshotNode, SnapshotReadError, SnapshotSink};
use std::{fmt, hash::Hash, ops::RangeBounds};

#[derive(Clone, Debug, PartialEq)]
struct ListChunk<T> {
    values: Vec<T>,
}
impl<T> ListChunk<T> {
    fn new(values: Vec<T>) -> Self {
        Self { values }
    }

    fn unwrap(self) -> Vec<T> {
        self.values
    }
}
impl<T> Composite for ListChunk<T> {
    type Element = T;
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        T: 'a;

    fn get(&self, index: usize) -> Option<&Self::Element> {
        self.values.get(index)
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn split_at(mut self, index: usize) -> (Self, Self) {
        assert!(index < self.values.len());
        let rest = self.values.split_off(index);
        (self, Self { values: rest })
    }

    fn concat(mut self, mut other: Self) -> Self {
        self.values.append(&mut other.values);
        self
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.values.iter()
    }
}

/// A convergent linear list CRDT backed by [[VecCoalescedLinearData]].
///
/// `LinearList` models an ordered sequence of values `T`. Inserts can add one item or a chunk
/// of items in one operation. Concurrent inserts at the same position are resolved
/// deterministically by id ordering and conflict-set rules in the underlying linear CRDT.
///
/// The public API intentionally keeps collection explicit:
/// callers read values via iterators and choose what collection type to build at the use site.
///
/// # Example
///
/// ```rust
/// use flotsync_data_types::any_data::list::LinearList;
///
/// let mut id_generator = 0u32..;
/// let list = LinearList::with_values(&mut id_generator, [10, 20, 30]);
///
/// let values: Vec<i32> = list.iter().copied().collect();
/// assert_eq!(values, vec![10, 20, 30]);
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct LinearList<Id, T> {
    data: VecCoalescedLinearData<Id, ListChunk<T>>,
}
impl<Id, T> LinearList<Id, T>
where
    Id: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    T: fmt::Debug + 'static,
{
    /// Create an empty list.
    pub fn new<I>(id_generator: &mut I) -> Self
    where
        I: Iterator<Item = Id>,
    {
        let data = VecCoalescedLinearData::new(id_generator);
        Self { data }
    }

    /// Create a list initialized with `initial_values`.
    ///
    /// If `initial_values` is empty this is equivalent to [[LinearList::new]].
    ///
    /// # Example
    ///
    /// ```rust
    /// use flotsync_data_types::any_data::list::LinearList;
    ///
    /// let mut id_generator = 0u32..;
    /// let list = LinearList::with_values(&mut id_generator, ["a", "b", "c"]);
    ///
    /// let values: Vec<&str> = list.iter().copied().collect();
    /// assert_eq!(values, vec!["a", "b", "c"]);
    /// ```
    pub fn with_values<I, Values>(id_generator: &mut I, initial_values: Values) -> Self
    where
        I: Iterator<Item = Id>,
        Values: IntoIterator<Item = T>,
    {
        let values = initial_values.into_iter().collect();
        let data = VecCoalescedLinearData::with_value(id_generator, ListChunk::new(values));
        Self { data }
    }

    /// Number of visible elements in the list.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the list contains no visible elements.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Iterate over visible values in list order.
    pub fn iter(&self) -> LinearListIter<'_, Id, T> {
        LinearListIter {
            underlying: self.data.iter_values(),
        }
    }

    /// Visit a stable, ordered snapshot stream of the current in-memory state.
    pub fn visit_snapshot<S>(&self, sink: &mut S) -> Result<(), S::Error>
    where
        S: SnapshotSink<IdWithIndex<Id>, [T]>,
    {
        self.data.visit_snapshot(sink, |value| value.values.as_slice())
    }

    pub fn from_snapshot_nodes<E, I>(nodes: I) -> Result<Self, SnapshotReadError<E>>
    where
        I: IntoIterator<Item = Result<SnapshotNode<IdWithIndex<Id>, Vec<T>>, E>>,
    {
        let mapped = nodes.into_iter().map(|entry| {
            entry.map(|node| SnapshotNode {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value: node.value.map(ListChunk::new),
            })
        });
        let base = VecLinearData::from_snapshot_nodes(mapped)?;
        let data = VecCoalescedLinearData::from_base_snapshot(base);
        Ok(Self { data })
    }

    /// Append one chunk of values at the end.
    ///
    /// Empty chunks are ignored.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut list = LinearList::new(&mut id_generator);
    /// list.append(next_id_with_index(), [1, 2, 3]);
    /// ```
    pub fn append<Values>(&mut self, id: IdWithIndex<Id>, values: Values)
    where
        Values: IntoIterator<Item = T>,
    {
        let values: Vec<T> = values.into_iter().collect();
        if values.is_empty() {
            return;
        }
        assert!(
            id.can_address(values.len()),
            "The id='{id:?}' cannot address all values."
        );
        self.data.append(id, ListChunk::new(values));
    }

    /// Convenience wrapper for appending a single item.
    pub fn append_item(&mut self, id: IdWithIndex<Id>, value: T) {
        self.append(id, [value]);
    }

    /// Prepend one chunk of values at the front.
    ///
    /// Empty chunks are ignored.
    pub fn prepend<Values>(&mut self, id: IdWithIndex<Id>, values: Values)
    where
        Values: IntoIterator<Item = T>,
    {
        let values: Vec<T> = values.into_iter().collect();
        if values.is_empty() {
            return;
        }
        assert!(
            id.can_address(values.len()),
            "The id='{id:?}' cannot address all values."
        );
        self.data.prepend(id, ListChunk::new(values));
    }

    /// Convenience wrapper for prepending a single item.
    pub fn prepend_item(&mut self, id: IdWithIndex<Id>, value: T) {
        self.prepend(id, [value]);
    }

    /// Insert a chunk at `position`.
    ///
    /// Inserting at `self.len()` is append-like.
    /// Returns the original chunk if `position` is out of bounds.
    pub fn insert_at<Values>(
        &mut self,
        position: usize,
        id: IdWithIndex<Id>,
        values: Values,
    ) -> Result<(), Vec<T>>
    where
        Values: IntoIterator<Item = T>,
    {
        let values: Vec<T> = values.into_iter().collect();
        if values.is_empty() {
            return Ok(());
        }
        assert!(
            id.can_address(values.len()),
            "The id='{id:?}' cannot address all values."
        );

        let ids = if position == self.len() {
            self.data.ids_before_end()
        } else if let Some(node_ids) = self.data.ids_at_pos(position) {
            node_ids.before()
        } else {
            return Err(values);
        };

        ids.insert(&mut self.data, id, ListChunk::new(values))
            .map_err(ListChunk::unwrap)
    }

    /// Convenience wrapper for inserting a single item at `position`.
    pub fn insert_item_at(
        &mut self,
        position: usize,
        id: IdWithIndex<Id>,
        value: T,
    ) -> Result<(), T> {
        self.insert_at(position, id, [value])
            .map_err(|values| values.into_iter().next().unwrap())
    }

    /// Delete the visible element at `position`.
    ///
    /// Returns `None` if the position is out of bounds.
    /// Repeated deletes of the same element are idempotent at the CRDT level.
    pub fn delete_at(&mut self, position: usize) -> Option<&T> {
        let node_ids = self.data.ids_at_pos(position)?;
        self.data.delete(&node_ids.current)
    }

    /// Return ids covering a contiguous visible range.
    ///
    /// The returned wrapper can be applied directly via [[NodeIdRangeList::delete]]
    /// or converted into replayable operations.
    pub fn ids_in_range<R>(&self, range: R) -> Option<NodeIdRangeList<Id>>
    where
        R: RangeBounds<usize>,
    {
        self.data.ids_in_range(range).map(NodeIdRangeList)
    }

    /// Resolve the concrete ids at the given visible position.
    pub fn ids_at_pos(&self, position: usize) -> Option<NodeIds<IdWithIndex<Id>>> {
        self.data.ids_at_pos(position)
    }

    /// Returns an iterator over all ids that are associated with some node in the underlying
    /// data structure.
    ///
    /// Note that there will always be duplicate ids.
    /// The head and end nodes share the same id, and also when a coalesced node was split
    /// later with another id being inserted within.
    pub fn iter_ids(&self) -> impl Iterator<Item = &Id> {
        self.data.iter_ids().map(|id| &id.id)
    }

    /// Build an append operation for replication.
    ///
    /// Returns `None` for empty chunks.
    pub fn append_operation<Values>(
        &self,
        id: IdWithIndex<Id>,
        values: Values,
    ) -> Option<ListOperation<Id, T>>
    where
        Values: IntoIterator<Item = T>,
    {
        let values: Vec<T> = values.into_iter().collect();
        if values.is_empty() {
            return None;
        }
        assert!(
            id.can_address(values.len()),
            "The id='{id:?}' cannot address all values."
        );

        let ids = self.data.ids_before_end();
        Some(ListOperation {
            op: ids.insert_operation(id, values),
        })
    }

    /// Build a prepend operation for replication.
    ///
    /// Returns `None` for empty chunks.
    pub fn prepend_operation<Values>(
        &self,
        id: IdWithIndex<Id>,
        values: Values,
    ) -> Option<ListOperation<Id, T>>
    where
        Values: IntoIterator<Item = T>,
    {
        let values: Vec<T> = values.into_iter().collect();
        if values.is_empty() {
            return None;
        }
        assert!(
            id.can_address(values.len()),
            "The id='{id:?}' cannot address all values."
        );

        let ids = self.data.ids_after_head();
        Some(ListOperation {
            op: ids.insert_operation(id, values),
        })
    }

    /// Build an insert operation at `position` for replication.
    ///
    /// Inserting at `self.len()` is append-like.
    /// Returns `Ok(None)` for empty chunks and `Err(values)` for out-of-bounds positions.
    pub fn insert_operation_at<Values>(
        &self,
        position: usize,
        id: IdWithIndex<Id>,
        values: Values,
    ) -> Result<Option<ListOperation<Id, T>>, Vec<T>>
    where
        Values: IntoIterator<Item = T>,
    {
        let values: Vec<T> = values.into_iter().collect();
        if values.is_empty() {
            return Ok(None);
        }
        assert!(
            id.can_address(values.len()),
            "The id='{id:?}' cannot address all values."
        );

        let ids: LinkIds<IdWithIndex<Id>> = if position == self.len() {
            self.data.ids_before_end()
        } else if let Some(node_ids) = self.data.ids_at_pos(position) {
            node_ids.before()
        } else {
            return Err(values);
        };

        Ok(Some(ListOperation {
            op: ids.insert_operation(id, values),
        }))
    }

    /// Build a delete operation for the value at `position`.
    ///
    /// Returns `None` if the position is out of bounds.
    pub fn delete_operation_at(&self, position: usize) -> Option<ListOperation<Id, T>> {
        let node_ids = self.data.ids_at_pos(position)?;
        Some(ListOperation {
            op: DataOperation::Delete {
                start: node_ids.current,
                end: None,
            },
        })
    }

    /// Apply a replicated operation received from another replica.
    ///
    /// The original operation is returned unchanged on failure.
    pub fn apply_operation(
        &mut self,
        operation: ListOperation<Id, T>,
    ) -> Result<(), ListOperation<Id, T>> {
        let op = operation.op.map_value(ListChunk::new);
        self.data.apply_operation(op).map_err(|op| ListOperation {
            op: op.map_value(ListChunk::unwrap),
        })
    }
}

impl<Id, T> LinearData<Vec<T>, T> for LinearList<Id, T>
where
    Id: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    T: fmt::Debug + 'static,
{
    type Id = IdWithIndex<Id>;

    type Iter<'a>
        = LinearListIter<'a, Id, T>
    where
        Self: 'a;

    fn ids_after_head(&self) -> LinkIds<Self::Id> {
        self.data.ids_after_head()
    }

    fn ids_before_end(&self) -> LinkIds<Self::Id> {
        self.data.ids_before_end()
    }

    fn ids_at_pos(&self, position: usize) -> Option<NodeIds<Self::Id>> {
        self.data.ids_at_pos(position)
    }

    fn insert(
        &mut self,
        id: Self::Id,
        pred: Self::Id,
        succ: Self::Id,
        value: Vec<T>,
    ) -> Result<(), Vec<T>> {
        let values = ListChunk::new(value);
        self.data
            .insert(id, pred, succ, values)
            .map_err(ListChunk::unwrap)
    }

    fn delete<'a>(&'a mut self, id: &Self::Id) -> Option<&'a T> {
        self.data.delete(id)
    }

    fn iter_values(&self) -> Self::Iter<'_> {
        self.iter()
    }

    fn iter_ids(&self) -> impl Iterator<Item = &Self::Id> {
        self.data.iter_ids()
    }

    fn apply_operation(
        &mut self,
        operation: DataOperation<Self::Id, Vec<T>>,
    ) -> Result<(), DataOperation<Self::Id, Vec<T>>> {
        let op = operation.map_value(ListChunk::new);
        self.data
            .apply_operation(op)
            .map_err(|op| op.map_value(ListChunk::unwrap))
    }
}

/// A replication operation for [[LinearList]].
///
/// This wraps a low-level [[DataOperation]] and keeps list payloads as `Vec<T>`.
#[derive(Clone, Debug, PartialEq)]
pub struct ListOperation<Id, T> {
    op: DataOperation<IdWithIndex<Id>, Vec<T>>,
}

/// Convenience wrapper around [[NodeIdRange]] when using it with [[LinearList]].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeIdRangeList<Id>(NodeIdRange<Id>);
impl<Id> NodeIdRangeList<Id>
where
    Id: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    /// Tries to delete all the nodes contained in the range.
    ///
    /// Returns the first failing range if unsuccessful.
    /// In this case the previous deletes will have been applied.
    pub fn delete<'a, T>(
        &'a self,
        data: &mut LinearList<Id, T>,
    ) -> Result<(), &'a IdWithIndexRange<Id>>
    where
        T: fmt::Debug + 'static,
    {
        self.0.delete(&mut data.data)
    }

    /// Convert this range into replayable delete operations.
    pub fn delete_operations<T>(self) -> impl Iterator<Item = ListOperation<Id, T>> {
        self.0.delete_operations().map(|op| ListOperation { op })
    }
}

pub struct LinearListIter<'a, Id, T> {
    underlying: VecCoalescedLinearDataIter<'a, IdWithIndex<Id>, ListChunk<T>>,
}
impl<'a, Id, T> Iterator for LinearListIter<'a, Id, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::linear_data::tests::TestIdGenerator;
    use itertools::Itertools;

    type Id = u32;
    type Value = i32;

    fn new_list(initial: impl IntoIterator<Item = Value>) -> LinearList<Id, Value> {
        let mut id_generator = TestIdGenerator::new();
        LinearList::with_values(&mut id_generator, initial)
    }

    #[test]
    fn with_values_roundtrip() {
        let list = new_list([1, 2, 3, 4]);
        assert_eq!(list.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
        assert_eq!(list.len(), 4);
        assert!(!list.is_empty());
    }

    #[test]
    fn supports_chunk_and_single_item_operations() {
        let mut id_generator = TestIdGenerator::new();
        let mut list = LinearList::new(&mut id_generator);

        list.append(id_generator.next_with_zero_index().unwrap(), [2, 3]);
        list.prepend(id_generator.next_with_zero_index().unwrap(), [0, 1]);
        assert_eq!(list.iter().copied().collect::<Vec<_>>(), vec![0, 1, 2, 3]);

        list.insert_at(2, id_generator.next_with_zero_index().unwrap(), [7, 8])
            .unwrap();
        assert_eq!(
            list.iter().copied().collect::<Vec<_>>(),
            vec![0, 1, 7, 8, 2, 3]
        );

        list.prepend_item(id_generator.next_with_zero_index().unwrap(), -1);
        list.append_item(id_generator.next_with_zero_index().unwrap(), 9);
        list.insert_item_at(3, id_generator.next_with_zero_index().unwrap(), 6)
            .unwrap();
        assert_eq!(
            list.iter().copied().collect::<Vec<_>>(),
            vec![-1, 0, 1, 6, 7, 8, 2, 3, 9]
        );

        let deleted_middle = list.delete_at(4).copied();
        assert_eq!(deleted_middle, Some(7));

        let deleted_end = list.delete_at(list.len() - 1).copied();
        assert_eq!(deleted_end, Some(9));
        assert_eq!(
            list.iter().copied().collect::<Vec<_>>(),
            vec![-1, 0, 1, 6, 8, 2, 3]
        );
    }

    #[test]
    fn insert_out_of_bounds_returns_values() {
        let mut id_generator = TestIdGenerator::new();
        let mut list = new_list([1, 2, 3]);

        let res = list.insert_at(5, id_generator.next_with_zero_index().unwrap(), [8, 9]);
        assert_eq!(res, Err(vec![8, 9]));
        assert_eq!(list.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn operation_roundtrip_converges_between_replicas() {
        let mut id_generator = TestIdGenerator::new();
        let base = LinearList::with_values(&mut id_generator, [1, 2, 3]);
        let mut a = base.clone();
        let mut b = base;

        let mut op_id_generator = TestIdGenerator::without_ids(a.iter_ids().cloned());
        let op = a
            .insert_operation_at(1, op_id_generator.next_with_zero_index().unwrap(), [9, 8])
            .unwrap()
            .unwrap();

        a.apply_operation(op.clone()).unwrap();
        b.apply_operation(op).unwrap();

        assert_eq!(a, b);
        assert_eq!(a.iter().copied().collect::<Vec<_>>(), vec![1, 9, 8, 2, 3]);
    }

    #[test]
    fn range_delete_operations_match_direct_delete() {
        let base = new_list([1, 2, 3, 4, 5]);
        let range = base.ids_in_range(1..=3).unwrap();

        let mut direct = base.clone();
        range.delete(&mut direct).unwrap();

        let mut via_ops = base;
        for op in range.clone().delete_operations() {
            via_ops.apply_operation(op).unwrap();
        }

        assert_eq!(direct, via_ops);
        assert_eq!(direct.iter().copied().collect::<Vec<_>>(), vec![1, 5]);
    }

    #[test]
    fn concurrent_inserts_converge_independent_of_delivery_order() {
        let base = new_list([0]);

        let op_a = base
            .insert_operation_at(1, IdWithIndex::zero(3), [10])
            .unwrap()
            .unwrap();
        let op_b = base
            .insert_operation_at(1, IdWithIndex::zero(4), [20])
            .unwrap()
            .unwrap();

        let mut r1 = new_list([0]);
        r1.apply_operation(op_a.clone()).unwrap();
        r1.apply_operation(op_b.clone()).unwrap();

        let mut r2 = new_list([0]);
        r2.apply_operation(op_b).unwrap();
        r2.apply_operation(op_a).unwrap();

        assert_eq!(r1, r2);
        assert_eq!(r1.iter().copied().collect::<Vec<_>>(), vec![0, 10, 20]);
    }

    #[test]
    fn three_way_concurrent_permutations_converge() {
        let base = new_list([0]);

        let ops = [
            base.insert_operation_at(1, IdWithIndex::zero(3), [10])
                .unwrap()
                .unwrap(),
            base.insert_operation_at(1, IdWithIndex::zero(4), [20])
                .unwrap()
                .unwrap(),
            base.insert_operation_at(1, IdWithIndex::zero(5), [30])
                .unwrap()
                .unwrap(),
        ];

        let mut previous_result: Option<LinearList<Id, Value>> = None;
        for perm in ops.iter().permutations(ops.len()) {
            let mut list = new_list([0]);
            for op in perm {
                list.apply_operation(op.clone()).unwrap();
            }

            assert_eq!(
                list.iter().copied().collect::<Vec<_>>(),
                vec![0, 10, 20, 30]
            );
            if let Some(ref previous) = previous_result {
                assert_eq!(previous, &list);
            }
            previous_result = Some(list);
        }
    }

    #[test]
    fn multi_writer_multi_step_convergence() {
        let shared_base = new_list([0]);

        // Three writers each produce a two-step causal chain from the same initial base.
        let writer_ops: [Vec<ListOperation<Id, Value>>; 3] = std::array::from_fn(|writer| {
            let mut local = shared_base.clone();
            let (first_id, first_value, second_id, second_value) = match writer {
                0 => (3, 100, 4, 101),
                1 => (5, 200, 6, 201),
                2 => (7, 300, 8, 301),
                _ => unreachable!(),
            };

            let op1 = local
                .insert_operation_at(1, IdWithIndex::zero(first_id), [first_value])
                .unwrap()
                .unwrap();
            local.apply_operation(op1.clone()).unwrap();

            let op2 = local
                .insert_operation_at(2, IdWithIndex::zero(second_id), [second_value])
                .unwrap()
                .unwrap();
            local.apply_operation(op2.clone()).unwrap();

            vec![op1, op2]
        });

        // Generate all interleavings preserving per-writer causal order.
        fn interleavings_with_local_order(
            per_writer_count: usize,
            num_writers: usize,
        ) -> Vec<Vec<usize>> {
            let total_steps = per_writer_count * num_writers;
            let mut out = Vec::new();
            let mut current = Vec::with_capacity(total_steps);
            let mut next_for_writer = vec![0usize; num_writers];

            fn dfs(
                per_writer_count: usize,
                total_steps: usize,
                current: &mut Vec<usize>,
                next_for_writer: &mut [usize],
                out: &mut Vec<Vec<usize>>,
            ) {
                if current.len() == total_steps {
                    out.push(current.clone());
                    return;
                }

                for writer in 0..next_for_writer.len() {
                    if next_for_writer[writer] < per_writer_count {
                        next_for_writer[writer] += 1;
                        current.push(writer);
                        dfs(per_writer_count, total_steps, current, next_for_writer, out);
                        current.pop();
                        next_for_writer[writer] -= 1;
                    }
                }
            }

            dfs(
                per_writer_count,
                total_steps,
                &mut current,
                &mut next_for_writer,
                &mut out,
            );
            out
        }

        let schedules = interleavings_with_local_order(2, 3);
        assert_eq!(schedules.len(), 90);

        let mut previous_result: Option<LinearList<Id, Value>> = None;
        for schedule in schedules {
            let schedule_trace = schedule.clone();
            let mut list = shared_base.clone();
            let mut next_for_writer = [0usize; 3];

            for writer in schedule {
                let next_idx = next_for_writer[writer];
                let op = writer_ops[writer][next_idx].clone();
                list.apply_operation(op).unwrap();
                next_for_writer[writer] += 1;
            }

            assert_eq!(next_for_writer, [2, 2, 2]);

            if let Some(ref previous) = previous_result {
                assert_eq!(
                    previous.iter().copied().collect::<Vec<_>>(),
                    list.iter().copied().collect::<Vec<_>>(),
                    "Result content did not match for schedule: {:?}",
                    schedule_trace
                );
                assert_eq!(
                    previous, &list,
                    "Result structure did not match for schedule: {:?}",
                    schedule_trace
                );
            }
            previous_result = Some(list);
        }
    }

    #[test]
    fn operation_with_missing_anchor_is_rejected() {
        let mut list = new_list([1, 2, 3]);
        let before = list.clone();

        let malformed = ListOperation {
            op: DataOperation::Insert {
                id: IdWithIndex::zero(42),
                pred: IdWithIndex::zero(999),
                succ: IdWithIndex::zero(1000),
                value: vec![7],
            },
        };

        let res = list.apply_operation(malformed.clone());
        assert_eq!(res, Err(malformed));
        assert_eq!(list, before);
    }

    #[test]
    fn duplicate_conflict_set_rejected() {
        let base = new_list([0]);
        let op = base.append_operation(IdWithIndex::zero(3), [10]).unwrap();

        let mut list = new_list([0]);
        list.apply_operation(op.clone()).unwrap();

        let before_retry = list.clone();
        let retry_res = list.apply_operation(op);
        assert!(retry_res.is_err());
        assert_eq!(list, before_retry);
    }

    #[test]
    fn causal_dependency_violation_fails_cleanly() {
        let base = new_list([0]);

        let mut writer = base.clone();
        let op1 = writer
            .insert_operation_at(1, IdWithIndex::zero(3), [10])
            .unwrap()
            .unwrap();
        writer.apply_operation(op1.clone()).unwrap();
        let op2 = writer
            .insert_operation_at(2, IdWithIndex::zero(6), [11])
            .unwrap()
            .unwrap();

        let mut target = new_list([0]);
        let before_invalid = target.clone();
        let invalid_res = target.apply_operation(op2.clone());
        assert!(invalid_res.is_err());
        assert_eq!(target, before_invalid);

        target.apply_operation(op1).unwrap();
        target.apply_operation(op2).unwrap();
        assert_eq!(target.iter().copied().collect::<Vec<_>>(), vec![0, 10, 11]);
    }
}
