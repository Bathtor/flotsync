use super::*;
use flotsync_utils::{debugging::DebugFormatting, require};
use std::ops::RangeBounds;

pub trait Composite: Sized {
    /// The indivisible element type of this composite type.
    ///
    /// E.g. `u8` for a `Vec<u8>`.
    type Element: ?Sized;

    /// The element iterator type return by [[Composite::iter]].
    type Iter<'a>: Iterator<Item = &'a Self::Element>
    where
        Self: 'a;

    /// Returns the element at `index`, if it exists.
    fn get(&self, index: usize) -> Option<&Self::Element>;

    /// Returns the number of elements.
    fn len(&self) -> usize;

    /// Split the data into two at the element with the given `index`.
    ///
    /// The element at `index` is the contained in the second part.
    fn split_at(self, index: usize) -> (Self, Self);

    /// Produce a new composite that starts with `self` and is followed by `other`.
    fn concat(self, other: Self) -> Self;

    /// Element iterator.
    fn iter(&self) -> Self::Iter<'_>;

    /* Default Implementations */

    /// Returns `true` if this composite has no elements.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub enum DeleteError {
    InvalidRange,
    NotFound,
}

/// An implementation of [[LinearData]] using a [[Vec]] to track the individual operation nodes.
///
/// # Coalescing
/// Sequences with identical metadata are coalesced into a single node to save space.
/// Coalesced nodes can be split automatically on demand to support inserts and deletes within
/// the coalesced range.
///
/// This requires Values to implement the [[Composite]] trait, to facilitate coalescing and splitting.
///
/// Otherwise the same properties as the [[VecLinearData]] apply.
#[derive(Clone, Debug)]
pub struct VecCoalescedLinearData<Id, Value> {
    /// The number of values in Insert nodes in `base`.
    len: usize,
    base: VecLinearData<IdWithIndex<Id>, Value>,
}
impl<BaseId, Value> VecCoalescedLinearData<BaseId, Value>
where
    BaseId: Clone + fmt::Debug + PartialEq + 'static,
    Value: Composite + fmt::Debug + 'static,
{
    pub fn new<I>(id_generator: &mut I) -> Self
    where
        I: Iterator<Item = BaseId>,
    {
        let mut sub_id_generator = IdGeneratorWithSubIndex::new(id_generator);
        let base = VecLinearData::new(&mut sub_id_generator);
        Self { len: 0, base }
    }

    pub fn with_value<I>(id_generator: &mut I, initial_value: Value) -> Self
    where
        I: Iterator<Item = BaseId>,
    {
        if initial_value.is_empty() {
            return Self::new(id_generator);
        }

        let value_len = initial_value.len();

        let begin_id = IdWithIndex::zero(
            id_generator
                .next()
                .expect("The generator must produce sufficient ids."),
        );
        let begin_node = Node {
            id: begin_id.clone(),
            origin: None,
            operation: Operation::Beginning,
        };

        let mut nodes = Vec::with_capacity(3);
        nodes.push(begin_node);

        let mut next_id = begin_id.increment();
        let mut remaining_value_opt = Some(initial_value);
        while let Some(remaining_value) = remaining_value_opt.take() {
            let remaining_indices = next_id.addressable_len();
            let (current_value, current_id) = if remaining_indices < remaining_value.len() {
                let (head, remainder) = remaining_value.split_at(remaining_indices);
                remaining_value_opt = Some(remainder);
                let id = std::mem::replace(
                    &mut next_id,
                    IdWithIndex::zero(
                        id_generator
                            .next()
                            .expect("The generator must produce sufficient ids."),
                    ),
                );
                (head, id)
            } else {
                let mut id = &next_id
                    + remaining_value
                        .len()
                        .try_into()
                        .expect("Should fit into u16 now");
                std::mem::swap(&mut id, &mut next_id);

                (remaining_value, id)
            };
            let value_node = Node {
                id: current_id,
                origin: nodes.last().map(|n| n.last_id()),
                operation: Operation::Insert {
                    value: current_value,
                },
            };
            nodes.push(value_node);
        }

        let end_id = next_id;
        let end_node = Node {
            id: end_id,
            origin: nodes.last().map(|n| n.last_id()),
            operation: Operation::End,
        };
        nodes.push(end_node);

        let base = VecLinearData { len: 1, nodes };
        Self {
            len: value_len,
            base,
        }
    }

    /// The number of elements, i.e. [[Composite::Element]] that are not deleted.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn append(&mut self, id: IdWithIndex<BaseId>, value: Value) {
        assert!(
            id.can_address(value.len()),
            "The id='{id:?}' cannot address all elements of '{value:?}'"
        );
        self.len += value.len();
        self.base.append(id, value);
    }

    pub fn prepend(&mut self, id: IdWithIndex<BaseId>, value: Value) {
        assert!(
            id.can_address(value.len()),
            "The id='{id:?}' cannot address all elements of '{value:?}'"
        );
        self.len += value.len();
        self.base.prepend(id, value);
    }

    /// Delete the (sub-range of the) node corresponding to [start, end].
    ///
    /// Returns Err (and deletes nothing) if this range is not part of a single node.
    pub fn delete_range(
        &mut self,
        start: &IdWithIndex<BaseId>,
        end: &IdWithIndex<BaseId>,
    ) -> Result<(), DeleteError> {
        if start == end {
            return self.delete(start).map(|_| ()).ok_or(DeleteError::NotFound);
        }
        require!(start.id == end.id, DeleteError::InvalidRange);
        require!(start.index <= end.index, DeleteError::InvalidRange);

        let (start_node_index, start_node) = self
            .base
            .nodes
            .iter()
            .enumerate()
            .find(|(_index, node)| node.contains(start))
            .ok_or(DeleteError::NotFound)?;

        enum DeleteMode {
            Suffix {
                node_index: usize,
            },
            Subrange {
                node_index: usize,
            },
            Full {
                node_index: usize,
            },
            Prefix {
                node_index: usize,
            },
            Skip {
                #[allow(unused, reason = "Useful for debugging and takes no extra space.")]
                node_index: usize,
            },
        }

        let item_from_node = |node_index: usize,
                              node: &Node<IdWithIndex<BaseId>, Value>,
                              contains_start: bool,
                              contains_end: bool| {
            let start_at_node_start = contains_start && &node.id == start;
            let end_at_node_end = contains_end && node.last_index() == end.index;

            if matches!(node.operation, Operation::Delete { .. }) {
                DeleteMode::Skip { node_index }
            } else if (!contains_start && !contains_end) || (start_at_node_start && end_at_node_end)
            {
                DeleteMode::Full { node_index }
            } else if start_at_node_start {
                DeleteMode::Prefix { node_index }
            } else if end_at_node_end {
                DeleteMode::Suffix { node_index }
            } else {
                DeleteMode::Subrange { node_index }
            }
        };
        let mut work_items: Vec<DeleteMode> = Vec::new();
        let mut found_end = start_node.contains(end);
        let start_item = item_from_node(start_node_index, start_node, true, found_end);
        work_items.push(start_item);

        let mut nodes = self
            .base
            .nodes
            .iter()
            .enumerate()
            .skip(start_node_index + 1)
            .filter(|(_index, node)| node.id.id == end.id);
        while !found_end {
            if let Some((node_index, node)) = nodes.next() {
                found_end = node.contains(end);
                let item = item_from_node(node_index, node, false, found_end);
                work_items.push(item);
            } else {
                return Err(DeleteError::NotFound);
            }
        }
        debug_assert!(found_end);

        for item in work_items {
            match item {
                DeleteMode::Skip { .. } => (), // Just do nothing for these.
                DeleteMode::Suffix { node_index } => {
                    let new_node_index =
                        self.split_node(node_index, start.index, SplitMode::Before);
                    let node = &mut self.base.nodes[new_node_index];
                    node.operation.delete();
                    self.len -= node.node_len();
                    self.base.len -= 1;
                }
                DeleteMode::Subrange { node_index } => {
                    let node_index_after_start_split =
                        self.split_node(node_index, start.index, SplitMode::Before);
                    let node_index_after_end_split =
                        self.split_node(node_index_after_start_split, end.index, SplitMode::After);
                    let node = &mut self.base.nodes[node_index_after_end_split];
                    node.operation.delete();
                    self.len -= node.node_len();
                    self.base.len -= 1;
                }
                DeleteMode::Full { node_index } => {
                    let node = &mut self.base.nodes[node_index];
                    node.operation.delete();
                    self.len -= node.node_len();
                    self.base.len -= 1;
                }
                DeleteMode::Prefix { node_index } => {
                    let new_node_index = self.split_node(node_index, end.index, SplitMode::After);
                    let node = &mut self.base.nodes[new_node_index];
                    node.operation.delete();
                    self.len -= node.node_len();
                    self.base.len -= 1;
                }
            }
        }

        Ok(())
    }

    /// Returns the ids that make up insert nodes in the given `range` of element positions.
    pub fn ids_in_range<R>(&self, range: R) -> Option<NodeIdRange<BaseId>>
    where
        R: RangeBounds<usize>,
    {
        let (start_node, start_id, mut next_position) = {
            match range.start_bound() {
                std::ops::Bound::Included(position) => {
                    self.node_at_position(*position).map(|pos| {
                        let node = &self.base.nodes[pos.node_index];
                        // This must fit if position is actually within the node.
                        let start_offset: u16 =
                            (position - pos.node_start_position).try_into().unwrap();
                        (pos, &node.id + start_offset, *position)
                    })?
                }
                std::ops::Bound::Excluded(position) => {
                    let node_at_position = self.node_at_position(*position)?;
                    let position_offset = position - node_at_position.node_start_position;
                    let included_position_offset = position_offset + 1;
                    let node = &self.base.nodes[node_at_position.node_index];
                    if node.node_len() > included_position_offset {
                        // The next position of the one we are excluding fits in here.
                        let id = &node.id + included_position_offset.try_into().unwrap();
                        (node_at_position, id, position + 1)
                    } else {
                        // The next position is the beginning of the next node.
                        self.base
                            .iter_inserts_from(node_at_position.node_index + 1)
                            .next()
                            .map(|(node_index, node)| {
                                let node_end_position =
                                    node_at_position.node_start_position + node.node_len();
                                let pos = NodePosition {
                                    node_index,
                                    node_start_position: node_end_position + 1,
                                };
                                let id = node.id.clone();
                                (pos, id, position + 1)
                            })?
                    }
                }
                std::ops::Bound::Unbounded => {
                    // First Insert.
                    self.base.iter_inserts().next().map(|(node_index, node)| {
                        let pos = NodePosition {
                            node_index,
                            node_start_position: 0,
                        };
                        let id = node.id.clone();
                        (pos, id, 0)
                    })?
                }
            }
        };

        let mut current_node_start_id = start_id.clone();
        let mut current_node_index = start_node.node_index;
        let mut current_node = &self.base.nodes[current_node_index];
        let mut current_node_start_position = start_node.node_start_position;
        let mut current_node_end_position =
            start_node.node_start_position + current_node.node_len();

        let mut insert_nodes = self.base.iter_inserts_from(start_node.node_index + 1);

        let mut node_ids_in_range: Vec<IdWithIndexRange<BaseId>> = Vec::with_capacity(1);

        let mut reached_the_end = false;
        while range.contains(&next_position) {
            if next_position >= current_node_end_position {
                // Move to the next node.
                if let Some((new_node_index, new_node)) = insert_nodes.next() {
                    let id = std::mem::replace(&mut current_node_start_id, new_node.id.clone());

                    // Close out the current node.
                    node_ids_in_range
                        .push(IdWithIndexRange::with_end(id, current_node.last_index()));

                    // Update the current node.
                    current_node_index = new_node_index;
                    current_node = new_node;
                    current_node_start_position = next_position;
                    current_node_end_position = next_position + new_node.node_len();
                } else {
                    reached_the_end = true;
                    // No nodes left.
                    match range.end_bound() {
                        std::ops::Bound::Included(_) => {
                            // The range's end does not occur in the collection.
                            return None;
                        }
                        std::ops::Bound::Excluded(position) => {
                            if *position == next_position {
                                // This is fine.
                                break;
                            } else {
                                // The range's end does not occur in the collection.
                                return None;
                            }
                        }
                        std::ops::Bound::Unbounded => {
                            // Very well, we reached the end.
                            break;
                        }
                    }
                }
            }
            next_position += 1;
        }

        // Close out the last node.
        let end_index = if reached_the_end {
            current_node.last_index()
        } else {
            let end_offset = match range.end_bound() {
                std::ops::Bound::Included(position) => {
                    (position - current_node_start_position).try_into().unwrap()
                }
                std::ops::Bound::Excluded(position) => (position - current_node_start_position - 1)
                    .try_into()
                    .unwrap(),
                std::ops::Bound::Unbounded => unreachable!("We should have reached the end."),
            };
            (&current_node.id + end_offset).index
        };
        let last_id_range = IdWithIndexRange::with_end(current_node_start_id, end_index);
        let end_id = last_id_range.last();
        node_ids_in_range.push(last_id_range);

        let predecessor = self.predecessor_id(&start_id, start_node.node_index);
        let successor = self.successor_id(&end_id, current_node_index);
        Some(NodeIdRange {
            predecessor,
            contained: node_ids_in_range,
            successor,
        })
    }

    fn predecessor_id(&self, id: &IdWithIndex<BaseId>, node_index: usize) -> IdWithIndex<BaseId> {
        let node = &self.base.nodes[node_index];
        let id_offset = id.index_diff(&node.id);
        if id_offset > 0 {
            // It's the same node, but the previous index.
            id.decrement()
        } else {
            // It's the last position of the previous node.
            let predecessor_node = &self.base.nodes[node_index - 1];
            predecessor_node.last_id()
        }
    }

    fn successor_id(&self, id: &IdWithIndex<BaseId>, node_index: usize) -> IdWithIndex<BaseId> {
        let node = &self.base.nodes[node_index];
        let id_offset = id.index_diff(&node.id);
        if id_offset as usize + 1 < node.node_len() {
            // It's the same node, but the next index.
            id.increment()
        } else {
            // It's the beginning of the next node.
            let successor_node = &self.base.nodes[node_index + 1];
            successor_node.id.clone()
        }
    }

    /// Returns the position info of the node containing the element at `position`.
    fn node_at_position(&self, position: usize) -> Option<NodePosition> {
        let mut node_index_at_position_opt: Option<usize> = None;
        let inserts = self.base.iter_inserts();
        let mut current_node_start_position = 0usize;
        for (node_index, node) in inserts {
            let node_value_len = node.get_len().unwrap();
            if position < (current_node_start_position + node_value_len) {
                // The position is somewhere within this node's value.
                node_index_at_position_opt = Some(node_index);
                break;
            } else {
                assert!(position > current_node_start_position);
                // We are looking for a later node.
                current_node_start_position += node_value_len;
            }
        }
        node_index_at_position_opt.map(|node_index| NodePosition {
            node_index,
            node_start_position: current_node_start_position,
        })
    }

    /// Splits the node at `node_index` according to `mode` around `split_index` and returns
    /// the node index of the new node with the id that matches `split_index`.
    fn split_node(&mut self, node_index: usize, split_index: u16, mut mode: SplitMode) -> usize {
        let target_node = &mut self.base.nodes[node_index];
        assert!(
            split_index <= target_node.last_index(),
            "Cannot split a node beyond the end (at {split_index}): {:?}",
            self.base.nodes[node_index]
        );
        let split_offset = split_index - target_node.id.index;

        let split_at_head = split_offset == 0;
        let split_at_end = split_index == target_node.last_index();
        match mode {
            SplitMode::Before => {
                assert!(
                    !split_at_head,
                    "Shouldn't have invoked split_node Before at the beginning of the node."
                );
            }
            SplitMode::After => {
                assert!(
                    !split_at_end,
                    "Shouldn't have invoked split_node After at the end of the node."
                );
            }
            SplitMode::BeforeAndAfter => {
                // We'll be a bit more lenient here and simply adjust the mode,
                // as long as at least one split can happen here.
                assert!(
                    !(split_at_head && split_at_end),
                    "Shouldn't have invoked split_node BeforeAndAfter on a single element node (len={})",
                    target_node.node_len()
                );
                if split_at_head {
                    mode = SplitMode::After;
                } else if split_at_end {
                    mode = SplitMode::Before;
                }
            }
        }

        // Construct a placeholder we can swap into place while we are splitting.
        let mut tmp_node = Node {
            id: target_node.id.clone(),
            origin: None,
            operation: Operation::Invalid,
        };
        std::mem::swap(&mut tmp_node, target_node);

        let (first_node, second_node, third_node_opt) = match mode {
            SplitMode::Before => {
                let mut first_node = tmp_node;
                let second_node_op = first_node
                    .operation
                    .split_off(split_offset)
                    .expect("Cannot split node.");
                let mut second_node_id = first_node.id.clone();
                second_node_id.index = split_index;
                let second_node = Node {
                    id: second_node_id,
                    origin: Some(first_node.id.clone()),
                    operation: second_node_op,
                };
                (first_node, second_node, None)
            }
            SplitMode::After => {
                let mut first_node = tmp_node;
                let second_node_op = first_node
                    .operation
                    .split_off(split_offset + 1)
                    .expect("Cannot split node.");
                let mut second_node_id = first_node.id.clone();
                second_node_id.index = split_index + 1;
                let second_node = Node {
                    id: second_node_id,
                    origin: Some(first_node.id.clone()),
                    operation: second_node_op,
                };
                (first_node, second_node, None)
            }
            SplitMode::BeforeAndAfter => {
                let mut first_node = tmp_node;
                let mut second_node_op = first_node
                    .operation
                    .split_off(split_offset)
                    .expect("Cannot split node.");
                let third_node_op = second_node_op.split_off(1).expect("Cannot split node");
                let mut second_node_id = first_node.id.clone();
                second_node_id.index = split_index;
                let second_node = Node {
                    id: second_node_id,
                    origin: Some(first_node.id.clone()),
                    operation: second_node_op,
                };
                let third_node_id = second_node.id.increment();
                let third_node = Node {
                    id: third_node_id,
                    origin: Some(second_node.id.clone()),
                    operation: third_node_op,
                };

                (first_node, second_node, Some(third_node))
            }
        };
        // Put the first node back into the original place.
        *target_node = first_node;
        // Insert second (and optionally third) node after.
        self.base.nodes.insert(node_index + 1, second_node);
        if let Some(third_node) = third_node_opt {
            self.base.nodes.insert(node_index + 2, third_node);
        }
        match mode {
            SplitMode::Before => {
                self.base.len += 1;
                node_index + 1
            }
            SplitMode::After => {
                self.base.len += 1;
                node_index
            }
            SplitMode::BeforeAndAfter => {
                self.base.len += 2;
                node_index + 1
            }
        }
    }

    #[cfg(test)]
    pub(in crate::text) fn check_integrity(&self) {
        use itertools::Itertools;

        self.base.check_integrity();
        let value_len = self
            .base
            .nodes
            .iter()
            .flat_map(|n| match n.operation {
                Operation::Insert { ref value } => Some(value.len()),
                _ => None,
            })
            .sum();
        assert_eq!(self.len, value_len);
        // Check that all the id ranges are non-overlapping.
        // This check is a bit expensive, but for now it's acceptable in testing.
        // Maybe turn it off if the tests are getting too slow.
        for pair in self.base.nodes.iter().combinations(2) {
            if let [left, right] = pair.as_slice() {
                for id in left.ids() {
                    assert!(
                        !right.contains(&id),
                        "Duplicate id detected. Node {right:?} contains {id:?} from node {left:?}."
                    );
                }
                for id in right.ids() {
                    assert!(
                        !left.contains(&id),
                        "Duplicate id detected. Node {left:?} contains {id:?} from node {right:?}."
                    );
                }
            } else {
                unreachable!("combinations should always produce 2-sized vectors.")
            }
        }
    }
}
impl<BaseId, Value> LinearData<Value, Value::Element> for VecCoalescedLinearData<BaseId, Value>
where
    BaseId: Clone + fmt::Debug + PartialEq + 'static,
    Value: Composite + fmt::Debug + 'static,
{
    type Id = IdWithIndex<BaseId>;

    type Iter<'a> = VecCoalescedLinearDataIter<'a, IdWithIndex<BaseId>, Value>;

    fn ids_after_head(&self) -> LinkIds<Self::Id> {
        LinkIds {
            predecessor: self.base.nodes[0].id.clone(),
            successor: self.base.nodes[1].id.clone(),
        }
    }

    fn ids_before_end(&self) -> LinkIds<Self::Id> {
        let len = self.base.nodes.len();
        LinkIds {
            predecessor: self.base.nodes[len - 2].last_id(),
            successor: self.base.nodes[len - 1].id.clone(),
        }
    }

    fn ids_at_pos(&self, position: usize) -> Option<NodeIds<Self::Id>> {
        if position < self.len {
            // This must exist in this branch, otherwise self.len is wrong.
            let NodePosition {
                node_index: node_index_at_position,
                node_start_position,
            } = self.node_at_position(position).unwrap();
            let position_offset: u16 = (position - node_start_position)
                .try_into()
                .expect("Index offsets must fit into a u16");

            // All of these must exist if the list is valid.
            let current_node = &self.base.nodes[node_index_at_position];
            let current = &current_node.id + position_offset;
            let predecessor = self.predecessor_id(&current, node_index_at_position);
            let successor = self.successor_id(&current, node_index_at_position);
            Some(NodeIds {
                predecessor,
                current,
                successor,
            })
        } else {
            None
        }
    }

    fn insert(
        &mut self,
        id: Self::Id,
        pred: Self::Id,
        succ: Self::Id,
        value: Value,
    ) -> Result<(), Value> {
        self.apply_operation(DataOperation::Insert {
            id,
            pred,
            succ,
            value,
        })
        .map_err(|op| match op {
            DataOperation::Insert { value, .. } => value,
            _ => {
                // The apply_operation should not return a different operation type on error.
                unreachable!();
            }
        })
    }

    fn delete<'a>(&'a mut self, id: &Self::Id) -> Option<&'a Value::Element> {
        //println!("Trying to delete id={id:?} from: {:#?}", self.nodes);
        let (node_index, node) = self
            .base
            .nodes
            .iter()
            .enumerate()
            .find(|(_index, n)| n.contains(id))?;

        // We are only supposed to delete a single element here.
        let must_split = matches!(
            node.operation,
            Operation::Insert { ref value } if value.len() > 1
        );
        let node_index = if must_split {
            self.split_node(node_index, id.index, SplitMode::BeforeAndAfter)
        } else {
            node_index
        };

        let node = &mut self.base.nodes[node_index];
        match node.operation {
            Operation::Insert { ref value } => {
                debug_assert!(value.len() == 1);
                node.operation.delete();
                self.len -= 1;
                self.base.len -= 1;
                if let Operation::Delete { ref value } = node.operation {
                    value.get(0)
                } else {
                    // We literally just put it there.
                    unreachable!()
                }
            }
            // Double delete is OK.
            Operation::Delete { ref value } => value.get((id.index - node.id.index) as usize),
            // These cannot be deleted.
            Operation::Beginning | Operation::End => {
                //println!("Tried to delete Beginning/End");
                None
            }
            Operation::Invalid => panic!("Node is invalid."),
        }
    }

    fn apply_operation(
        &mut self,
        operation: DataOperation<Self::Id, Value>,
    ) -> Result<(), DataOperation<Self::Id, Value>> {
        match operation {
            DataOperation::Insert {
                ref pred, ref succ, ..
            } => {
                let pred_opt = self
                    .base
                    .nodes
                    .iter()
                    .enumerate()
                    .find(|(_, node)| node.contains(pred));
                if let Some((pred_index, pred_node)) = pred_opt {
                    let succ_opt = if pred_node.contains(succ) {
                        Some((pred_index, pred_node))
                    } else {
                        self.base
                            .nodes
                            .iter()
                            .enumerate()
                            .skip(pred_index)
                            .find(|(_, node)| node.contains(succ))
                    };
                    if let Some((succ_index, succ_node)) = succ_opt {
                        if pred_index == succ_index {
                            if pred.is_followed_by(succ) {
                                // We need to split and then we can insert where we split.
                                let new_succ_index =
                                    self.split_node(pred_index, succ.index, SplitMode::Before);
                                // Now we don't need the original operation anymore, so we can properly deconstruct it.
                                if let DataOperation::Insert {
                                    id, pred, value, ..
                                } = operation
                                {
                                    self.len += value.len();
                                    self.base.len += 1;
                                    self.base.nodes.insert(
                                        new_succ_index,
                                        Node {
                                            id,
                                            origin: Some(pred),
                                            operation: Operation::Insert { value },
                                        },
                                    );
                                    Ok(())
                                } else {
                                    unreachable!("We *know* it's an Insert.");
                                }
                            } else {
                                todo!("The nodes are apart and we need to resolve a position.");
                            }
                        } else if pred_index + 1 == succ_index {
                            if pred_node.last_id() == *pred && succ_node.id == *succ {
                                // We can just insert directly at the existing boundary.

                                // Now we don't need the original operation anymore, so we can properly deconstruct it.
                                if let DataOperation::Insert {
                                    id, pred, value, ..
                                } = operation
                                {
                                    self.len += value.len();
                                    self.base.len += 1;
                                    self.base.nodes.insert(
                                        succ_index,
                                        Node {
                                            id,
                                            origin: Some(pred),
                                            operation: Operation::Insert { value },
                                        },
                                    );
                                    Ok(())
                                } else {
                                    unreachable!("We *know* it's an Insert.");
                                }
                            } else {
                                todo!("The nodes are apart and we need to resolve a position.");
                            }
                        } else {
                            todo!("The nodes are apart and we need to resolve a position.");
                        }
                    } else {
                        // println!("Successor {succ:?} does not exist.");
                        Err(operation)
                    }
                } else {
                    // println!("Pred {pred:?} does not exist.");
                    Err(operation)
                }
            }
            DataOperation::Delete { ref start, ref end } => match end {
                Some(end) => self.delete_range(start, end).map_err(|_| operation),
                None => self.delete(start).map(|_| ()).ok_or(operation),
            },
        }
    }

    fn iter_values(&self) -> Self::Iter<'_> {
        VecCoalescedLinearDataIter {
            underlying: self.base.nodes.iter(),
            current_node_iter: None,
        }
    }

    fn iter_ids(&self) -> impl Iterator<Item = &Self::Id> {
        self.base.iter_ids()
    }
}
impl<BaseId, Value> DebugFormatting for VecCoalescedLinearData<BaseId, Value>
where
    BaseId: fmt::Display + 'static,
    Value: Composite + fmt::Display + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{{ ")?;
        for node in self.base.nodes.iter() {
            let content = match node.operation {
                Operation::Insert { ref value } => format!("'{}'", value),
                Operation::Delete { ref value } => format!("[^'{}']", value),
                Operation::Beginning => "$".to_owned(),
                Operation::End => "X".to_owned(),
                Operation::Invalid => "?!?".to_owned(),
            };
            if let Some(ref origin) = node.origin {
                write!(f, "{}|{} @ {}", origin, node.id, content)?;
            } else {
                write!(f, "{} @ {}", node.id, content)?;
            }
            if !matches!(node.operation, Operation::End) {
                write!(f, " -> ")?;
            }
        }
        write!(f, " }}")?;
        write!(
            f,
            "[{} live values, {} live nodes]",
            self.len, self.base.len
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SplitMode {
    /// New node starts with target id.
    Before,
    /// New node ends with target id.
    After,
    /// New node contains only target id.
    BeforeAndAfter,
}

pub struct VecCoalescedLinearDataIter<'a, Id, Value>
where
    Value: Composite,
{
    underlying: std::slice::Iter<'a, Node<Id, Value>>,
    current_node_iter: Option<Value::Iter<'a>>,
}
impl<'a, Id, Value> VecCoalescedLinearDataIter<'a, Id, Value>
where
    Value: Composite,
{
    fn next_from_underlying(&mut self) -> Option<&'a Value::Element> {
        match self.underlying.find_map(Node::get_current_value) {
            None => None,
            Some(node) => {
                let mut it = node.iter();
                let first = it.next();
                self.current_node_iter = Some(it);
                assert!(first.is_some(), "Empty elements are not allowed.");
                first
            }
        }
    }
}
impl<'a, Id, Value> Iterator for VecCoalescedLinearDataIter<'a, Id, Value>
where
    Value: Composite,
{
    type Item = &'a Value::Element;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut node_iter) = self.current_node_iter {
            if let Some(next) = node_iter.next() {
                return Some(next);
            } else {
                self.current_node_iter = None;
            }
        }
        self.next_from_underlying()
    }
}

#[derive(Clone, Copy, Debug)]
struct NodePosition {
    /// Position in `nodes` where the node is located.
    node_index: usize,
    /// Position in the total list of elements where the node's first element is located.
    node_start_position: usize,
}

pub struct IdGeneratorWithZeroIndex<'a, I>
where
    I: Iterator,
{
    underlying: &'a mut I,
}
impl<'a, I> IdGeneratorWithZeroIndex<'a, I>
where
    I: Iterator,
{
    pub fn new(iter: &'a mut I) -> Self {
        Self { underlying: iter }
    }
}
impl<'a, I> Iterator for IdGeneratorWithZeroIndex<'a, I>
where
    I: Iterator,
{
    type Item = IdWithIndex<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying
            .next()
            .map(|id| IdWithIndex { id, index: 0 })
    }
}

struct IdGeneratorWithSubIndex<'a, I>
where
    I: Iterator,
{
    underlying: &'a mut I,
    next_id: Option<I::Item>,
    next_index: Option<u16>,
}
impl<'a, I> IdGeneratorWithSubIndex<'a, I>
where
    I: Iterator,
{
    pub fn new(underlying: &'a mut I) -> Self {
        Self {
            underlying,
            next_id: None,
            next_index: Some(0u16),
        }
    }
}
impl<'a, I> Iterator for IdGeneratorWithSubIndex<'a, I>
where
    I: Iterator,
    I::Item: Clone,
{
    type Item = IdWithIndex<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        // Make sure we polled this at least once.
        if self.next_id.is_none() {
            self.next_id = self.underlying.next();
        }
        if let Some(ref id) = self.next_id {
            if let Some(index) = self.next_index {
                self.next_index = index.checked_add(1);
                Some(IdWithIndex {
                    id: id.clone(),
                    index,
                })
            } else {
                // Try to get another id and start the indexing over.
                self.next_id = self.underlying.next();
                if let Some(ref id) = self.next_id {
                    self.next_index = Some(1u16);
                    Some(IdWithIndex {
                        id: id.clone(),
                        index: 0u16,
                    })
                } else {
                    // We are empty.
                    None
                }
            }
        } else {
            // We are definitely empty.
            None
        }
    }
}

/// A variant of an operation id that allows multiple ordered operations at the same time,
/// without having to do explictly different operations for each.
#[allow(unused)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct IdWithIndex<Id> {
    pub id: Id,
    pub index: u16, // Probably sufficient for a single operation.
}
#[allow(unused)]
impl<Id> IdWithIndex<Id>
where
    Id: Clone + fmt::Debug + PartialEq,
{
    /// The maximum length of a single [[Composite]] addressed by an [[IdWithIndex]] starting
    /// at index 0.
    pub const MAX_LENGTH: usize = u16::MAX as usize;

    pub fn zero(id: Id) -> Self {
        IdWithIndex { id, index: 0 }
    }

    /// Gives the next sub-id (i.e. the next `index`).
    ///
    /// # Panics
    /// - If there are no indices left.
    pub fn increment(&self) -> Self {
        self.checked_increment()
            .expect("Cannot support more that 2^16 individual operations per id")
    }

    /// Gives the next sub-id (i.e. the next `index`), if there is one left.
    ///
    /// Otherwise the caller needs to produce a new major `id`.
    pub fn checked_increment(&self) -> Option<Self> {
        let mut next = self.clone();
        next.index.checked_add(1u16).map(|next_index| {
            next.index = next_index;
            next
        })
    }

    /// Gives the previous sub-id (i.e. the previous `index`).
    ///
    /// # Panics
    /// - If index is 0.
    pub fn decrement(&self) -> Self {
        self.checked_decrement()
            .expect("Cannot support more that 2^16 individual operations per id")
    }

    /// Gives the previous sub-id (i.e. the previous `index`), if index is not 0.
    pub fn checked_decrement(&self) -> Option<Self> {
        let mut prev = self.clone();
        prev.index.checked_sub(1u16).map(|prev_index| {
            prev.index = prev_index;
            prev
        })
    }

    /// Whether we can address all elements in a `num_elements` sized [[Composite]]
    /// if the first element is at `self.index`.
    #[inline]
    pub fn can_address(&self, num_elements: usize) -> bool {
        self.addressable_len() >= num_elements
    }

    /// Returns the number of elements in a [[Composite]] that can at most be addressed by this id.
    #[inline]
    pub fn addressable_len(&self) -> usize {
        Self::MAX_LENGTH - (self.index as usize)
    }

    /// Returns `true` iff `other` is the same as what `self.increment()` would return.
    pub fn is_followed_by(&self, other: &Self) -> bool {
        self.id == other.id
            && self
                .index
                .checked_add(1)
                .map(|next_index| next_index == other.index)
                .unwrap_or(false)
    }

    fn index_diff(&self, other: &IdWithIndex<Id>) -> u16 {
        assert_eq!(self.id, other.id);
        self.index
            .checked_sub(other.index)
            .expect("Index different overflowed")
    }

    /// Returns a new id where `id` is the the same as `self.id` and `index`` is the largest
    /// possible value.
    pub fn with_max_index(&self) -> Self {
        Self {
            id: self.id.clone(),
            index: u16::MAX,
        }
    }

    /// Returns a new id where `id` is the the same as `self.id` and `index`` is the smallest
    /// possible value (i.e. 0).
    pub fn with_zero_index(&self) -> Self {
        Self {
            id: self.id.clone(),
            index: 0,
        }
    }
}
impl<Id> std::ops::Add<u16> for IdWithIndex<Id>
where
    Id: Clone,
{
    type Output = Self;

    fn add(mut self, rhs: u16) -> Self::Output {
        self.index += rhs;
        self
    }
}
impl<Id> std::ops::Add<u16> for &IdWithIndex<Id>
where
    Id: Clone,
{
    type Output = IdWithIndex<Id>;

    fn add(self, rhs: u16) -> Self::Output {
        IdWithIndex {
            id: self.id.clone(),
            index: self.index + rhs,
        }
    }
}
impl<Id> fmt::Display for IdWithIndex<Id>
where
    Id: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.id, self.index)
    }
}

/// A range of indices identifying a particular contiguous range of ids.
///
/// This range is inclusive: [id:start_index, id:end_index]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdWithIndexRange<Id> {
    pub id: Id,
    pub start_index: u16,
    pub end_index: u16,
}
impl<Id> IdWithIndexRange<Id>
where
    Id: Clone,
{
    pub fn with_end(id: IdWithIndex<Id>, end_index: u16) -> Self {
        Self {
            id: id.id,
            start_index: id.index,
            end_index,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.start_index == self.end_index
    }

    pub fn len(&self) -> u16 {
        assert!(self.start_index <= self.end_index);
        self.end_index - self.start_index
    }

    pub fn first(&self) -> IdWithIndex<Id> {
        IdWithIndex {
            id: self.id.clone(),
            index: self.start_index,
        }
    }

    pub fn last(&self) -> IdWithIndex<Id> {
        IdWithIndex {
            id: self.id.clone(),
            index: self.end_index,
        }
    }
}

/// A group of ids identifying the concrete position of a consecutive range of nodes
/// at a particular point in time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeIdRange<Id> {
    pub predecessor: IdWithIndex<Id>,
    pub contained: Vec<IdWithIndexRange<Id>>,
    pub successor: IdWithIndex<Id>,
}
impl<Id> NodeIdRange<Id>
where
    Id: Clone + fmt::Debug + PartialEq + 'static,
{
    /// Tries to delete all the nodes contained in the range.
    ///
    /// Returns the first failing range if unsuccessful.
    /// In this case the previous deletes will have been applied.
    pub fn delete<'a, Value>(
        &'a self,
        data: &mut VecCoalescedLinearData<Id, Value>,
    ) -> Result<(), &'a IdWithIndexRange<Id>>
    where
        Value: Composite + fmt::Debug + 'static,
    {
        for id_range in self.contained.iter() {
            let start = id_range.first();
            let end = id_range.last();
            let op = DataOperation::Delete {
                start,
                end: Some(end),
            };
            data.apply_operation(op).map_err(|_| id_range)?;
        }
        Ok(())
    }

    pub fn delete_operations<T>(self) -> impl Iterator<Item = DataOperation<IdWithIndex<Id>, T>> {
        self.contained.into_iter().map(|range| {
            let start = range.first();
            let end = range.last();
            DataOperation::Delete {
                start,
                end: Some(end),
            }
        })
    }
}
