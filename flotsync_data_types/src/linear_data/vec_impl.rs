use super::*;
use crate::snapshot::{
    SnapshotHeader,
    SnapshotNode,
    SnapshotNodeRef,
    SnapshotReadError,
    SnapshotSink,
};

/// An implementation of [[LinearData]] using a [[Vec]] to track the individual operation nodes.
///
/// # Note
/// While the natural representation of this data structure is linked nodes,
/// storing them in a Vec is likely more efficient in practice for most usages
/// (e.g. read-mostly strings).
#[derive(Clone, Debug, PartialEq)]
pub struct VecLinearData<Id, Value> {
    /// The number of Insert nodes in the linear data.
    pub(super) len: usize,
    pub(super) nodes: Vec<Node<Id, Value>>,
}
impl<Id, Value> VecLinearData<Id, Value> {
    pub(crate) fn visit_snapshot<S, ValueRef: ?Sized, F>(
        &self,
        sink: &mut S,
        mut map_value: F,
    ) -> Result<(), S::Error>
    where
        S: SnapshotSink<Id, ValueRef>,
        F: FnMut(&Value) -> &ValueRef,
    {
        sink.begin(SnapshotHeader {
            node_count: self.nodes.len(),
        })?;

        let last_index = self.nodes.len().saturating_sub(1);
        for (index, node) in self.nodes.iter().enumerate() {
            let is_boundary = index == 0 || index == last_index;
            let (deleted, value) = if is_boundary {
                (false, None)
            } else {
                match &node.operation {
                    Operation::Insert { value } => (false, Some(map_value(value))),
                    Operation::Delete { value } => (true, Some(map_value(value))),
                    Operation::Beginning | Operation::End => {
                        panic!("Non-boundary node cannot be beginning/end.")
                    }
                    Operation::Invalid => panic!("Node is invalid."),
                }
            };

            let node_ref = SnapshotNodeRef {
                id: &node.id,
                left: node.left_origin.as_ref(),
                right: node.right_origin.as_ref(),
                deleted,
                value,
            };
            sink.node(index, node_ref)?;
        }

        sink.end()
    }

    pub(crate) fn from_snapshot_nodes<E, I>(nodes: I) -> Result<Self, SnapshotReadError<E>>
    where
        I: IntoIterator<Item = Result<SnapshotNode<Id, Value>, E>>,
    {
        let mut iter = nodes.into_iter();
        let (lower, _) = iter.size_hint();

        let Some(first) = iter.next() else {
            return Err(SnapshotReadError::MissingBoundaryNodes);
        };
        let first = first.map_err(SnapshotReadError::from_source)?;
        if first.left.is_some() {
            return Err(SnapshotReadError::BoundaryNodeHasLeft { index: 0 });
        }
        if first.value.is_some() {
            return Err(SnapshotReadError::BoundaryNodeHasValue { index: 0 });
        }
        if first.deleted {
            return Err(SnapshotReadError::BoundaryNodeMarkedDeleted { index: 0 });
        }

        let Some(second) = iter.next() else {
            return Err(SnapshotReadError::MissingBoundaryNodes);
        };
        let mut pending = second.map_err(SnapshotReadError::from_source)?;

        let mut reconstructed_nodes = Vec::with_capacity(lower.max(2));
        reconstructed_nodes.push(Node {
            id: first.id,
            left_origin: first.left,
            right_origin: first.right,
            operation: Operation::Beginning,
        });

        let mut len = 0usize;
        let mut pending_index = 1usize;
        for entry in iter {
            let next = entry.map_err(SnapshotReadError::from_source)?;

            let SnapshotNode {
                id,
                left,
                right,
                deleted,
                value,
            } = pending;

            let left = left.ok_or(SnapshotReadError::NonBoundaryNodeMissingLeft {
                index: pending_index,
            })?;
            let right = right.ok_or(SnapshotReadError::NonBoundaryNodeMissingRight {
                index: pending_index,
            })?;
            let value = value.ok_or(SnapshotReadError::NonBoundaryNodeMissingValue {
                index: pending_index,
            })?;
            let operation = if deleted {
                Operation::Delete { value }
            } else {
                len += 1;
                Operation::Insert { value }
            };
            reconstructed_nodes.push(Node {
                id,
                left_origin: Some(left),
                right_origin: Some(right),
                operation,
            });

            pending = next;
            pending_index += 1;
        }

        if pending.right.is_some() {
            return Err(SnapshotReadError::BoundaryNodeHasRight {
                index: pending_index,
            });
        }
        if pending.value.is_some() {
            return Err(SnapshotReadError::BoundaryNodeHasValue {
                index: pending_index,
            });
        }
        if pending.deleted {
            return Err(SnapshotReadError::BoundaryNodeMarkedDeleted {
                index: pending_index,
            });
        }
        reconstructed_nodes.push(Node {
            id: pending.id,
            left_origin: pending.left,
            right_origin: pending.right,
            operation: Operation::End,
        });

        Ok(Self {
            len,
            nodes: reconstructed_nodes,
        })
    }
}
impl<Id, Value> VecLinearData<Id, Value>
where
    Id: Clone + fmt::Debug + PartialEq + Eq,
{
    pub fn new<I>(id_generator: &mut I) -> Self
    where
        I: Iterator<Item = Id>,
    {
        let begin_id = id_generator
            .next()
            .expect("The generator must produce sufficient ids.");
        let end_id = id_generator
            .next()
            .expect("The generator must produce sufficient ids.");
        let begin_node = Node {
            id: begin_id.clone(),
            left_origin: None,
            right_origin: Some(end_id.clone()),
            operation: Operation::Beginning,
        };
        let end_node = Node {
            id: end_id,
            left_origin: Some(begin_id),
            right_origin: None,
            operation: Operation::End,
        };
        let nodes = vec![begin_node, end_node];
        Self { len: 0, nodes }
    }

    pub fn with_value<I>(id_generator: &mut I, initial_value: Value) -> Self
    where
        I: Iterator<Item = Id>,
    {
        let begin_id = id_generator
            .next()
            .expect("The generator must produce sufficient ids.");
        let value_id = id_generator
            .next()
            .expect("The generator must produce sufficient ids.");
        let end_id = id_generator
            .next()
            .expect("The generator must produce sufficient ids.");

        let begin_node = Node {
            id: begin_id.clone(),
            left_origin: None,
            right_origin: Some(value_id.clone()),
            operation: Operation::Beginning,
        };
        let value_node = Node {
            id: value_id.clone(),
            left_origin: Some(begin_id.clone()),
            right_origin: Some(end_id.clone()),
            operation: Operation::Insert {
                value: initial_value,
            },
        };
        let end_node = Node {
            id: end_id,
            left_origin: Some(value_id.clone()),
            right_origin: None,
            operation: Operation::End,
        };
        let nodes = vec![begin_node, value_node, end_node];
        Self { len: 1, nodes }
    }

    /// Returns `true` iff `node` is in the transitive right subtree that includes `boundary`.
    ///
    /// In other words, if you follow `right_origin` anchors starting from node, you hit the
    /// node with `id = boundary` before you find a `None`.
    pub(super) fn ends_in_right_tree<'a>(
        &'a self,
        mut node: &'a Node<Id, Value>,
        boundary: &Id,
    ) -> bool {
        loop {
            if &node.id == boundary {
                return true;
            }

            if let Some(ref right) = node.right_origin {
                node = self
                    .nodes
                    .iter()
                    .find(|n| &n.id == right)
                    .expect("For every origin a node should exist");
            } else {
                return false;
            };
        }
    }
}
impl<Id, Value> VecLinearData<Id, Value>
where
    Id: Clone + fmt::Debug + PartialEq + Eq + PartialOrd + Ord,
    Value: fmt::Debug,
{
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn append(&mut self, id: Id, value: Value) {
        let end_index = self.nodes.len() - 1;

        let end_node = &self.nodes[end_index];
        assert_matches!(end_node.operation, Operation::End);

        let last_index = end_index - 1;
        let last_node = &self.nodes[last_index];
        let last_node_id = last_node.id.clone();

        self.nodes.insert(
            end_index,
            Node {
                id,
                left_origin: Some(last_node_id),
                right_origin: Some(end_node.id.clone()),
                operation: Operation::Insert { value },
            },
        );
        self.len += 1;
    }

    pub fn prepend(&mut self, id: Id, value: Value) {
        let begin_node = &self.nodes[0];
        assert_matches!(begin_node.operation, Operation::Beginning);
        let begin_node_id = begin_node.id.clone();

        let first_node = &self.nodes[1];

        self.nodes.insert(
            1,
            Node {
                id,
                left_origin: Some(begin_node_id),
                right_origin: Some(first_node.id.clone()),
                operation: Operation::Insert { value },
            },
        );
        self.len += 1;
    }

    #[cfg(test)]
    pub(crate) fn check_integrity(&self) {
        let mut len = 0usize;
        for index in 0..self.nodes.len() {
            let current = &self.nodes[index];
            assert!(current.operation.is_valid());
            if index == 0 {
                assert!(matches!(current.operation, Operation::Beginning));
            } else if index == self.nodes.len() {
                assert!(matches!(current.operation, Operation::End));
            } else if matches!(current.operation, Operation::Insert { .. }) {
                len += 1;
            }
        }
        assert_eq!(self.len, len);
    }

    pub(super) fn iter_inserts(&self) -> impl Iterator<Item = (usize, &Node<Id, Value>)> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| matches!(n.operation, Operation::Insert { .. }))
    }

    pub(super) fn iter_inserts_from(
        &self,
        start_index: usize,
    ) -> impl Iterator<Item = (usize, &Node<Id, Value>)> {
        self.nodes[start_index..]
            .iter()
            .enumerate()
            .map(move |(index, node)| (start_index + index, node))
            .filter(|(_, n)| matches!(n.operation, Operation::Insert { .. }))
    }
}
impl<Id, Value> LinearData<Value> for VecLinearData<Id, Value>
where
    Id: Clone + fmt::Debug + PartialEq + Eq + PartialOrd + Ord + 'static,
    Value: fmt::Debug,
{
    type Id = Id;
    type Iter<'a>
        = VecLinearDataIter<'a, Id, Value>
    where
        Self: 'a,
        Value: 'a;

    fn ids_after_head(&self) -> LinkIds<Self::Id> {
        LinkIds {
            predecessor: self.nodes[0].id.clone(),
            successor: self.nodes[1].id.clone(),
        }
    }

    fn ids_before_end(&self) -> LinkIds<Self::Id> {
        let len = self.nodes.len();
        LinkIds {
            predecessor: self.nodes[len - 2].id.clone(),
            successor: self.nodes[len - 1].id.clone(),
        }
    }

    fn ids_at_pos(&self, position: usize) -> Option<NodeIds<Id>> {
        if position < self.len() {
            let index_at_position = self
                .iter_inserts()
                .nth(position)
                .map(|(index, _node)| index)
                .unwrap(); // This must exist in this branch.
            // All of these must exist if the list is valid.
            let predecessor = &self.nodes[index_at_position - 1];
            let current = &self.nodes[index_at_position];
            let successor = &self.nodes[index_at_position + 1];
            Some(NodeIds {
                predecessor: predecessor.id.clone(),
                current: current.id.clone(),
                successor: successor.id.clone(),
            })
        } else {
            None
        }
    }

    fn insert(&mut self, id: Id, pred: Id, succ: Id, value: Value) -> Result<(), Value> {
        self.apply_operation(DataOperation::Insert {
            id,
            pred,
            succ,
            value,
        })
        .map_err(|op| match op {
            DataOperation::Insert { value, .. } => value,
            _ => unreachable!(
                "apply_operation should not return a different operation type on error."
            ),
        })
    }

    fn delete(&mut self, id: &Self::Id) -> Option<&Value> {
        //println!("Trying to delete id={id:?} from: {:#?}", self.nodes);
        if let Some(node) = self.nodes.iter_mut().find(|n| &n.id == id) {
            match node.operation {
                Operation::Insert { .. } => {
                    node.operation.delete();
                    self.len -= 1;
                    if let Operation::Delete { ref value } = node.operation {
                        Some(value)
                    } else {
                        // We literally just put it there.
                        unreachable!()
                    }
                }
                // Double delete is OK.
                Operation::Delete { ref value } => Some(value),
                // These cannot be deleted.
                Operation::Beginning | Operation::End => {
                    //println!("Tried to delete Beginning/End");
                    None
                }
                Operation::Invalid => panic!("Node is invalid."),
            }
        } else {
            //println!("Could not find node.");
            None
        }
    }

    fn apply_operation(
        &mut self,
        operation: DataOperation<Self::Id, Value>,
    ) -> Result<(), DataOperation<Self::Id, Value>> {
        match operation {
            DataOperation::Insert {
                ref id,
                ref pred,
                ref succ,
                ..
            } => {
                let pred_index_opt = self
                    .nodes
                    .iter()
                    .enumerate()
                    .find_map(|(index, node)| option_when!(node.id == *pred, index));
                if let Some(pred_index) = pred_index_opt {
                    let succ_index_opt = self
                        .nodes
                        .iter()
                        .enumerate()
                        .skip(pred_index)
                        .find_map(|(index, node)| option_when!(node.id == *succ, index));
                    if let Some(succ_index) = succ_index_opt {
                        if pred_index + 1 == succ_index {
                            // We can insert directly at the existing boundary.
                            if let DataOperation::Insert {
                                id,
                                pred,
                                succ,
                                value,
                            } = operation
                            {
                                self.nodes.insert(
                                    succ_index,
                                    Node {
                                        id,
                                        left_origin: Some(pred),
                                        right_origin: Some(succ),
                                        operation: Operation::Insert { value },
                                    },
                                );
                                self.len += 1;
                                Ok(())
                            } else {
                                unreachable!("We *know* it's an Insert.");
                            }
                        } else if pred_index < succ_index {
                            // There is a gap between pred and succ that may contain concurrent inserts.
                            let left_right_range = (pred_index + 1)..succ_index;
                            let mut conflicting_nodes: Vec<(&Id, usize)> =
                                Vec::with_capacity(left_right_range.len());
                            // The right subtree is all nodes that have succ as successor,
                            // and all nodes that can reach those nodes by following right_origin.
                            let mut right_subtree_start_index_opt = None;
                            for node_index in left_right_range {
                                let node = &self.nodes[node_index];
                                if node.left_origin.as_ref() == Some(pred)
                                    && node.right_origin.as_ref() == Some(succ)
                                {
                                    conflicting_nodes.push((&node.id, node_index));
                                }
                                if right_subtree_start_index_opt.is_none()
                                    && self.ends_in_right_tree(node, succ)
                                {
                                    right_subtree_start_index_opt = Some(node_index);
                                }
                            }
                            let right_subtree_start_index =
                                right_subtree_start_index_opt.unwrap_or(succ_index);

                            let position = if conflicting_nodes.is_empty() {
                                right_subtree_start_index
                            } else {
                                debug_assert!(
                                    conflicting_nodes.is_sorted_by_key(|(one, _)| one),
                                    "Conflict range should already be sorted by id, but was: {conflicting_nodes:?}"
                                );
                                match conflicting_nodes
                                    .binary_search_by(|&(probe, _)| probe.cmp(id))
                                {
                                    Ok(_found_index) => {
                                        // Duplicate insert for the same conflict set.
                                        return Err(operation);
                                    }
                                    Err(insert_index) => {
                                        if insert_index == 0 {
                                            // Insert before the first conflicting node and its local subtree.
                                            pred_index + 1
                                        } else if insert_index < conflicting_nodes.len() {
                                            let (target_conflict_id, target_conflict_pos) =
                                                conflicting_nodes[insert_index];
                                            // Insert before the target conflicting node's local
                                            // subtree, not just before the node itself.
                                            // Otherwise the relative order of sibling subtrees can
                                            // depend on delivery order.
                                            ((pred_index + 1)..target_conflict_pos)
                                                .find(|node_index| {
                                                    let node = &self.nodes[*node_index];
                                                    self.ends_in_right_tree(
                                                        node,
                                                        target_conflict_id,
                                                    )
                                                })
                                                .unwrap_or(target_conflict_pos)
                                        } else {
                                            // Insert before succ, to the right of all conflicting nodes.
                                            succ_index
                                        }
                                    }
                                }
                            };

                            if let DataOperation::Insert {
                                id,
                                pred,
                                succ,
                                value,
                            } = operation
                            {
                                self.nodes.insert(
                                    position,
                                    Node {
                                        id,
                                        left_origin: Some(pred),
                                        right_origin: Some(succ),
                                        operation: Operation::Insert { value },
                                    },
                                );
                                self.len += 1;
                                Ok(())
                            } else {
                                unreachable!("We *know* it's an Insert.");
                            }
                        } else {
                            // Successor cannot appear before predecessor in a valid operation.
                            Err(operation)
                        }
                    } else {
                        Err(operation)
                    }
                } else {
                    Err(operation)
                }
            }
            DataOperation::Delete { ref start, ref end } => {
                // Ranges aren't supported in this impl.
                if end.is_some() {
                    return Err(operation);
                }
                self.delete(start).map(|_| ()).ok_or(operation)
            }
        }
    }

    fn iter_values(&self) -> Self::Iter<'_> {
        VecLinearDataIter {
            underlying: self.nodes.iter(),
        }
    }

    fn iter_ids(&self) -> impl Iterator<Item = &Self::Id> {
        self.nodes.iter().map(|n| &n.id)
    }
}

pub struct VecLinearDataIter<'a, Id, Value> {
    underlying: std::slice::Iter<'a, Node<Id, Value>>,
}
impl<'a, Id, Value> Iterator for VecLinearDataIter<'a, Id, Value> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next = self.underlying.next().map(Node::get_current_value);
        while let Some(None) = next {
            next = self.underlying.next().map(Node::get_current_value);
        }
        next.flatten()
    }
}
