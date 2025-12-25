use super::*;

/// An implementation of [[LinearData]] using a [[Vec]] to track the individual operation nodes.
///
/// # Note
/// While the natural representation of this data structure is linked nodes,
/// storing them in a Vec is likely more efficient in practice for most usages
/// (e.g. read-mostly strings).
#[derive(Clone, Debug)]
pub struct VecLinearData<Id, Value> {
    /// The number of Insert nodes in the linear data.
    pub(super) len: usize,
    pub(super) nodes: Vec<Node<Id, Value>>,
}
impl<Id, Value> VecLinearData<Id, Value>
where
    Id: Clone,
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
            origin: None,
            operation: Operation::Beginning,
        };
        let end_node = Node {
            id: end_id,
            origin: Some(begin_id),
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
            origin: None,
            operation: Operation::Beginning,
        };
        let value_node = Node {
            id: begin_id.clone(),
            origin: Some(begin_id.clone()),
            operation: Operation::Insert {
                value: initial_value,
            },
        };
        let end_node = Node {
            id: end_id,
            origin: Some(value_id.clone()),
            operation: Operation::End,
        };
        let nodes = vec![begin_node, value_node, end_node];
        Self { len: 1, nodes }
    }
}
impl<Id, Value> VecLinearData<Id, Value>
where
    Id: Clone + fmt::Debug + PartialEq,
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
                origin: Some(last_node_id),
                operation: Operation::Insert { value },
            },
        );
        self.len += 1;
    }

    pub fn prepend(&mut self, id: Id, value: Value) {
        let begin_node = &self.nodes[0];
        assert_matches!(begin_node.operation, Operation::Beginning);
        let begin_node_id = begin_node.id.clone();

        self.nodes.insert(
            1,
            Node {
                id,
                origin: Some(begin_node_id),
                operation: Operation::Insert { value },
            },
        );
        self.len += 1;
    }

    #[cfg(test)]
    pub(in crate::text) fn check_integrity(&self) {
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
    Id: Clone + fmt::Debug + PartialEq + 'static,
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
                ref pred, ref succ, ..
            } => {
                let pred_index_opt = self
                    .nodes
                    .iter()
                    .enumerate()
                    .find_map(|(index, node)| option_when!(node.id == *pred, index));
                if let Some(pred_index) = pred_index_opt {
                    let succ_index = pred_index + 1;
                    if let Some(succ_node) = self.nodes.get(succ_index) {
                        if succ_node.id == *succ {
                            // Now we don't need the original operation anymore, so we can properly deconstruct it.
                            if let DataOperation::Insert {
                                id, pred, value, ..
                            } = operation
                            {
                                self.nodes.insert(
                                    succ_index,
                                    Node {
                                        id,
                                        origin: Some(pred),
                                        operation: Operation::Insert { value },
                                    },
                                );
                                self.len += 1;
                                Ok(())
                            } else {
                                unreachable!("We *know* it's an Insert.");
                            }
                        } else {
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
