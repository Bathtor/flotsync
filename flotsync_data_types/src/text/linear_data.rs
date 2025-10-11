use std::{
    assert_matches::assert_matches,
    collections::{LinkedList, linked_list},
    fmt,
    iter::FlatMap,
    vec,
};

use flotsync_utils::option_when;

pub trait LinearData<Value> {
    type Id;

    type Iter<'a>: Iterator<Item = &'a Value>
    where
        Self: 'a,
        Value: 'a;

    /// Get the ids of the nodes at, before, and after `position`.
    ///
    /// Position is counted only for currently existing Insert nodes, consistent with the iterator.
    fn ids_at_pos(&self, position: usize) -> Option<NodeIds<&Self::Id>>;

    /// Insert `id -> value` between `pred` and `succ` if these nodes exist and `succ`
    /// is the current predecessor of `pred`.
    /// Otherwise return `value`.
    fn insert(
        &mut self,
        id: Self::Id,
        pred: Self::Id,
        succ: Self::Id,
        value: Value,
    ) -> Result<(), Value>;

    fn iter_values(&self) -> Self::Iter<'_>;
}

/// An implementation of [[LinearData]] using a [[Vec]] to track the individual operation nodes.
///
/// # Note
/// While the natural representation of this data structure is linked nodes,
/// storing them in a Vec is likely more efficient in practice for most usages
/// (e.g. read-mostly strings).
#[derive(Clone, Debug)]
pub struct VecLinearData<Id, Value> {
    nodes: Vec<Node<Id, Option<Id>, Value>>,
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
            predecessor: None,
            successor: Some(end_id.clone()),
            origin: None,
            operation: Operation::Beginning,
        };
        let end_node = Node {
            id: end_id,
            predecessor: Some(begin_id.clone()),
            successor: None,
            origin: Some(begin_id),
            operation: Operation::End,
        };
        let nodes = vec![begin_node, end_node];
        Self { nodes }
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
            predecessor: None,
            successor: Some(value_id.clone()),
            origin: None,
            operation: Operation::Beginning,
        };
        let value_node = Node {
            id: begin_id.clone(),
            predecessor: Some(begin_id.clone()),
            successor: Some(end_id.clone()),
            origin: Some(begin_id.clone()),
            operation: Operation::Insert {
                value: initial_value,
            },
        };
        let end_node = Node {
            id: end_id,
            predecessor: Some(value_id.clone()),
            successor: None,
            origin: Some(value_id.clone()),
            operation: Operation::End,
        };
        let nodes = vec![begin_node, value_node, end_node];
        Self { nodes }
    }
}
impl<Id, Value> VecLinearData<Id, Value>
where
    Id: Clone + fmt::Debug + PartialEq,
    Value: fmt::Debug,
{
    pub fn len(&self) -> usize {
        let underlying_len = self.nodes.len();
        assert!(
            underlying_len >= 2,
            "Invalid list. Missing Beginning or End node: {:#?}",
            self.nodes
        );
        underlying_len - 2
    }

    pub fn append(&mut self, id: Id, value: Value) {
        let end_index = self.nodes.len() - 1;

        let end_node = &mut self.nodes[end_index];
        assert_matches!(end_node.operation, Operation::End);
        end_node.predecessor = Some(id.clone());
        let end_node_id = end_node.id.clone();

        let last_index = end_index - 1;
        let last_node = &mut self.nodes[last_index];
        last_node.successor = Some(id.clone());
        let last_node_id = last_node.id.clone();

        self.nodes.insert(
            end_index,
            Node {
                id,
                predecessor: Some(last_node_id.clone()),
                successor: Some(end_node_id),
                origin: Some(last_node_id),
                operation: Operation::Insert { value },
            },
        );
    }

    pub fn prepend(&mut self, id: Id, value: Value) {
        let mut current_index = 0usize;

        let begin_node = &mut self.nodes[current_index];
        assert_matches!(begin_node.operation, Operation::Beginning);
        begin_node.successor = Some(id.clone());
        let begin_node_id = begin_node.id.clone();

        current_index += 1;
        let first_node = &mut self.nodes[current_index];
        first_node.predecessor = Some(id.clone());
        let first_node_id = first_node.id.clone();

        self.nodes.insert(
            current_index,
            Node {
                id,
                predecessor: Some(begin_node_id.clone()),
                successor: Some(first_node_id),
                origin: Some(begin_node_id),
                operation: Operation::Insert { value },
            },
        );
    }

    pub(crate) fn check_integrity(&self) {
        let mut index = 0usize;
        for index in 0..self.nodes.len() {
            let current = &self.nodes[index];
            if index > 0 {
                let previous = &self.nodes[index - 1];
                assert_eq!(previous.successor.as_ref(), Some(&current.id));
                assert_eq!(current.predecessor.as_ref(), Some(&previous.id));
            } else {
                assert_eq!(current.predecessor, None);
                assert!(matches!(current.operation, Operation::Beginning));
            }
            if (index + 1) < self.nodes.len() {
                let next = &self.nodes[index + 1];
                assert_eq!(next.predecessor.as_ref(), Some(&current.id));
                assert_eq!(current.successor.as_ref(), Some(&next.id));
            } else {
                assert_eq!(current.successor, None);
                assert!(matches!(current.operation, Operation::End));
            }
        }
    }
}
impl<Id, Value> LinearData<Value> for VecLinearData<Id, Value>
where
    Id: Clone + fmt::Debug + PartialEq,
    Value: fmt::Debug,
{
    type Id = Id;
    type Iter<'a>
        = VecLinearDataIter<'a, Id, Value>
    where
        Self: 'a,
        Value: 'a;

    fn ids_at_pos(&self, position: usize) -> Option<NodeIds<&Id>> {
        if position < self.len() {
            let mut it = self.nodes.iter().skip(position);
            // All of these must exist if the list is valid, which `len` checks.
            let predecessor = it.next().unwrap();
            let current = it.next().unwrap();
            let successor = it.next().unwrap();
            Some(NodeIds {
                predecessor: &predecessor.id,
                current: &current.id,
                successor: &successor.id,
            })
        } else {
            None
        }
    }

    fn insert(&mut self, id: Id, pred: Id, succ: Id, value: Value) -> Result<(), Value> {
        let pred_index_opt = self
            .nodes
            .iter()
            .enumerate()
            .find_map(|(index, node)| option_when!(node.id == pred, index));
        if let Some(pred_index) = pred_index_opt {
            let succ_index = pred_index + 1;
            if let Some(succ_node) = self.nodes.get_mut(succ_index) {
                if succ_node.id == succ {
                    succ_node.predecessor = Some(id.clone());
                    // We already checked above that this index exists.
                    let pred_node = &mut self.nodes[pred_index];
                    pred_node.successor = Some(id.clone());

                    self.nodes.insert(
                        succ_index,
                        Node {
                            id,
                            predecessor: Some(pred.clone()),
                            successor: Some(succ),
                            origin: Some(pred),
                            operation: Operation::Insert { value },
                        },
                    );
                    Ok(())
                } else {
                    Err(value)
                }
            } else {
                Err(value)
            }
        } else {
            Err(value)
        }
    }

    fn iter_values(&self) -> Self::Iter<'_> {
        VecLinearDataIter {
            underlying: self.nodes.iter(),
        }
    }
}

pub struct VecLinearDataIter<'a, Id, Value> {
    underlying: std::slice::Iter<'a, Node<Id, Option<Id>, Value>>,
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

/// A variant of an operation id that allows multiple ordered operations at the same time,
/// without having to do explictly different operations for each.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct IdWithIndex<Id> {
    pub id: Id,
    pub index: u16, // Probably sufficient for a single operation.
}
impl<Id> IdWithIndex<Id>
where
    Id: Clone,
{
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeIds<Id> {
    pub predecessor: Id,
    pub current: Id,
    pub successor: Id,
}
impl<Id> NodeIds<Id> {
    pub fn insert_before<L, Value>(self, data: &mut L, id: Id, value: Value) -> Result<(), Value>
    where
        L: LinearData<Value, Id = Id>,
    {
        data.insert(id, self.predecessor, self.current, value)
    }

    pub fn insert_after<L, Value>(self, data: &mut L, id: Id, value: Value) -> Result<(), Value>
    where
        L: LinearData<Value, Id = Id>,
    {
        data.insert(id, self.current, self.successor, value)
    }
}
impl<Id> NodeIds<&'_ Id>
where
    Id: Clone,
{
    pub fn cloned(&self) -> NodeIds<Id> {
        NodeIds {
            predecessor: self.predecessor.clone(),
            current: self.current.clone(),
            successor: self.successor.clone(),
        }
    }
}

// pub struct LinkedLinearData<Id, Value> {
//     nodes: LinkedList<Node<IdWithIndex<Id>, Option<IdWithIndex<Id>>, Value>>,
// }
// impl<Id, Value> LinkedLinearData<Id, Value>
// where
//     Id: Clone + fmt::Debug + PartialEq,
//     Value: fmt::Debug,
// {
//     pub fn new(initial_id: Id) -> Self {
//         let begin_id = IdWithIndex {
//             id: initial_id,
//             index: 0,
//         };
//         let end_id = begin_id.increment();
//         let begin_node = Node {
//             id: begin_id.clone(),
//             predecessor: None,
//             successor: Some(end_id.clone()),
//             origin: None,
//             operation: Operation::Beginning,
//         };
//         let end_node = Node {
//             id: end_id,
//             predecessor: Some(begin_id.clone()),
//             successor: None,
//             origin: Some(begin_id),
//             operation: Operation::End,
//         };
//         let nodes = LinkedList::from([begin_node, end_node]);
//         Self { nodes }
//     }

//     pub fn with_value(initial_id: Id, initial_value: Value) -> LinkedLinearData<Id, Value> {
//         let begin_id = IdWithIndex {
//             id: initial_id,
//             index: 0,
//         };
//         let value_id = begin_id.increment();
//         let end_id = begin_id.increment();
//         let begin_node = Node {
//             id: begin_id.clone(),
//             predecessor: None,
//             successor: Some(value_id.clone()),
//             origin: None,
//             operation: Operation::Beginning,
//         };
//         let value_node = Node {
//             id: begin_id.clone(),
//             predecessor: Some(begin_id.clone()),
//             successor: Some(end_id.clone()),
//             origin: Some(begin_id.clone()),
//             operation: Operation::Insert {
//                 value: initial_value,
//             },
//         };
//         let end_node = Node {
//             id: end_id,
//             predecessor: Some(value_id.clone()),
//             successor: None,
//             origin: Some(value_id.clone()),
//             operation: Operation::End,
//         };
//         let nodes = LinkedList::from([begin_node, value_node, end_node]);
//         Self { nodes }
//     }

//     /// Insert `id -> value` between `pred` and `succ` if these nodes exist and `succ`
//     /// is the current predecessor of `pred`.
//     /// Otherwise return `value`.
//     pub fn insert(
//         &mut self,
//         id: IdWithIndex<Id>,
//         pred: IdWithIndex<Id>,
//         succ: IdWithIndex<Id>,
//         value: Value,
//     ) -> Result<(), Value> {
//         let mut cursor = self.nodes.cursor_front_mut();
//         while let Some(node) = cursor.current() {
//             if node.id == pred && node.successor.as_ref() == Some(&succ) {
//                 node.successor = Some(id.clone());
//                 cursor.insert_after(Node {
//                     id: id.clone(),
//                     predecessor: Some(pred.clone()),
//                     successor: Some(succ),
//                     origin: Some(pred),
//                     operation: Operation::Insert { value },
//                 });
//                 cursor.move_next();
//                 if let Some(node) = cursor.current() {
//                     assert_ne!(node.id, id);
//                     node.predecessor = Some(id);
//                 }
//                 return Ok(());
//             } else {
//                 cursor.move_next();
//             }
//         }
//         Err(value)
//     }

//     pub fn append(&mut self, id: IdWithIndex<Id>, value: Value) {
//         let mut cursor = self.nodes.cursor_back_mut();

//         let end_node = cursor.current().expect("Should always have a final node");
//         assert_matches!(end_node.operation, Operation::End);
//         end_node.predecessor = Some(id.clone());
//         let end_node_id = end_node.id.clone();

//         cursor.move_prev();
//         let last_node = cursor.current().expect("Should always have a last node");
//         last_node.successor = Some(id.clone());
//         let last_node_id = last_node.id.clone();

//         cursor.insert_after(Node {
//             id,
//             predecessor: Some(last_node_id.clone()),
//             successor: Some(end_node_id),
//             origin: Some(last_node_id),
//             operation: Operation::Insert { value },
//         });
//     }

//     pub fn prepend(&mut self, id: IdWithIndex<Id>, value: Value) {
//         let mut cursor = self.nodes.cursor_front_mut();

//         let begin_node = cursor.current().expect("Should always have a begin node");
//         assert_matches!(begin_node.operation, Operation::Beginning);
//         begin_node.successor = Some(id.clone());
//         let begin_node_id = begin_node.id.clone();

//         cursor.move_next();
//         let first_node = cursor.current().expect("Should always have a first node");
//         first_node.predecessor = Some(id.clone());
//         let first_node_id = first_node.id.clone();

//         cursor.insert_after(Node {
//             id,
//             predecessor: Some(begin_node_id.clone()),
//             successor: Some(first_node_id),
//             origin: Some(begin_node_id),
//             operation: Operation::Insert { value },
//         });
//     }

//     pub fn ids_at_pos(&self, pos: usize) -> Option<NodeIds<Id>> {
//         let mut past_nodes_chars = 0usize;
//         let mut iter = self.nodes.iter();
//         todo!(
//             "Not every type will have a length. So the whole IdWithIndex thingy should be a wrapper for types with a length."
//         );
//         // while let Some(node) = iter.next() {
//         //     if let Some(value) = node.get_current_value() {
//         //         let end = past_nodes_chars + value.
//         //     }
//         // }
//     }
// }
// impl<Id, Value> LinearData<Value> for LinkedLinearData<Id, Value>
// where
//     Id: Clone,
// {
//     type Id = Id;
//     type Iter<'a>
//         = LinkedLinearDataIter<'a, Id, Value>
//     where
//         Self: 'a,
//         Value: 'a;

//     fn iter_values(&self) -> Self::Iter<'_> {
//         LinkedLinearDataIter {
//             underlying: self.nodes.iter(),
//         }
//     }
// }

// pub struct LinkedLinearDataIter<'a, Id, Value> {
//     underlying: linked_list::Iter<'a, Node<IdWithIndex<Id>, Option<IdWithIndex<Id>>, Value>>,
// }
// impl<'a, Id, Value> Iterator for LinkedLinearDataIter<'a, Id, Value> {
//     type Item = &'a Value;

//     fn next(&mut self) -> Option<Self::Item> {
//         let mut next = self.underlying.next().map(Node::get_current_value);
//         while let Some(None) = next {
//             next = self.underlying.next().map(Node::get_current_value);
//         }
//         next.flatten()
//     }
// }

#[derive(Clone, Debug)]
pub struct Node<Id, Link, Value> {
    id: Id,
    predecessor: Link,
    successor: Link,
    origin: Link,
    operation: Operation<Value>,
}
impl<Id, Link, Value> Node<Id, Link, Value> {
    pub fn get_current_value(&self) -> Option<&Value> {
        match self.operation {
            Operation::Insert { ref value } => Some(value),
            Operation::Delete { .. } => None,
            Operation::Beginning => None,
            Operation::End => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Operation<Value> {
    Insert {
        value: Value,
    },
    Delete {
        // TODO: It's unclear if we will actually need this,
        //       but the paper suggests that deleted nodes retain their values.
        value: Value,
    },
    Beginning,
    End,
}
