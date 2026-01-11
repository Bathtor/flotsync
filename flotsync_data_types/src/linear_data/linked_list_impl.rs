use super::*;
use std::collections::LinkedList;

pub struct LinkedLinearData<Id, Value> {
    nodes: LinkedList<Node<IdWithIndex<Id>, Option<IdWithIndex<Id>>, Value>>,
}
impl<Id, Value> LinkedLinearData<Id, Value>
where
    Id: Clone + fmt::Debug + PartialEq,
    Value: fmt::Debug,
{
    pub fn new(initial_id: Id) -> Self {
        let begin_id = IdWithIndex {
            id: initial_id,
            index: 0,
        };
        let end_id = begin_id.increment();
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
        let nodes = LinkedList::from([begin_node, end_node]);
        Self { nodes }
    }

    pub fn with_value(initial_id: Id, initial_value: Value) -> LinkedLinearData<Id, Value> {
        let begin_id = IdWithIndex {
            id: initial_id,
            index: 0,
        };
        let value_id = begin_id.increment();
        let end_id = begin_id.increment();
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
        let nodes = LinkedList::from([begin_node, value_node, end_node]);
        Self { nodes }
    }

    /// Insert `id -> value` between `pred` and `succ` if these nodes exist and `succ`
    /// is the current predecessor of `pred`.
    /// Otherwise return `value`.
    pub fn insert(
        &mut self,
        id: IdWithIndex<Id>,
        pred: IdWithIndex<Id>,
        succ: IdWithIndex<Id>,
        value: Value,
    ) -> Result<(), Value> {
        let mut cursor = self.nodes.cursor_front_mut();
        while let Some(node) = cursor.current() {
            if node.id == pred && node.successor.as_ref() == Some(&succ) {
                node.successor = Some(id.clone());
                cursor.insert_after(Node {
                    id: id.clone(),
                    predecessor: Some(pred.clone()),
                    successor: Some(succ),
                    origin: Some(pred),
                    operation: Operation::Insert { value },
                });
                cursor.move_next();
                if let Some(node) = cursor.current() {
                    assert_ne!(node.id, id);
                    node.predecessor = Some(id);
                }
                return Ok(());
            } else {
                cursor.move_next();
            }
        }
        Err(value)
    }

    pub fn append(&mut self, id: IdWithIndex<Id>, value: Value) {
        let mut cursor = self.nodes.cursor_back_mut();

        let end_node = cursor.current().expect("Should always have a final node");
        assert_matches!(end_node.operation, Operation::End);
        end_node.predecessor = Some(id.clone());
        let end_node_id = end_node.id.clone();

        cursor.move_prev();
        let last_node = cursor.current().expect("Should always have a last node");
        last_node.successor = Some(id.clone());
        let last_node_id = last_node.id.clone();

        cursor.insert_after(Node {
            id,
            predecessor: Some(last_node_id.clone()),
            successor: Some(end_node_id),
            origin: Some(last_node_id),
            operation: Operation::Insert { value },
        });
    }

    pub fn prepend(&mut self, id: IdWithIndex<Id>, value: Value) {
        let mut cursor = self.nodes.cursor_front_mut();

        let begin_node = cursor.current().expect("Should always have a begin node");
        assert_matches!(begin_node.operation, Operation::Beginning);
        begin_node.successor = Some(id.clone());
        let begin_node_id = begin_node.id.clone();

        cursor.move_next();
        let first_node = cursor.current().expect("Should always have a first node");
        first_node.predecessor = Some(id.clone());
        let first_node_id = first_node.id.clone();

        cursor.insert_after(Node {
            id,
            predecessor: Some(begin_node_id.clone()),
            successor: Some(first_node_id),
            origin: Some(begin_node_id),
            operation: Operation::Insert { value },
        });
    }

    pub fn ids_at_pos(&self, pos: usize) -> Option<NodeIds<Id>> {
        let mut past_nodes_chars = 0usize;
        let mut iter = self.nodes.iter();
        todo!(
            "Not every type will have a length. So the whole IdWithIndex thingy should be a wrapper for types with a length."
        );
        // while let Some(node) = iter.next() {
        //     if let Some(value) = node.get_current_value() {
        //         let end = past_nodes_chars + value.
        //     }
        // }
    }
}
impl<Id, Value> LinearData<Value> for LinkedLinearData<Id, Value>
where
    Id: Clone,
{
    type Id = Id;
    type Iter<'a>
        = LinkedLinearDataIter<'a, Id, Value>
    where
        Self: 'a,
        Value: 'a;

    fn iter_values(&self) -> Self::Iter<'_> {
        LinkedLinearDataIter {
            underlying: self.nodes.iter(),
        }
    }
}

pub struct LinkedLinearDataIter<'a, Id, Value> {
    underlying: linked_list::Iter<'a, Node<IdWithIndex<Id>, Option<IdWithIndex<Id>>, Value>>,
}
impl<'a, Id, Value> Iterator for LinkedLinearDataIter<'a, Id, Value> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next = self.underlying.next().map(Node::get_current_value);
        while let Some(None) = next {
            next = self.underlying.next().map(Node::get_current_value);
        }
        next.flatten()
    }
}
