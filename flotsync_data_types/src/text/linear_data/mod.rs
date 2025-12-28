use flotsync_utils::option_when;
use std::{assert_matches::assert_matches, fmt, vec};

mod coalesced;
pub use coalesced::{
    Composite,
    IdGeneratorWithZeroIndex,
    IdWithIndex,
    IdWithIndexRange,
    NodeIdRange,
    VecCoalescedLinearData,
    VecCoalescedLinearDataIter,
};
// TODO: Might or might not continue this, but don't build it for now.
//mod linked_list_impl;
mod vec_impl;
pub use vec_impl::VecLinearData;

#[derive(Clone, Debug, PartialEq)]
pub enum DataOperation<Id, Value> {
    Insert {
        id: Id,
        pred: Id,
        succ: Id,
        value: Value,
    },
    Delete {
        start: Id,
        end: Option<Id>,
    },
}
impl<Id, Value> DataOperation<Id, Value> {
    pub fn map_value<Output, F>(self, mapper: F) -> DataOperation<Id, Output>
    where
        F: FnOnce(Value) -> Output,
    {
        match self {
            DataOperation::Insert {
                id,
                pred,
                succ,
                value,
            } => DataOperation::Insert {
                id,
                pred,
                succ,
                value: mapper(value),
            },
            DataOperation::Delete { start, end } => DataOperation::Delete { start, end },
        }
    }
}

pub trait LinearData<Value, ValueRef = Value>
where
    ValueRef: ?Sized,
{
    type Id: 'static;

    type Iter<'a>: Iterator<Item = &'a ValueRef>
    where
        Self: 'a,
        ValueRef: 'a;

    /// Get the node ids between head and its successor.
    ///
    /// As opposed to [[ids_at_pos]], this always exists.
    fn ids_after_head(&self) -> LinkIds<Self::Id>;

    /// Get the node ids between the end and its predecessor.
    ///
    /// As opposed to [[ids_at_pos]], this always exists.
    fn ids_before_end(&self) -> LinkIds<Self::Id>;

    /// Get the ids of the nodes at, before, and after `position`.
    ///
    /// Position is counted only for currently existing Insert nodes, consistent with the iterator.
    ///
    /// In particular, that means empty `self` always returns `None`.
    fn ids_at_pos(&self, position: usize) -> Option<NodeIds<Self::Id>>;

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

    /// Deletes the value with `id` and returns a reference to it if successful.
    fn delete<'a>(&'a mut self, id: &Self::Id) -> Option<&'a ValueRef>;

    /// Apply a modification operation.
    ///
    /// May try to resolve a new position if the requested operation cannot exactly be applied due
    /// to a change in the structure since the location's ids were retrieved originally.
    ///
    /// Returns the original operation on failure.
    fn apply_operation(
        &mut self,
        operation: DataOperation<Self::Id, Value>,
    ) -> Result<(), DataOperation<Self::Id, Value>>;

    fn iter_values(&self) -> Self::Iter<'_>;

    /// Returns an iterator over all ids that are associated with some node in the underlying
    /// data structure.
    fn iter_ids(&self) -> impl Iterator<Item = &Self::Id>;
}

/// A pair of ids identifying a concrete position *between* two nodes at a particular point in time.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LinkIds<Id> {
    pub predecessor: Id,
    pub successor: Id,
}
impl<Id> LinkIds<Id> {
    pub fn insert<L, Value, ValueRef>(self, data: &mut L, id: Id, value: Value) -> Result<(), Value>
    where
        ValueRef: ?Sized,
        L: LinearData<Value, ValueRef, Id = Id>,
    {
        data.insert(id, self.predecessor, self.successor, value)
    }

    pub(crate) fn insert_operation<Value>(self, id: Id, value: Value) -> DataOperation<Id, Value>
    where
        Id: Clone + fmt::Debug + PartialEq + 'static,
    {
        DataOperation::Insert {
            id,
            pred: self.predecessor,
            succ: self.successor,
            value,
        }
    }
}

/// A group of ids identifying the concrete position of a node at a particular point in time.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeIds<Id> {
    pub predecessor: Id,
    pub current: Id,
    pub successor: Id,
}
impl<Id> NodeIds<Id> {
    /// Return a reference to the link between this node and its predecessor.
    pub fn before(self) -> LinkIds<Id> {
        LinkIds {
            predecessor: self.predecessor,
            successor: self.current,
        }
    }

    /// Return a reference to the link between this node and its successor.
    pub fn after(self) -> LinkIds<Id> {
        LinkIds {
            predecessor: self.current,
            successor: self.successor,
        }
    }

    pub fn insert_before<L, Value, ValueRef>(
        self,
        data: &mut L,
        id: Id,
        value: Value,
    ) -> Result<(), Value>
    where
        ValueRef: ?Sized,
        L: LinearData<Value, ValueRef, Id = Id>,
    {
        self.before().insert(data, id, value)
    }

    pub fn insert_after<L, Value, ValueRef>(
        self,
        data: &mut L,
        id: Id,
        value: Value,
    ) -> Result<(), Value>
    where
        ValueRef: ?Sized,
        L: LinearData<Value, ValueRef, Id = Id>,
    {
        self.after().insert(data, id, value)
    }

    pub fn delete<'a, L, Value, ValueRef>(&self, data: &'a mut L) -> Option<&'a ValueRef>
    where
        ValueRef: ?Sized,
        L: LinearData<Value, ValueRef, Id = Id>,
    {
        data.delete(&self.current)
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

#[derive(Clone, Debug, PartialEq)]
struct Node<Id, Value> {
    id: Id,
    left_origin: Option<Id>,
    right_origin: Option<Id>,
    operation: Operation<Value>,
}
impl<Id, Value> Node<Id, Value> {
    pub fn get_current_value(&self) -> Option<&Value> {
        match self.operation {
            Operation::Insert { ref value } => Some(value),
            Operation::Delete { .. } => None,
            Operation::Beginning => None,
            Operation::End => None,
            Operation::Invalid => panic!("Node is invalid."),
        }
    }
}
impl<Id, Value> Node<IdWithIndex<Id>, Value>
where
    Id: Clone + PartialEq,
    Value: Composite,
{
    pub fn contains(&self, id: &IdWithIndex<Id>) -> bool {
        self.id.id == id.id
            && match self.operation {
                Operation::Insert { .. } | Operation::Delete { .. } => {
                    self.id.index <= id.index && id.index <= self.last_index()
                }
                Operation::Beginning | Operation::End => self.id.index == id.index,
                Operation::Invalid => panic!("Node is invalid."),
            }
    }

    /// Get the size of the current value, if any.
    pub fn get_len(&self) -> Option<usize> {
        self.get_current_value().map(|v| v.len())
    }

    /// Gets the size of the node's value.
    ///
    /// Zero-sized nodes (e.g. begin/end) return 0.
    pub fn node_len(&self) -> usize {
        match self.operation {
            Operation::Insert { ref value } => value.len(),
            Operation::Delete { ref value } => value.len(),
            Operation::Beginning => 0,
            Operation::End => 0,
            Operation::Invalid => panic!("Node is invalid."),
        }
    }

    pub fn last_index(&self) -> u16 {
        let node_len = self.node_len();
        if node_len == 0 || node_len == 1 {
            self.id.index
        } else {
            let last_offset: u16 = (node_len - 1)
                .try_into()
                .expect("Nodes must not be longer than can be addressed");
            self.id
                .index
                .checked_add(last_offset)
                .expect("Nodes must not be longer than can be addressed")
        }
    }

    pub fn last_id(&self) -> IdWithIndex<Id> {
        let mut id = self.id.clone();
        id.index = self.last_index();
        id
    }

    #[allow(unused, reason = "Used in testing atm.")]
    pub fn ids(&self) -> impl Iterator<Item = IdWithIndex<Id>> {
        (self.id.index..=self.last_index()).map(|index| IdWithIndex {
            id: self.id.id.clone(),
            index,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Operation<Value> {
    Insert {
        value: Value,
    },
    Delete {
        value: Value,
    },
    Beginning,
    End,
    /// Used as a temp value while replacing nodes.
    Invalid,
}
impl<Value> Operation<Value> {
    #[allow(unused)]
    pub fn is_valid(&self) -> bool {
        !matches!(self, Operation::Invalid)
    }

    // Temporarily replace the operation, so we own the value.
    fn take_value(&mut self) -> Option<Value> {
        match std::mem::replace(self, Operation::Invalid) {
            Operation::Insert { value } => Some(value),
            Operation::Delete { value } => Some(value),
            Operation::Beginning | Operation::End | Operation::Invalid => None,
        }
    }
}
impl<Value> Operation<Value>
where
    Value: fmt::Debug,
{
    pub fn delete(&mut self) {
        assert_matches!(
            self,
            Operation::Insert { .. },
            "Cannot delete non-insert node."
        );
        let value = self.take_value().unwrap();
        *self = Operation::Delete { value };
    }
}

impl<Value> Operation<Value>
where
    Value: Composite,
{
    /// Split this operation off at the given element index into two operations.
    ///
    /// The the element at `split_index` begins the second operation.
    ///
    /// Only supported for Insert/Delete with values of sufficient length.
    /// Returns `None` if unsupported.
    fn split_off(&mut self, at: u16) -> Option<Self> {
        let is_insert = match self {
            Operation::Insert { .. } => true,
            Operation::Delete { .. } => false,
            Operation::Beginning | Operation::End | Operation::Invalid => return None,
        };
        let value = self.take_value().unwrap();
        let (remaining_self_value, new_value) = value.split_at(at as usize);
        let new_operation = if is_insert {
            *self = Operation::Insert {
                value: remaining_self_value,
            };
            Operation::Insert { value: new_value }
        } else {
            *self = Operation::Delete {
                value: remaining_self_value,
            };
            Operation::Delete { value: new_value }
        };
        Some(new_operation)
    }
}
