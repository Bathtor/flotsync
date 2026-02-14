use crate::linear_data::{DataOperation, LinearData, VecLinearData};
use std::{fmt, hash::Hash};

/// A single-slot *latest value wins* register with Yjs `ReplaceManager` semantics.
///
/// `LinearLatestValueWins` models one logical value. Each
/// assignment publishes a new candidate value, and the register’s visible value is the
/// deterministically selected “winning” candidate after considering concurrency.
///
/// Conceptually, every write creates a new *version node* identified by `Id` and placed into
/// a single convergent linear order using the same Yjs-style insertion rules as other
/// `VecLinearData`-based types. The register’s value is then derived from that ordered set.
///
/// ## Semantics
///
/// - **Write:** publishing a new value `T` creates a new version node.
/// - **Visible value:** the register evaluates the set of version nodes and returns the
///   value of the single node that is considered *current* under the ReplaceManager rule.
/// - **Concurrency:** if multiple writes are concurrent, all replicas still choose the same
///   current node deterministically (no reliance on wall-clock time), if they have seen the same
///   set of published values (in any order).
///
/// ## Guarantees
///
/// - **Convergence:** given the same set of writes, all replicas compute the same visible value,
///   independent of delivery order.
/// - **Determinism under concurrency:** concurrent writes resolve to a single winner via the
///   same ordering/tie-break rules used by the underlying linear CRDT.
/// - Insertion order must satisfy causality. That is, if a client A has seen update 5 from client,
///   B before producing its own update 6, then A:5 must be applied before applying the diff for B:6
///   at any other node.
///
/// ## Identifier requirements
///
/// `Id` must uniquely identify each write and provide (directly or indirectly) a deterministic
/// ordering used to break ties between concurrent writes, so that all replicas pick the same
/// winner.
///
/// ## Notes
///
/// This type intentionally does **not** encode “real time” recency. The winning write is the
/// one that is *latest in the convergent Yjs order*, which is a deterministic function of the
/// set of operations, not of wall-clock timestamps.
#[derive(Clone, Debug, PartialEq)]
pub struct LinearLatestValueWins<Id, T> {
    data: VecLinearData<Id, T>,
}
impl<Id, T> LinearLatestValueWins<Id, T>
where
    Id: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    T: Clone + fmt::Debug,
{
    pub fn new<I>(id_generator: &mut I, initial_value: T) -> Self
    where
        I: Iterator<Item = Id>,
    {
        let data = VecLinearData::with_value(id_generator, initial_value);
        Self { data }
    }

    /// Returns the current value of this CRDT.
    pub fn content(&self) -> &T {
        self.data
            .iter_values()
            .next()
            .expect("Empty states are not allowed.")
    }

    /// Update the current value of this CRDT to `new_value`.
    pub fn update(&mut self, id: Id, new_value: T) {
        let op = self.update_operation(id, new_value);
        self.apply_operation(op)
            .expect("Direct updates must succeed.");
    }

    /// Produce an operation that can be sent to other replicas and represents an attempt to
    /// update the current value of this CRDT to `new_value.`
    /// Depending on concurrent updates, `new_value` may never be the `current` value at some
    /// replicas.
    pub fn update_operation(&self, id: Id, new_value: T) -> UpdateOperation<Id, T> {
        let ids = self.data.ids_after_head();
        let data_op = ids.insert_operation(id, new_value);
        UpdateOperation { op: data_op }
    }

    /// Apply an update operation received from some replica (including ourselves).
    pub fn apply_operation(
        &mut self,
        operation: UpdateOperation<Id, T>,
    ) -> Result<(), UpdateOperation<Id, T>> {
        self.data
            .apply_operation(operation.op)
            .map_err(|op| UpdateOperation { op })
    }

    /// Returns all values that we at some point part of this CRDT.
    ///
    /// Conceptually they are returned newest to oldest, accounting for concurrency.
    pub fn all_values(&self) -> impl Iterator<Item = &T> {
        self.data.iter_values()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateOperation<Id, T> {
    op: DataOperation<Id, T>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::linear_data::tests::TestIdGenerator;

    type Id = u32;

    fn new_reg(initial: u64) -> LinearLatestValueWins<Id, u64> {
        let mut id_generator = TestIdGenerator::new();
        LinearLatestValueWins::new(&mut id_generator, initial)
    }

    #[test]
    fn content_is_initial_value() {
        let reg = new_reg(42);
        assert_eq!(*reg.content(), 42);
    }

    #[test]
    fn local_update_changes_content_and_tracks_history() {
        let mut reg = new_reg(0);

        reg.update(3, 10);
        assert_eq!(*reg.content(), 10);

        reg.update(4, 11);
        assert_eq!(*reg.content(), 11);

        // Newest to oldest.
        let vals: Vec<u64> = reg.all_values().copied().collect();
        assert_eq!(vals, vec![11, 10, 0]);
    }

    #[test]
    fn roundtrip_operation_applies_on_other_replica() {
        let base = new_reg(0);
        let mut a = base.clone();
        let mut b = base;

        let op = a.update_operation(3, 7);
        a.apply_operation(op.clone()).unwrap();

        assert_eq!(*a.content(), 7);
        assert_eq!(*b.content(), 0);

        b.apply_operation(op).unwrap();
        assert_eq!(*b.content(), 7);

        // Histories should match once both have seen the same set of ops.
        let a_vals: Vec<u64> = a.all_values().copied().collect();
        let b_vals: Vec<u64> = b.all_values().copied().collect();
        assert_eq!(a_vals, b_vals);
    }

    #[test]
    fn concurrent_updates_converge_independent_of_delivery_order() {
        let base = new_reg(0);
        let a0 = base.clone();
        let b0 = base;

        // Two replicas create concurrent updates from the same base state.
        let op_a = a0.update_operation(3, 10);
        let op_b = b0.update_operation(4, 20);

        // Apply in opposite orders on two replicas.
        let mut r1 = new_reg(0);
        r1.apply_operation(op_a.clone()).unwrap();
        r1.apply_operation(op_b.clone()).unwrap();

        let mut r2 = new_reg(0);
        r2.apply_operation(op_b).unwrap();
        r2.apply_operation(op_a).unwrap();

        // Same final visible value and same history ordering.
        assert_eq!(r1, r2);

        // With `u32` ids, conflicting inserts are ordered by `Ord` on id.
        // 3 < 4, so the value at id=3 appears first and wins.
        assert_eq!(*r1.content(), 10);

        let vals: Vec<u64> = r1.all_values().copied().collect();
        assert_eq!(vals, vec![10, 20, 0]);
    }

    #[test]
    fn applying_same_operation_twice_is_illegal() {
        let base = new_reg(0);
        let op = base.update_operation(3, 10);

        let mut reg = new_reg(0);
        reg.apply_operation(op.clone()).unwrap();

        // Re-applying the same op should be illegal.
        let res = reg.apply_operation(op);
        assert!(res.is_err());
    }
}
