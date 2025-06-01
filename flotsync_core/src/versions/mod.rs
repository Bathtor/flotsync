//! The [[VersionVector]] is a variant of a [Version Vector](https://en.wikipedia.org/wiki/Version_vector).
//! It has entries for the local version at each group member, which may however be collapsed to save space when they are all the same.

mod happened_before;
pub use happened_before::*;
mod flat_vector;
pub use flat_vector::*;
mod group_vector;
pub use group_vector::*;

#[cfg(test)]
mod tests {
    use crate::member::Identifier;

    use super::*;
    use proptest::{prelude::*, strategy::Union};
    use std::num::NonZeroUsize;

    const LARGE_VERSION: u64 = (u32::MAX as u64) + 1;

    #[test]
    fn flat_string_representations() {
        const FOUR_MEMBERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

        let synced_v = VersionVector::Synced {
            num_members: FOUR_MEMBERS,
            version: 12,
        };
        assert_eq!(synced_v.to_string(), "〈0-3:12〉".to_string());

        let override_v_inner = OverrideVersion::with_next_version(12, 2);
        assert_eq!(
            override_v_inner.to_string(),
            "〈12..., 2:13, 12...〉".to_string()
        );
        let override_v = VersionVector::Override {
            num_members: FOUR_MEMBERS,
            version: override_v_inner,
        };
        assert_eq!(
            override_v.to_string(),
            "〈0-1:12, 2:13, 3-3:12〉".to_string()
        );

        let full_v_inner = PureVersionVector::from([12, 13, 12, 11]);
        assert_eq!(full_v_inner.to_string(), "〈12, 13, 12, 11〉".to_string());
        let full_v = VersionVector::Full(full_v_inner);
        assert_eq!(full_v.to_string(), "〈12, 13, 12, 11〉".to_string());
    }

    #[test]
    fn group_string_representations() {
        const FOUR_MEMBERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

        let member_list = vec![
            Identifier::from_array(["a", "a1"]),
            Identifier::from_array(["b", "b2"]),
            Identifier::from_array(["c", "c3"]),
            Identifier::from_array(["d", "d4"]),
        ];

        let synced_v = GroupVersionVector::new(
            member_list.clone(),
            VersionVector::Synced {
                num_members: FOUR_MEMBERS,
                version: 12,
            },
        );
        assert_eq!(
            synced_v.to_string(),
            concat!(
                "〈",
                "a.a1 -> 12, ",
                "b.b2 -> 12, ",
                "c.c3 -> 12, ",
                "d.d4 -> 12",
                "〉"
            )
            .to_string()
        );
        assert_eq!(
            synced_v.format_line_by_line().to_string(),
            concat!(
                "〈\n",
                " a.a1 -> 12,\n",
                " b.b2 -> 12,\n",
                " c.c3 -> 12,\n",
                " d.d4 -> 12\n",
                "〉"
            )
            .to_string()
        );

        let override_v_inner = OverrideVersion::with_next_version(12, 2);
        let override_v = GroupVersionVector::new(
            member_list.clone(),
            VersionVector::Override {
                num_members: FOUR_MEMBERS,
                version: override_v_inner,
            },
        );
        assert_eq!(
            override_v.to_string(),
            concat!(
                "〈",
                "a.a1 -> 12, ",
                "b.b2 -> 12, ",
                "c.c3 -> 13, ",
                "d.d4 -> 12",
                "〉"
            )
            .to_string()
        );
        assert_eq!(
            override_v.format_line_by_line().to_string(),
            concat!(
                "〈\n",
                " a.a1 -> 12,\n",
                " b.b2 -> 12,\n",
                " c.c3 -> 13,\n",
                " d.d4 -> 12\n",
                "〉"
            )
            .to_string()
        );

        let full_v_inner = PureVersionVector::from([12, 13, 12, 11]);
        let full_v =
            GroupVersionVector::new(member_list.clone(), VersionVector::Full(full_v_inner));
        assert_eq!(
            full_v.to_string(),
            concat!(
                "〈",
                "a.a1 -> 12, ",
                "b.b2 -> 13, ",
                "c.c3 -> 12, ",
                "d.d4 -> 11",
                "〉"
            )
            .to_string()
        );
        assert_eq!(
            full_v.format_line_by_line().to_string(),
            concat!(
                "〈\n",
                " a.a1 -> 12,\n",
                " b.b2 -> 13,\n",
                " c.c3 -> 12,\n",
                " d.d4 -> 11\n",
                "〉"
            )
            .to_string()
        );
    }

    /// Just some shorthands, to make the tests easier to read.
    mod helpers {
        use super::*;

        pub const BEFORE: HappenedBeforeOrdering = HappenedBeforeOrdering::Before;
        pub const AFTER: HappenedBeforeOrdering = HappenedBeforeOrdering::After;
        pub const EQUAL: HappenedBeforeOrdering = HappenedBeforeOrdering::Equal;
        pub const CONCURRENT: HappenedBeforeOrdering = HappenedBeforeOrdering::Concurrent;
        pub fn pure(a: [u64; 3]) -> VersionVector {
            let full = PureVersionVector::from(a);
            VersionVector::Full(full)
        }
        pub fn over(gv: u64, ov: (usize, u64)) -> VersionVector {
            let version = OverrideVersion::new(gv, ov.0, ov.1);
            VersionVector::Override {
                num_members: NonZeroUsize::new(3).unwrap(),
                version,
            }
        }

        pub fn sync(version: u64) -> VersionVector {
            VersionVector::Synced {
                num_members: NonZeroUsize::new(3).unwrap(),
                version,
            }
        }
    }

    #[test]
    fn special_equivalents() {
        use helpers::*;
        // These values are the same in different representations
        assert_eq!(sync(1), pure([1, 1, 1]));
        assert_eq!(sync(1).hb_cmp(&pure([1, 1, 1])), EQUAL);

        assert_eq!(over(1, (0, 2)), pure([2, 1, 1]));
        assert_eq!(over(1, (0, 2)).hb_cmp(&pure([2, 1, 1])), EQUAL);

        assert_eq!(over(1, (1, 3)), pure([1, 3, 1]));
        assert_eq!(over(1, (1, 3)).hb_cmp(&pure([1, 3, 1])), EQUAL);
    }

    #[test]
    fn basic_relationships() {
        use helpers::*;

        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([1, 2, 3])), EQUAL);
        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([1, 3, 3])), BEFORE);
        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([1, 1, 3])), AFTER);
        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([4, 5, 6])), BEFORE);
        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([1, 2, 18])), BEFORE);
        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([1, 3, 1])), CONCURRENT);

        assert_eq!(over(1, (1, 2)).hb_cmp(&over(1, (1, 2))), EQUAL);
        assert_eq!(over(1, (1, 2)).hb_cmp(&pure([1, 2, 1])), EQUAL);
        assert_eq!(over(1, (1, 2)).hb_cmp(&over(1, (1, 3))), BEFORE);
        assert_eq!(over(0, (1, 2)).hb_cmp(&over(1, (1, 2))), BEFORE);
        assert_eq!(over(0, (1, 3)).hb_cmp(&over(1, (1, 2))), CONCURRENT);
        assert_eq!(over(1, (1, 2)).hb_cmp(&over(1, (0, 2))), CONCURRENT);
        assert_eq!(over(0, (1, 1)).hb_cmp(&over(1, (0, 2))), BEFORE);
        assert_eq!(over(0, (1, 2)).hb_cmp(&over(0, (1, 1))), AFTER);

        assert_eq!(sync(1).hb_cmp(&sync(1)), EQUAL);
        assert_eq!(sync(1).hb_cmp(&pure([1, 1, 1])), EQUAL);
        assert_eq!(sync(1).hb_cmp(&sync(2)), BEFORE);
        assert_eq!(sync(3).hb_cmp(&sync(2)), AFTER);
        assert_eq!(sync(1).hb_cmp(&pure([1, 2, 1])), BEFORE);
        assert_eq!(sync(1).hb_cmp(&pure([0, 2, 1])), CONCURRENT);
    }

    #[test]
    fn successors() {
        use helpers::*;

        assert_eq!(pure([1, 2, 3]).succ_at(0), pure([2, 2, 3]));
        assert_eq!(pure([1, 2, 3]).succ_at(1), pure([1, 3, 3]));
        assert_eq!(pure([1, 2, 3]).succ_at(2), pure([1, 2, 4]));
        assert_eq!(pure([1, 2, 3]).succ_at(2).succ_at(2), pure([1, 2, 5]));
        assert_eq!(pure([1, 2, 3]).succ_at(1).succ_at(2), pure([1, 3, 4]));

        assert_eq!(over(1, (0, 2)).succ_at(0), over(1, (0, 3)));
        assert_eq!(over(1, (0, 2)).succ_at(0).succ_at(0), over(1, (0, 4)));
        assert_eq!(over(1, (0, 2)).succ_at(1), pure([2, 2, 1]));
        assert_eq!(over(1, (0, 2)).succ_at(0).succ_at(1), pure([3, 2, 1]));

        assert_eq!(sync(1).succ_at(0), over(1, (0, 2)));
        assert_eq!(sync(1).succ_at(1), over(1, (1, 2)));
        assert_eq!(sync(1).succ_at(0).succ_at(1), pure([2, 2, 1]));
    }

    fn full_vector_strategy() -> impl Strategy<Value = VersionVector> {
        // Don't make ridiculously large vectors, since they take a lot of memory.
        prop::collection::vec(any::<u64>(), 1..100)
            .prop_map(|entries| VersionVector::Full(PureVersionVector::from(entries)))
    }

    fn override_vector_strategy() -> impl Strategy<Value = VersionVector> {
        (any::<NonZeroUsize>(), 0..LARGE_VERSION).prop_flat_map(|(num_members, group_version)| {
            (0..num_members.get(), (group_version + 1)..u64::MAX).prop_map(
                move |(override_position, override_version)| VersionVector::Override {
                    num_members,
                    version: OverrideVersion::new(
                        group_version,
                        override_position,
                        override_version,
                    ),
                },
            )
        })
    }

    fn version_vector_strategy() -> impl Strategy<Value = VersionVector> {
        prop_oneof![
            full_vector_strategy(),
            override_vector_strategy(),
            (any::<NonZeroUsize>(), any::<u64>()).prop_map(|(num_members, version)| {
                VersionVector::Synced {
                    num_members,
                    version,
                }
            }),
        ]
    }

    fn fixed_size_full_vector_strategy(
        num_members: NonZeroUsize,
    ) -> impl Strategy<Value = VersionVector> {
        prop::collection::vec(any::<u64>(), num_members.get())
            .prop_map(|entries| VersionVector::Full(PureVersionVector(entries.into_boxed_slice())))
    }

    fn fixed_size_override_vector_strategy(
        num_members: NonZeroUsize,
    ) -> impl Strategy<Value = VersionVector> {
        (0..LARGE_VERSION).prop_flat_map(move |group_version| {
            (0..num_members.get(), (group_version + 1)..u64::MAX).prop_map(
                move |(override_position, override_version)| VersionVector::Override {
                    num_members,
                    version: OverrideVersion::new(
                        group_version,
                        override_position,
                        override_version,
                    ),
                },
            )
        })
    }

    fn fixed_size_synced_strategy(
        num_members: NonZeroUsize,
    ) -> impl Strategy<Value = VersionVector> {
        any::<u64>().prop_map(move |version| VersionVector::Synced {
            num_members,
            version,
        })
    }

    fn fixed_size_version_vector_strategy(
        num_members: NonZeroUsize,
    ) -> BoxedStrategy<VersionVector> {
        let mut strategies = vec![
            fixed_size_full_vector_strategy(num_members).boxed(),
            fixed_size_synced_strategy(num_members).boxed(),
        ];
        if num_members.get() > 1 {
            strategies.push(fixed_size_override_vector_strategy(num_members).boxed());
        }
        Union::new(strategies).boxed()
    }

    fn version_vector_size_strategy() -> impl Strategy<Value = NonZeroUsize> {
        (1usize..100usize).prop_map(|u| NonZeroUsize::new(u).unwrap())
    }

    prop_compose! {
        fn equal_size_version_vector_strategy()(l in version_vector_size_strategy())(v1 in fixed_size_version_vector_strategy(l), v2 in fixed_size_version_vector_strategy(l), v3 in fixed_size_version_vector_strategy(l)) -> (VersionVector, VersionVector, VersionVector) {
            (v1, v2, v3)
        }
    }

    proptest! {
        #[test]
        fn version_vector_invariants(v1 in version_vector_strategy(), v2 in version_vector_strategy(), v3 in version_vector_strategy()) {
            version_vector_invariants_impl(v1, v2, v3)
        }

        #[test]
        fn version_vector_equal_size_invariants((v1, v2, v3) in equal_size_version_vector_strategy()) {
            version_vector_invariants_impl(v1, v2, v3)
        }
    }
    fn version_vector_invariants_impl(v1: VersionVector, v2: VersionVector, v3: VersionVector) {
        single_version_vector_invariants_impl(v1.clone());
        single_version_vector_invariants_impl(v2.clone());
        single_version_vector_invariants_impl(v3.clone());
        two_version_vector_invariants_impl(v1.clone(), v2.clone());
        two_version_vector_invariants_impl(v2.clone(), v3.clone());
        two_version_vector_invariants_impl(v1.clone(), v3.clone());

        // Transitive
        if v1 <= v2 && v2 <= v3 {
            assert!(v1 <= v3);
        }
        if v1.hb_cmp(&v2) == HappenedBeforeOrdering::Before
            && v2.hb_cmp(&v3) == HappenedBeforeOrdering::Before
        {
            assert_eq!(v1.hb_cmp(&v3), HappenedBeforeOrdering::Before);
        }
    }
    fn single_version_vector_invariants_impl(v: VersionVector) {
        // Reflexive
        #[allow(clippy::eq_op)]
        {
            assert_eq!(v, v);
        }
        assert_eq!(v.hb_cmp(&v), HappenedBeforeOrdering::Equal);

        // Ensure that we don't overflow, and also we don't run out of memory if we need to expand to a full vector.
        if v.max_version() < u64::MAX - 2 && v.num_members().get() < 100 {
            // Increments
            for pos in 0..v.num_members().get() {
                let next = v.succ_at(pos);
                assert!(v < next);
                assert_eq!(v.hb_cmp(&next), HappenedBeforeOrdering::Before);
                assert_eq!(next.hb_cmp(&v), HappenedBeforeOrdering::After);

                let next_again = next.succ_at(pos);
                assert!(v < next_again);
                assert!(next < next_again);
                assert_eq!(v.hb_cmp(&next_again), HappenedBeforeOrdering::Before);
                assert_eq!(next.hb_cmp(&next_again), HappenedBeforeOrdering::Before);
            }
        }
    }
    #[allow(clippy::neg_cmp_op_on_partial_ord)]
    fn two_version_vector_invariants_impl(v1: VersionVector, v2: VersionVector) {
        // v1 == v2 iff v1 =hb= v2
        if v1 == v2 {
            assert_eq!(v1.hb_cmp(&v2), HappenedBeforeOrdering::Equal);
        } else {
            assert_ne!(v1.hb_cmp(&v2), HappenedBeforeOrdering::Equal);
        }
        if v1.hb_cmp(&v2) == HappenedBeforeOrdering::Equal {
            assert_eq!(v1, v2);
        } else {
            assert_ne!(v1, v2);
        }

        // v1 < v2 iff v1 -> v2
        if v1 < v2 {
            assert_eq!(v1.hb_cmp(&v2), HappenedBeforeOrdering::Before);
        } else {
            assert_ne!(v1.hb_cmp(&v2), HappenedBeforeOrdering::Before);
        }
        if v1.hb_cmp(&v2) == HappenedBeforeOrdering::Before {
            assert!(v1 < v2);
        } else {
            assert!(!(v1 < v2));
        }
        // v1 > v2 iff v2 -> v1
        if v1 > v2 {
            assert_eq!(v1.hb_cmp(&v2), HappenedBeforeOrdering::After);
        } else {
            assert_ne!(v1.hb_cmp(&v2), HappenedBeforeOrdering::After);
        }
        if v1.hb_cmp(&v2) == HappenedBeforeOrdering::After {
            assert!(v1 > v2);
        } else {
            assert!(!(v1 > v2));
        }

        // Antisymmetry.
        assert_eq!(v1.hb_cmp(&v2), v2.hb_cmp(&v1).reverse());
    }

    proptest! {
        #[test]
        fn single_override_version_compare(v in 0u64..LARGE_VERSION) {
            single_override_version_compare_impl(v)
        }
    }
    fn single_override_version_compare_impl(v: u64) {
        let ov = OverrideVersion::with_next_version(v, 2usize);
        #[allow(clippy::eq_op)]
        {
            assert_eq!(ov, ov);
        }
        assert_eq!(ov.hb_cmp(&ov), HappenedBeforeOrdering::Equal);
    }

    proptest! {
        #[test]
        fn two_override_version_compare(v1 in 0u64..u64::MAX, v2 in 0u64..LARGE_VERSION) {
            two_override_version_compare_impl(v1, v2)
        }
    }
    fn two_override_version_compare_impl(v1: u64, v2: u64) {
        let v1_id1 = OverrideVersion::with_next_version(v1, 2usize);
        let v2_id1 = OverrideVersion::with_next_version(v2, 2usize);

        let number_compare: HappenedBeforeOrdering = v1.cmp(&v2).into();
        assert_eq!(v1_id1.hb_cmp(&v2_id1), number_compare);

        let v2_id2 = OverrideVersion::with_next_version(v2, 5usize);

        let compare_result = v1_id1.hb_cmp(&v2_id2);
        if v1 < v2 - 1 {
            assert_eq!(compare_result, HappenedBeforeOrdering::Before);
        } else if v2 < v1 - 1 {
            assert_eq!(compare_result, HappenedBeforeOrdering::After);
        } else {
            assert_eq!(compare_result, HappenedBeforeOrdering::Concurrent);
        }
        // println!("v1={v1}, v2={v2}, v1_id1={v1_id1}, v2_id1={v2_id1}, v2_id2={v2_id2}");
    }
}
