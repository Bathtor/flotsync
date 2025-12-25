use super::*;
use crate::text::grapheme_string::GraphemeString;

pub type LinearWordString<Id> = VecLinearData<Id, String>;
#[allow(unused, reason = "Testing")]
pub type LinearWordStringUntracked = LinearWordString<()>;
impl<Id> fmt::Display for LinearWordString<Id>
where
    Id: Clone + fmt::Debug + PartialEq + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.iter_values().try_for_each(|s| f.write_str(s))
    }
}

#[derive(Clone, Debug)]
pub struct LinearString<Id> {
    data: VecCoalescedLinearData<Id, GraphemeString>,
}
impl<Id> LinearString<Id>
where
    Id: Clone + fmt::Debug + PartialEq + 'static,
{
    pub fn new<I>(id_generator: &mut I) -> Self
    where
        I: Iterator<Item = Id>,
    {
        let data = VecCoalescedLinearData::new(id_generator);
        Self { data }
    }

    pub fn with_value<I>(id_generator: &mut I, initial_value: String) -> Self
    where
        I: Iterator<Item = Id>,
    {
        if initial_value.is_empty() {
            Self::new(id_generator)
        } else {
            let wrapped_value = GraphemeString::new(initial_value);
            let data = VecCoalescedLinearData::with_value(id_generator, wrapped_value);
            Self { data }
        }
    }

    pub fn append(&mut self, id: IdWithIndex<Id>, value: String) {
        self.data.append(id, GraphemeString::new(value));
    }

    pub fn prepend(&mut self, id: IdWithIndex<Id>, value: String) {
        self.data.prepend(id, GraphemeString::new(value));
    }

    /// This is the number of UTF-8 Graphemes in this string.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn ids_in_range<R>(&self, range: R) -> Option<NodeIdRangeString<Id>>
    where
        R: RangeBounds<usize>,
    {
        self.data.ids_in_range(range).map(NodeIdRangeString)
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

    #[cfg(test)]
    pub(in crate::text) fn check_integrity(&self) {
        self.data.check_integrity();
    }
}
impl<Id> fmt::Display for LinearString<Id>
where
    Id: Clone + fmt::Debug + PartialEq + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.data.iter_values().try_for_each(|s| f.write_str(s))
    }
}
impl<Id> LinearData<String, str> for LinearString<Id>
where
    Id: Clone + fmt::Debug + PartialEq + 'static,
{
    type Id = IdWithIndex<Id>;

    type Iter<'a> = LinearStringIter<'a, Id>;

    fn ids_after_head(&self) -> LinkIds<Self::Id> {
        self.data.ids_after_head()
    }

    fn ids_before_end(&self) -> LinkIds<Self::Id> {
        self.data.ids_before_end()
    }

    /// Get the ids relevant for the UTF-8 Grapheme at `position`.
    ///
    /// Returns `None` if this position does not exist, for example when `self` is empty.
    fn ids_at_pos(&self, position: usize) -> Option<NodeIds<Self::Id>> {
        self.data.ids_at_pos(position)
    }

    fn insert(
        &mut self,
        id: Self::Id,
        pred: Self::Id,
        succ: Self::Id,
        value: String,
    ) -> Result<(), String> {
        let graphemes = GraphemeString::new(value);
        self.data
            .insert(id, pred, succ, graphemes)
            .map_err(GraphemeString::unwrap)
    }

    fn delete<'a>(&'a mut self, id: &Self::Id) -> Option<&'a str> {
        self.data.delete(id)
    }

    fn iter_values(&self) -> Self::Iter<'_> {
        LinearStringIter {
            underlying: self.data.iter_values(),
        }
    }

    fn iter_ids(&self) -> impl Iterator<Item = &Self::Id> {
        self.data.iter_ids()
    }

    fn apply_operation(
        &mut self,
        operation: linear_data::DataOperation<Self::Id, String>,
    ) -> Result<(), linear_data::DataOperation<Self::Id, String>> {
        let op = operation.map_value(GraphemeString::new);
        self.data
            .apply_operation(op)
            .map_err(|op| op.map_value(GraphemeString::unwrap))
    }
}
impl<Id> DebugFormatting for LinearString<Id>
where
    Id: fmt::Display + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        DebugFormatting::fmt(&self.data, f)
    }
}

/// Convenience wrapper around [[NodeIdRange]] when using it with [[LinearString]].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeIdRangeString<Id>(NodeIdRange<Id>);
impl<Id> NodeIdRangeString<Id>
where
    Id: Clone + fmt::Debug + PartialEq + 'static,
{
    /// Tries to delete all the nodes contained in the range.
    ///
    /// Returns the first failing range if unsuccessful.
    /// In this case the previous deletes will have been applied.
    pub fn delete<'a>(
        &'a self,
        data: &mut LinearString<Id>,
    ) -> Result<(), &'a IdWithIndexRange<Id>> {
        self.0.delete(&mut data.data)
    }

    pub fn delete_operations(self) -> impl Iterator<Item = DataOperation<IdWithIndex<Id>, String>> {
        self.0.delete_operations()
    }
}

pub struct LinearStringIter<'a, Id> {
    underlying: VecCoalescedLinearDataIter<'a, IdWithIndex<Id>, GraphemeString>,
}
impl<'a, Id> Iterator for LinearStringIter<'a, Id> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        self.underlying.next()
    }
}

#[allow(unused, reason = "Testing")]
pub fn linear_word_string<S>(s: S) -> LinearWordStringUntracked
where
    S: Into<String>,
{
    let s_owned: String = s.into();
    VecLinearData::with_value(&mut std::iter::repeat(()), s_owned)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use flotsync_utils::testing::CloneExt;

    const TEST_VALUES: [&str; 8] = ["A", " ", "simple", " ", "test", " ", "string", "."];
    const UNICODE_TEST_VALUES: [&str; 12] = [
        "A",       // basic ASCII
        " ",       // space
        "café",    // Latin + combining accent normalized
        "naïve",   // more accents
        "日本語",  // CJK (3-byte UTF-8 per char)
        "русский", // Cyrillic
        "עִבְרִית",   // Hebrew + Niqqud (combining marks)
        "हिन्दी",   // Devanagari
        "🙂",      // single emoji
        "👨‍👩‍👦",      // emoji family using zero-width joiners
        "🏴‍☠️",      // flag + variation selector + ZWJ
        "𝟘𝟙𝟚",     // Mathematical bold digits (4-byte UTF-8)
    ];

    pub(crate) struct TestIdGenerator {
        current: Option<u32>,
    }
    impl TestIdGenerator {
        pub fn new() -> Self {
            Self { current: Some(0) }
        }

        pub fn without_ids(existing_ids: impl Iterator<Item = u32>) -> Self {
            let max_id = existing_ids.max().unwrap_or(0);
            Self {
                current: Some(max_id + 1),
            }
        }

        pub fn next_with_zero_index(&mut self) -> Option<IdWithIndex<u32>> {
            self.next().map(IdWithIndex::zero)
        }
    }
    impl Iterator for TestIdGenerator {
        type Item = u32;

        fn next(&mut self) -> Option<Self::Item> {
            let current = self.current.take();
            if let Some(current) = current {
                self.current = current.checked_add(1);
            }
            current
        }
    }

    mod linear_string {
        use super::*;

        #[test]
        fn single_value_roundtrip() {
            let input = "A simple test string";
            let linear = LinearString::with_value(&mut TestIdGenerator::new(), input.to_string());
            linear.check_integrity();
            assert_eq!(linear.to_string(), input);
        }

        #[test]
        fn ascii_appends() {
            let mut id_generator = TestIdGenerator::new();

            let mut reference = String::new();
            let mut linear = LinearString::new(&mut id_generator);
            assert_eq!(reference, linear.to_string());
            for s in TEST_VALUES {
                reference.push_str(s);
                linear.append(id_generator.next_with_zero_index().unwrap(), s.to_string());
                linear.check_integrity();
                assert_eq!(linear.to_string(), reference);
                assert_eq!(linear.len(), reference.len());
            }
        }

        #[test]
        fn unicode_appends() {
            let mut id_generator = TestIdGenerator::new();

            let mut reference = String::new();
            let mut linear = LinearString::new(&mut id_generator);
            assert_eq!(reference, linear.to_string());
            for s in UNICODE_TEST_VALUES {
                reference.push_str(s);
                linear.append(id_generator.next_with_zero_index().unwrap(), s.to_string());
                linear.check_integrity();
                assert_eq!(linear.to_string(), reference);
                assert_eq!(linear.len(), reference.graphemes(true).count());
            }
        }

        #[test]
        fn ascii_prepends() {
            let mut id_generator = TestIdGenerator::new();

            let mut reference = String::new();
            let mut linear = LinearString::new(&mut id_generator);
            assert_eq!(linear.to_string(), reference);
            for s in TEST_VALUES {
                reference.push_str(s);
            }
            for s in TEST_VALUES.iter().rev() {
                linear.prepend(id_generator.next_with_zero_index().unwrap(), s.to_string());
                linear.check_integrity();
            }
            assert_eq!(linear.to_string(), reference);
            assert_eq!(linear.len(), reference.len());
        }

        #[test]
        fn unicode_prepends() {
            let mut id_generator = TestIdGenerator::new();

            let mut reference = String::new();
            let mut linear = LinearString::new(&mut id_generator);
            assert_eq!(linear.to_string(), reference);
            for s in UNICODE_TEST_VALUES {
                reference.push_str(s);
            }
            for s in UNICODE_TEST_VALUES.iter().rev() {
                linear.prepend(id_generator.next_with_zero_index().unwrap(), s.to_string());
                linear.check_integrity();
            }
            assert_eq!(linear.to_string(), reference);
            assert_eq!(linear.len(), reference.graphemes(true).count());
        }

        #[test]
        fn ascii_inserts() {
            let mut id_generator = TestIdGenerator::new();

            // Setup both strings to be the same.
            let mut reference = TEST_VALUES.join("");
            let mut linear = LinearString::new(&mut id_generator);
            linear.append(
                IdWithIndex::zero(id_generator.next().unwrap()),
                reference.clone(),
            );
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);

            // Make an insertion.
            const INSERT_STR: &str = "yet important ";
            let char_index_at_test_index_three = TEST_VALUES[..=3].iter().map(|s| s.len()).sum();
            reference.insert_str(char_index_at_test_index_three, INSERT_STR);

            // This happens to work, because this text only uses ascii chars.
            let nodes_at_test_index_three =
                linear.ids_at_pos(char_index_at_test_index_three).unwrap();
            nodes_at_test_index_three
                .insert_before(
                    &mut linear,
                    IdWithIndex::zero(id_generator.next().unwrap()),
                    INSERT_STR.to_string(),
                )
                .expect("failed to insert");
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);

            // Make an prepend-like insertion.
            const FRONT_INSERT_STR: &str = "Not ";
            reference.insert_str(0, FRONT_INSERT_STR);
            let nodes_at_beginning = linear.ids_at_pos(0).unwrap();
            nodes_at_beginning
                .insert_before(
                    &mut linear,
                    IdWithIndex::zero(id_generator.next().unwrap()),
                    FRONT_INSERT_STR.to_string(),
                )
                .expect("failed to insert");
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);

            // Make an append-like insertion.
            const END_INSERT_STR: &str = "!?";
            reference.push_str(END_INSERT_STR);
            let nodes_at_end = linear.ids_at_pos(linear.len() - 1).unwrap();
            nodes_at_end
                .insert_after(
                    &mut linear,
                    IdWithIndex::zero(id_generator.next().unwrap()),
                    END_INSERT_STR.to_string(),
                )
                .expect("failed to insert");
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);
        }

        #[test]
        fn unicode_inserts() {
            let mut id_generator = TestIdGenerator::new();

            // Setup both strings to be the same.
            let mut reference = UNICODE_TEST_VALUES.join("");
            let mut linear = LinearString::new(&mut id_generator);
            linear.append(
                IdWithIndex::zero(id_generator.next().unwrap()),
                reference.clone(),
            );
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);

            // Make an insertion.
            const INSERT_STR: &str = " inte brå ";

            let byte_index_at_test_index_seven =
                UNICODE_TEST_VALUES[..=7].iter().map(|s| s.len()).sum();
            reference.insert_str(byte_index_at_test_index_seven, INSERT_STR);

            let char_index_at_test_index_seven = UNICODE_TEST_VALUES[..=7]
                .iter()
                .map(|s| s.graphemes(true).count())
                .sum();
            let nodes_at_test_index_three =
                linear.ids_at_pos(char_index_at_test_index_seven).unwrap();
            nodes_at_test_index_three
                .insert_before(
                    &mut linear,
                    IdWithIndex::zero(id_generator.next().unwrap()),
                    INSERT_STR.to_string(),
                )
                .expect("failed to insert");
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);

            // Make an prepend-like insertion.
            const FRONT_INSERT_STR: &str = "Нет ";
            reference.insert_str(0, FRONT_INSERT_STR);
            let nodes_at_beginning = linear.ids_at_pos(0).unwrap();
            nodes_at_beginning
                .insert_before(
                    &mut linear,
                    IdWithIndex::zero(id_generator.next().unwrap()),
                    FRONT_INSERT_STR.to_string(),
                )
                .expect("failed to insert");
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);

            // Make an append-like insertion.
            const END_INSERT_STR: &str = "⸘";
            reference.push_str(END_INSERT_STR);
            let nodes_at_end = linear.ids_at_pos(linear.len() - 1).unwrap();
            nodes_at_end
                .insert_after(
                    &mut linear,
                    IdWithIndex::zero(id_generator.next().unwrap()),
                    END_INSERT_STR.to_string(),
                )
                .expect("failed to insert");
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);
        }

        #[test]
        fn single_char_deletes() {
            let mut id_generator = TestIdGenerator::new();

            let mut linear = LinearString::with_value(&mut id_generator, TEST_VALUES.join(""));
            linear.check_integrity();

            assert_eq!(linear.to_string(), TEST_VALUES.join(""));

            let ids_at_three = linear.ids_at_pos(3).unwrap();
            assert_eq!(ids_at_three.delete(&mut linear), Some("i"));
            linear.check_integrity();
            assert_eq!(linear.to_string().as_str(), "A smple test string.");
            // Doing it again should be fine but change nothing.
            assert_eq!(ids_at_three.delete(&mut linear), Some("i"));
            linear.check_integrity();
            assert_eq!(linear.to_string().as_str(), "A smple test string.");

            // Delete the space.
            let ids_at_one = linear.ids_at_pos(1).unwrap();
            assert_eq!(ids_at_one.delete(&mut linear), Some(" "));
            linear.check_integrity();
            assert_eq!(linear.to_string().as_str(), "Asmple test string.");

            // Delete everything.
            while !linear.is_empty() {
                let current_id_at_head = linear.ids_at_pos(0).unwrap();
                current_id_at_head
                    .delete(&mut linear)
                    .expect("failed to delete");
                linear.check_integrity();
            }
            assert_eq!(linear.ids_at_pos(0), None);
            assert_eq!(linear.len(), 0);
            assert_eq!(linear.to_string().as_str(), "");
        }

        #[test]
        fn range_deletes() {
            let mut id_generator = TestIdGenerator::new();

            let mut linear = LinearString::with_value(&mut id_generator, TEST_VALUES.join(""));
            linear.check_integrity();
            assert_eq!(linear.to_string(), TEST_VALUES.join(""));

            let ids_for_simple = linear.ids_in_range(2..=7).unwrap();
            assert_eq!(ids_for_simple.delete(&mut linear), Ok(()));
            linear.check_integrity();
            assert_eq!(linear.to_string().as_str(), "A  test string.");

            // Doing it again should be fine but change nothing.
            assert_eq!(ids_for_simple.delete(&mut linear), Ok(()));
            linear.check_integrity();
            assert_eq!(linear.to_string().as_str(), "A  test string.");

            // Delete the space that's now at this position.
            let ids_for_space = linear.ids_in_range(2..=2).unwrap();
            assert_eq!(ids_for_space.delete(&mut linear), Ok(()));
            linear.check_integrity();
            assert_eq!(linear.to_string().as_str(), "A test string.");

            // Delete everything (in various ways).
            fn empty_checks(l: &LinearString<u32>) {
                l.check_integrity();
                assert_eq!(l.ids_at_pos(0), None);
                assert_eq!(l.ids_in_range(0..=0), None);
                assert_eq!(l.len(), 0);
                assert_eq!(l.to_string().as_str(), "");
            }
            linear.with_copy(|mut l| {
                let ids_for_everything = l.ids_in_range(0..l.len()).unwrap();
                assert_eq!(ids_for_everything.delete(&mut l), Ok(()));
                empty_checks(&l);
            });
            linear.with_copy(|mut l| {
                let ids_for_everything = l.ids_in_range(0..).unwrap();
                assert_eq!(ids_for_everything.delete(&mut l), Ok(()));
                empty_checks(&l);
            });
            linear.with_copy(|mut l| {
                let ids_for_everything = l.ids_in_range(..l.len()).unwrap();
                assert_eq!(ids_for_everything.delete(&mut l), Ok(()));
                empty_checks(&l);
            });
            linear.with_copy(|mut l| {
                let ids_for_everything = l.ids_in_range(..).unwrap();
                assert_eq!(ids_for_everything.delete(&mut l), Ok(()));
                empty_checks(&l);
            });
        }
    }

    mod linear_word_string {
        use super::*;

        #[test]
        fn single_value_roundtrip() {
            let input = "A simple test string";
            let linear = linear_word_string(input);
            linear.check_integrity();
            assert_eq!(linear.to_string(), input);
        }

        #[test]
        fn appends() {
            let mut id_generator = &mut std::iter::repeat(());

            let mut reference = String::new();
            let mut linear = LinearWordStringUntracked::new(&mut id_generator);
            assert_eq!(reference, linear.to_string());
            for s in TEST_VALUES {
                reference.push_str(s);
                linear.append((), s.to_string());
                linear.check_integrity();
                assert_eq!(linear.to_string(), reference);
            }
        }

        #[test]
        fn prepends() {
            let mut id_generator = &mut std::iter::repeat(());

            let mut reference = String::new();
            let mut linear = LinearWordStringUntracked::new(&mut id_generator);
            assert_eq!(linear.to_string(), reference);
            for s in TEST_VALUES {
                reference.push_str(s);
            }
            for s in TEST_VALUES.iter().rev() {
                linear.prepend((), s.to_string());
                linear.check_integrity();
            }
            assert_eq!(linear.to_string(), reference);
        }

        #[test]
        fn inserts() {
            let mut id_generator = TestIdGenerator::new();

            // Setup both strings to be the same.
            let mut reference = String::new();
            let mut linear = LinearWordString::new(&mut id_generator);
            assert_eq!(linear.to_string(), reference);
            for s in TEST_VALUES {
                reference.push_str(s);
                linear.append(id_generator.next().unwrap(), s.to_string());
                linear.check_integrity();
            }
            assert_eq!(linear.to_string(), reference);

            // Make an insertion.
            const INSERT_STR: &str = "yet important ";
            let char_index_at_test_index_three = TEST_VALUES[..=3].iter().map(|s| s.len()).sum();
            reference.insert_str(char_index_at_test_index_three, INSERT_STR);
            let nodes_at_three = linear.ids_at_pos(3).unwrap();
            nodes_at_three
                .insert_after(
                    &mut linear,
                    id_generator.next().unwrap(),
                    INSERT_STR.to_string(),
                )
                .expect("failed to insert");
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);

            // Make an prepend-like insertion.
            const FRONT_INSERT_STR: &str = "Not ";
            reference.insert_str(0, FRONT_INSERT_STR);
            let nodes_at_beginning = linear.ids_at_pos(0).unwrap();
            nodes_at_beginning
                .insert_before(
                    &mut linear,
                    id_generator.next().unwrap(),
                    FRONT_INSERT_STR.to_string(),
                )
                .expect("failed to insert");
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);

            // Make an append-like insertion.
            const END_INSERT_STR: &str = "!?";
            reference.push_str(END_INSERT_STR);
            let nodes_at_end = linear.ids_at_pos(linear.len() - 1).unwrap();
            nodes_at_end
                .insert_after(
                    &mut linear,
                    id_generator.next().unwrap(),
                    END_INSERT_STR.to_string(),
                )
                .expect("failed to insert");
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);
        }

        #[test]
        fn deletes() {
            let mut id_generator = TestIdGenerator::new();

            let mut linear = LinearWordString::new(&mut id_generator);
            for s in TEST_VALUES {
                linear.append(id_generator.next().unwrap(), s.to_string());
                linear.check_integrity();
            }

            assert_eq!(linear.to_string(), TEST_VALUES.join(""));

            let ids_at_two = linear.ids_at_pos(2).unwrap();
            assert_eq!(
                ids_at_two.delete(&mut linear).map(|s| s.as_str()),
                Some("simple")
            );
            assert_eq!(linear.to_string().as_str(), "A  test string.");
            // Doing it again should be fine but change nothing.
            assert_eq!(
                ids_at_two.delete(&mut linear).map(|s| s.as_str()),
                Some("simple")
            );
            assert_eq!(linear.to_string().as_str(), "A  test string.");

            // Delete the space that's now at this position.
            let new_ids_at_two = linear.ids_at_pos(2).unwrap();
            assert_eq!(
                new_ids_at_two.delete(&mut linear).map(|s| s.as_str()),
                Some(" ")
            );
            assert_eq!(linear.to_string().as_str(), "A test string.");

            // Delete everything.
            while !linear.is_empty() {
                let current_id_at_head = linear.ids_at_pos(0).unwrap();
                current_id_at_head
                    .delete(&mut linear)
                    .expect("failed to delete");
            }
            assert_eq!(linear.ids_at_pos(0), None);
            assert_eq!(linear.len(), 0);
            assert_eq!(linear.to_string().as_str(), "");
        }
    }
}
