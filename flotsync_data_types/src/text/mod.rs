use crate::text::linear_data::VecLinearData;

// TODO: Remove this, it's just for dev so everything isn't yellow all the time.
#[allow(unused)]
mod linear_data;
pub use linear_data::{LinearData, NodeIds};
/// Simple diffs on plain old strings.
mod text_diff;

pub trait LinearDataString {
    fn to_string(&self) -> String;
}

impl<L> LinearDataString for L
where
    L: linear_data::LinearData<String>,
{
    fn to_string(&self) -> String {
        let mut builder = String::new();

        for s in self.iter_values() {
            builder.push_str(s);
        }

        builder
    }
}

pub type LinearString<Id> = VecLinearData<Id, String>;
pub type LinearStringUntracked = LinearString<()>;

pub fn linear_string<S>(s: S) -> LinearStringUntracked
where
    S: Into<String>,
{
    let s_owned: String = s.into();
    VecLinearData::with_value(&mut std::iter::repeat(()), s_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_string_single_value_roundtrip() {
        let input = "A simple test string";
        let linear = linear_string(input);
        linear.check_integrity();
        assert_eq!(linear.to_string(), input);
    }

    #[test]
    fn linear_string_appends() {
        const TEST_VALUES: [&str; 8] = ["A", " ", "simple", " ", "test", " ", "string", "."];
        let mut id_generator = &mut std::iter::repeat(());

        let mut reference = String::new();
        let mut linear = LinearStringUntracked::new(&mut id_generator);
        assert_eq!(reference, linear.to_string());
        for s in TEST_VALUES {
            reference.push_str(s);
            //println!("before append: {:#?}", linear);
            linear.append((), s.to_string());
            //println!("after append: {:#?}", linear);
            linear.check_integrity();
            assert_eq!(linear.to_string(), reference);
        }
    }

    #[test]
    fn linear_string_prepends() {
        const TEST_VALUES: [&str; 8] = ["A", " ", "simple", " ", "test", " ", "string", "."];
        let mut id_generator = &mut std::iter::repeat(());

        let mut reference = String::new();
        let mut linear = LinearStringUntracked::new(&mut id_generator);
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
    fn linear_string_inserts() {
        const TEST_VALUES: [&str; 8] = ["A", " ", "simple", " ", "test", " ", "string", "."];
        let mut test_id = 0u16;
        let mut id_generator = std::iter::repeat_with(|| {
            let next_id = test_id;
            test_id += 1;
            next_id
        });
        // The number of ids we ended up assigning during creation
        // Just hardcode this. Otherwise we'd have to drop the generator to inspect test_id.
        // let empty_offset = 2;

        // Setup both strings to be the same.
        let mut reference = String::new();
        let mut linear = LinearString::new(&mut id_generator);
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
        let nodes_at_three = linear.ids_at_pos(3).unwrap().cloned();
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
        let nodes_at_beginning = linear.ids_at_pos(0).unwrap().cloned();
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
        let nodes_at_end = linear.ids_at_pos(linear.len() - 1).unwrap().cloned();
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
}
