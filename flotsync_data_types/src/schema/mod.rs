//! The flotsync data model is based around strict [[Schema]]s which specify both
//! the underlying storage type and value domain as well as the resolution semantics under
//! concurrent modification.
use std::{borrow::Cow, collections::HashMap, fmt, ops::Index};

use crate::FieldOperations;

pub mod datamodel;
mod public_api;
pub mod values;
pub use public_api::*;
pub use values::{NULL, OrderedValue, OrderedValueError};

/// A schema a collection of named, and typed columns.
///
/// The data model is position-independent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    /// Columns by name.
    pub columns: HashMap<String, Field>,
    /// A map containing information about this schema.
    pub metadata: HashMap<String, String>,
}
impl Schema {
    pub fn from_fields<const N: usize>(fields: [Field; N]) -> Self {
        let mut columns = HashMap::with_capacity(N);
        for field in fields {
            if let Some(default_value) = field.default_value.clone() {
                if let Err(source) = field.initial(default_value) {
                    panic!("Invalid default value for field '{}': {source}", field.name);
                }
            }
            if let Some(existing_field) = columns.insert(field.name.to_string(), field) {
                panic!("Duplicate field name: {}", existing_field.name);
            }
        }
        Schema {
            columns,
            metadata: HashMap::new(),
        }
    }

    pub fn borrow(&self) -> Cow<'_, Schema> {
        Cow::Borrowed(self)
    }

    pub fn field(&self, field_name: &str) -> Option<&Field> {
        self.columns.get(field_name)
    }
}
impl Index<&str> for Schema {
    type Output = Field;

    fn index(&self, index: &str) -> &Self::Output {
        self.columns
            .get(index)
            .unwrap_or_else(|| panic!("Unknown schema field: {index}"))
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fields = self.columns.values().collect::<Vec<_>>();
        fields.sort_unstable_by(|left, right| left.name.cmp(&right.name));

        if f.alternate() {
            f.write_str("SCHEMA (\n")?;
            for (index, field) in fields.iter().enumerate() {
                f.write_str("  ")?;
                write!(f, "{field}")?;
                if index + 1 < fields.len() {
                    f.write_str(",")?;
                }
                f.write_str("\n")?;
            }
            f.write_str(")")?;
            if !self.metadata.is_empty() {
                f.write_str("\n")?;
                write_tblproperties(&self.metadata, f)?;
            }
            return Ok(());
        }

        f.write_str("SCHEMA (")?;
        for (index, field) in fields.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{field}")?;
        }
        f.write_str(")")?;
        if !self.metadata.is_empty() {
            f.write_str(" ")?;
            write_tblproperties(&self.metadata, f)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// Name of this column.
    pub name: String,
    /// The data type of this field.
    pub data_type: ReplicatedDataType,
    /// Optional default value materialized on insert when no explicit value is provided.
    pub default_value: Option<datamodel::NullableBasicValue>,
    /// A map containing information about this column.
    pub metadata: HashMap<String, String>,
}
impl Field {
    pub fn latest_value_wins<S: Into<String>>(name: S, value_type: NullableBasicDataType) -> Self {
        Self {
            name: name.into(),
            data_type: ReplicatedDataType::LatestValueWins { value_type },
            default_value: None,
            metadata: HashMap::new(),
        }
    }

    pub fn linear_string<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            data_type: ReplicatedDataType::LinearString,
            default_value: None,
            metadata: HashMap::new(),
        }
    }

    pub fn linear_list<S: Into<String>>(name: S, value_type: PrimitiveType) -> Self {
        Self {
            name: name.into(),
            data_type: ReplicatedDataType::LinearList { value_type },
            default_value: None,
            metadata: HashMap::new(),
        }
    }

    pub fn monotonic_counter<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            data_type: ReplicatedDataType::MonotonicCounter { small_range: false },
            default_value: None,
            metadata: HashMap::new(),
        }
    }

    pub fn small_monotonic_counter<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            data_type: ReplicatedDataType::MonotonicCounter { small_range: true },
            default_value: None,
            metadata: HashMap::new(),
        }
    }

    pub fn total_order_register<S: Into<String>>(
        name: S,
        value_type: PrimitiveType,
        direction: Direction,
    ) -> Self {
        Self {
            name: name.into(),
            data_type: ReplicatedDataType::TotalOrderRegister {
                value_type,
                direction,
            },
            default_value: None,
            metadata: HashMap::new(),
        }
    }

    pub fn finite_state_register<S, I, V>(
        name: S,
        states: I,
    ) -> Result<Self, values::OrderedValueError>
    where
        S: Into<String>,
        I: IntoIterator<Item = V>,
        V: Into<values::OrderedValue>,
    {
        let states = values::NullablePrimitiveValueArray::ordered(states)?;
        let value_type = states.value_type();
        Ok(Self {
            name: name.into(),
            data_type: ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            default_value: None,
            metadata: HashMap::new(),
        })
    }
}
impl<OperationId> FieldOperations<OperationId> for Field {
    fn get_from_row<'a, R>(&self, row: &'a R) -> &'a crate::InMemoryFieldValue<OperationId>
    where
        R: crate::RowOperations<OperationId>,
    {
        row.get_field(&self.name)
            .expect("The row had a different schema than expected by this field.")
    }

    fn get_value<'a, R, T>(
        &self,
        row: &'a R,
    ) -> Result<std::borrow::Cow<'a, T>, crate::DecodeValueError>
    where
        R: crate::RowOperations<OperationId>,
        T: ?Sized + crate::Decode<OperationId>,
    {
        row.get_field_value(&self.name)
    }

    fn get_nullable_value<'a, T, R>(
        &self,
        row: &'a R,
    ) -> Result<Option<std::borrow::Cow<'a, T>>, crate::DecodeValueError>
    where
        R: crate::RowOperations<OperationId>,
        T: ?Sized + crate::Decode<OperationId>,
    {
        row.get_nullable_field_value(&self.name)
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_identifier(&self.name, f)?;
        write!(f, " {}", self.data_type)?;
        if !self.metadata.is_empty() {
            f.write_str(" ")?;
            write_tblproperties(&self.metadata, f)?;
        }
        Ok(())
    }
}

/// A data type with a particular set of resolution semantics under concurrent modification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicatedDataType {
    /// A convergent single value register.
    ///
    /// Current value is either the latest value, or under concurrent modification the latest value
    /// from the node with the highest id.
    ///
    /// Can hold any arbitrary data, but is prone to a form of lost update,
    /// in the sense that some values may never be observed by some nodes.
    /// All values are available from the type's internal history.
    LatestValueWins {
        /// Nullable value domain for the register.
        value_type: NullableBasicDataType,
    },
    /// A convergent string type.
    ///
    /// Does not suffer from lost updates, but merged values may be unintentional by any participant.
    ///
    /// Merging happens at the UTF-8 grapheme level, so not character splitting is possible.
    LinearString,
    /// A list implementation with similar semantics to [[ReplicatedDataType::LinearString]],
    /// but where values can be any primitive type, not just UTF-8 graphemes.
    LinearList { value_type: PrimitiveType },
    /// A unsigned integer monotonically incrementing counter.
    ///
    /// Always starts at 0, increments may be larger positive integers.
    ///
    /// Saturates at the max value of the underlying type without overflow.
    MonotonicCounter {
        /// If `true` this corresponds to a [[PrimitiveType::Byte]] underlying value,
        /// otherwise [[PrimitiveType::UInt]].
        small_range: bool,
    },
    /// Similar to a monotonic counter, but instead of applying integer increments,
    /// new values are sent and the largest value in the built-in order of the underlying type wins.
    TotalOrderRegister {
        value_type: PrimitiveType,
        /// Whether to use the built-in order ascending or descending for comparisons.
        ///
        /// I.e. for ascending order, highest value wins,
        /// for descending order, lowest value wins.
        direction: Direction,
    },
    /// Like the [[TotalOrderRegister]] but with an explicitly defined linear state space.
    ///
    /// Transitions are only possible from values with lower indices to values with higher indices,
    /// and in conflicts the value with the highest index wins.
    TotalOrderFiniteStateRegister {
        /// Nullable value domain for this finite-state register.
        value_type: NullablePrimitiveType,
        /// The exhaustive list of legal states in the order they can be transitioned between.
        states: values::NullablePrimitiveValueArray,
    },
}

impl fmt::Display for ReplicatedDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_value_type(f)?;
        if !self.is_nullable() {
            f.write_str(" NOT NULL")?;
        }
        f.write_str(" USING ")?;
        self.write_crdt_name(f)
    }
}

impl ReplicatedDataType {
    fn write_value_type(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LatestValueWins { value_type } => write!(f, "{}", value_type.value_type()),
            Self::LinearString => write!(f, "{}", PrimitiveType::String),
            Self::LinearList { value_type } => write!(
                f,
                "{}",
                BasicDataType::Array(Box::new(ArrayType {
                    element_type: *value_type
                }))
            ),
            Self::MonotonicCounter { small_range } => {
                if *small_range {
                    write!(f, "{}", PrimitiveType::Byte)
                } else {
                    write!(f, "{}", PrimitiveType::UInt)
                }
            }
            Self::TotalOrderRegister { value_type, .. } => write!(f, "{value_type}"),
            Self::TotalOrderFiniteStateRegister { value_type, .. } => {
                write!(f, "{}", value_type.value_type())
            }
        }
    }

    fn is_nullable(&self) -> bool {
        match self {
            Self::LatestValueWins { value_type } => value_type.is_nullable(),
            Self::LinearString => false,
            Self::LinearList { .. } => false,
            Self::MonotonicCounter { .. } => false,
            Self::TotalOrderRegister { .. } => false,
            Self::TotalOrderFiniteStateRegister { value_type, .. } => value_type.is_nullable(),
        }
    }

    fn write_crdt_name(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LatestValueWins { .. } => f.write_str("LATEST_VALUE_WINS"),
            Self::LinearString => f.write_str("LINEAR_STRING"),
            Self::LinearList { .. } => f.write_str("LINEAR_LIST"),
            Self::MonotonicCounter { .. } => f.write_str("MONOTONIC_COUNTER"),
            Self::TotalOrderRegister { direction, .. } => {
                write!(f, "TOTAL_ORDER_REGISTER({direction})")
            }
            Self::TotalOrderFiniteStateRegister { states, .. } => {
                write!(f, "TOTAL_ORDER_FSM({states})")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Smaller values come first in the ordering.
    Ascending,
    /// Larger values come first in the ordering.
    Descending,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ascending => f.write_str("ASC"),
            Self::Descending => f.write_str("DESC"),
        }
    }
}

/// A building-block data type without resolution method.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BasicDataType {
    /// UTF-8 encoded string of characters
    Primitive(PrimitiveType),
    /// An array stores a variable length collection of items of some type.
    Array(Box<ArrayType>),
}

impl fmt::Display for BasicDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Primitive(value_type) => write!(f, "{value_type}"),
            Self::Array(array_type) => write!(f, "{array_type}"),
        }
    }
}

/// Nullability wrapper for [[BasicDataType]].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NullableBasicDataType {
    NonNull(BasicDataType),
    Nullable(BasicDataType),
}
impl NullableBasicDataType {
    pub fn value_type(&self) -> &BasicDataType {
        match self {
            Self::NonNull(value_type) | Self::Nullable(value_type) => value_type,
        }
    }

    pub fn is_nullable(&self) -> bool {
        matches!(self, Self::Nullable(_))
    }
}

impl fmt::Display for NullableBasicDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value_type())?;
        if !self.is_nullable() {
            f.write_str(" NOT NULL")?;
        }
        Ok(())
    }
}

/// An array over primitive types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArrayType {
    /// The type of element stored in this array
    pub element_type: PrimitiveType,
}

impl fmt::Display for ArrayType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ARRAY<{}>", self.element_type)
    }
}

/// Primitive types and what they can represent.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PrimitiveType {
    /// UTF-8 encoded string of characters
    String,
    /// u64: 8-byte unsigned integer. Range 0 to 2^64-1
    UInt,
    /// i64: 8-byte signed integer. Range: -9223372036854775808 to 9223372036854775807
    Int,
    /// u8: 1-byte unsigned integer number. Range: 0 to 255
    Byte,
    /// f64: 8-byte double-precision floating-point numbers
    Float,
    /// bool: boolean values
    Boolean,
    /// A variable size array of bytes.
    Binary,
    /// A simple date, i.e year, month, day
    Date,
    /// Millisecond precision UNIX timestamp. UTC relative to the UNIX epoch.
    Timestamp,
}

impl fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String => f.write_str("STRING"),
            Self::UInt => f.write_str("U64"),
            Self::Int => f.write_str("I64"),
            Self::Byte => f.write_str("U8"),
            Self::Float => f.write_str("F64"),
            Self::Boolean => f.write_str("BOOL"),
            Self::Binary => f.write_str("BINARY"),
            Self::Date => f.write_str("DATE"),
            Self::Timestamp => f.write_str("TIMESTAMP"),
        }
    }
}

/// Nullability wrapper for [[PrimitiveType]].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NullablePrimitiveType {
    NonNull(PrimitiveType),
    Nullable(PrimitiveType),
}
impl NullablePrimitiveType {
    pub fn value_type(self) -> PrimitiveType {
        match self {
            Self::NonNull(value_type) | Self::Nullable(value_type) => value_type,
        }
    }

    pub fn is_nullable(self) -> bool {
        matches!(self, Self::Nullable(_))
    }
}

impl fmt::Display for NullablePrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value_type())?;
        if !self.is_nullable() {
            f.write_str(" NOT NULL")?;
        }
        Ok(())
    }
}

fn write_tblproperties(
    metadata: &HashMap<String, String>,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    f.write_str("TBLPROPERTIES (")?;
    let mut entries = metadata.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
    for (index, (key, value)) in entries.iter().enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }
        write_identifier(key, f)?;
        f.write_str("=")?;
        write_string_literal(value, f)?;
    }
    f.write_str(")")
}

fn write_identifier(identifier: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if is_valid_identifier(identifier) {
        return f.write_str(identifier);
    }

    f.write_str("`")?;
    for ch in identifier.chars() {
        if ch == '`' {
            f.write_str("``")?;
        } else {
            write!(f, "{ch}")?;
        }
    }
    f.write_str("`")
}

fn write_string_literal(value: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("'")?;
    for ch in value.chars() {
        if ch == '\'' {
            f.write_str("''")?;
        } else {
            write!(f, "{ch}")?;
        }
    }
    f.write_str("'")
}

fn is_valid_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !matches!(first, 'a'..='z' | 'A'..='Z' | '_') {
        return false;
    }
    chars.all(|ch| matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::values::{NULL, OrderedValue};

    #[test]
    fn schema_display_uses_sql_like_format() {
        let mut schema = Schema::from_fields([
            Field::linear_string("title"),
            Field::linear_list("tags", PrimitiveType::String),
            Field::monotonic_counter("counter"),
            Field::total_order_register("priority", PrimitiveType::UInt, Direction::Descending),
            Field::finite_state_register("status", ["draft".into(), NULL, "published".into()])
                .unwrap(),
            Field::latest_value_wins(
                "profile-bin",
                NullableBasicDataType::Nullable(BasicDataType::Array(Box::new(ArrayType {
                    element_type: PrimitiveType::Binary,
                }))),
            ),
        ]);
        schema
            .metadata
            .insert("owner".to_owned(), "sync".to_owned());
        schema.metadata.insert("version".to_owned(), "1".to_owned());

        assert_eq!(
            schema.to_string(),
            "SCHEMA (counter U64 NOT NULL USING MONOTONIC_COUNTER, priority U64 NOT NULL USING TOTAL_ORDER_REGISTER(DESC), `profile-bin` ARRAY<BINARY> USING LATEST_VALUE_WINS, status STRING USING TOTAL_ORDER_FSM(['draft', NULL, 'published']), tags ARRAY<STRING> NOT NULL USING LINEAR_LIST, title STRING NOT NULL USING LINEAR_STRING) TBLPROPERTIES (owner='sync', version='1')"
        );
    }

    #[test]
    fn schema_display_pretty_prints_with_alternate_flag() {
        let mut schema = Schema::from_fields([Field::latest_value_wins(
            "field-name",
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String)),
        )]);
        schema
            .metadata
            .insert("owner".to_owned(), "o'hara".to_owned());

        assert_eq!(
            format!("{schema:#}"),
            "SCHEMA (\n  `field-name` STRING USING LATEST_VALUE_WINS\n)\nTBLPROPERTIES (owner='o''hara')"
        );
    }

    #[test]
    fn replicated_data_type_display_elides_nullable_marker() {
        let value_type =
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String));
        assert_eq!(
            ReplicatedDataType::LatestValueWins { value_type }.to_string(),
            "STRING USING LATEST_VALUE_WINS"
        );
    }

    #[test]
    fn replicated_data_type_display_marks_non_nullable() {
        let data_type = ReplicatedDataType::TotalOrderRegister {
            value_type: PrimitiveType::UInt,
            direction: Direction::Ascending,
        };
        assert_eq!(
            data_type.to_string(),
            "U64 NOT NULL USING TOTAL_ORDER_REGISTER(ASC)"
        );
    }

    #[test]
    fn invalid_identifiers_are_backtick_quoted() {
        let mut field = Field::linear_string("with-hyphen");
        field.metadata.insert("x-y".to_owned(), "value".to_owned());
        assert_eq!(
            field.to_string(),
            "`with-hyphen` STRING NOT NULL USING LINEAR_STRING TBLPROPERTIES (`x-y`='value')"
        );
    }

    #[test]
    fn backticks_inside_identifiers_are_escaped() {
        let field = Field::linear_string("with`tick");
        assert_eq!(
            field.to_string(),
            "`with``tick` STRING NOT NULL USING LINEAR_STRING"
        );
    }

    #[test]
    fn ordered_value_strings_use_single_quotes() {
        let states =
            values::NullablePrimitiveValueArray::ordered([OrderedValue::from("in_review"), NULL])
                .unwrap();
        let data_type = ReplicatedDataType::TotalOrderFiniteStateRegister {
            value_type: states.value_type(),
            states,
        };
        assert_eq!(
            data_type.to_string(),
            "STRING USING TOTAL_ORDER_FSM(['in_review', NULL])"
        );
    }

    #[test]
    fn single_quotes_inside_states_are_escaped() {
        let states =
            values::NullablePrimitiveValueArray::ordered([OrderedValue::from("in'review"), NULL])
                .unwrap();
        let data_type = ReplicatedDataType::TotalOrderFiniteStateRegister {
            value_type: states.value_type(),
            states,
        };
        assert_eq!(
            data_type.to_string(),
            "STRING USING TOTAL_ORDER_FSM(['in''review', NULL])"
        );
    }

    #[test]
    #[should_panic(expected = "Invalid default value for field 'priority'")]
    fn schema_from_fields_rejects_invalid_defaults() {
        let mut field =
            Field::total_order_register("priority", PrimitiveType::UInt, Direction::Ascending);
        field.default_value = Some("wrong-type".into());
        let _schema = Schema::from_fields([field]);
    }
}
