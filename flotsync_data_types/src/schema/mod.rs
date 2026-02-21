//! The flotsync data model is based around strict [[Schema]]s which specify both
//! the underlying storage type and value domain as well as the resolution semantics under
//! concurrent modification.
use std::collections::HashMap;

pub mod datamodel;
pub mod values;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// Name of this column.
    pub name: String,
    /// The data type of this field.
    pub data_type: ReplicatedDataType,
    /// A map containing information about this column.
    pub metadata: HashMap<String, String>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Smaller values come first in the ordering.
    Ascending,
    /// Larger values come first in the ordering.
    Descending,
}

/// A building-block data type without resolution method.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BasicDataType {
    /// UTF-8 encoded string of characters
    Primitive(PrimitiveType),
    /// An array stores a variable length collection of items of some type.
    Array(Box<ArrayType>),
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

/// An array over primitive types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArrayType {
    /// The type of element stored in this array
    pub element_type: PrimitiveType,
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
