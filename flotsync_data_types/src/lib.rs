#![feature(vec_from_fn)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
pub use chrono;
use snafu::{Location, prelude::*};
use std::borrow::Cow;

pub mod any_data;
#[allow(unused, reason = "Might re-use some already implemented things later.")]
mod linear_data;
pub mod schema;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod text;
pub mod snapshot {
    pub use crate::linear_data::snapshot::*;
}

pub use linear_data::{DataOperation, IdWithIndex, IdWithIndexRange, IntegrityError};
pub use schema::{
    Direction,
    Field,
    InitialFieldValue,
    NULL,
    OrderedValue,
    OrderedValueError,
    PendingFieldUpdate,
    PrimitiveType,
    ReplicatedDataType,
    Schema,
    datamodel::{InMemoryFieldValue, NullableBasicValue, SchemaOperation},
};

#[derive(Debug, Snafu)]
#[snafu(display("The diff could not be applied due to a logic error at {location}: {context}"))]
pub struct InternalError {
    context: String,
    #[snafu(implicit)]
    location: Location,
}

#[derive(Debug, Snafu)]
pub enum OperationError {
    #[snafu(display(
        "An unusupported operation variant was encountered at {location}. {explanation}"
    ))]
    UnsupportedOperationVariant {
        explanation: Cow<'static, str>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("The operation targets a row that does not exist: {row_id}."))]
    UnknownRowId { row_id: String },
    #[snafu(display("Cannot modify row {row_id} because it has already been deleted."))]
    ModifyDeletedRow { row_id: String },
    #[snafu(display("The operation targets a row that already exists: {row_id}."))]
    DuplicateRowId { row_id: String },
    #[snafu(display("The operation could not be applied to the in-memory dataset."))]
    InMemoryData {
        source: schema::datamodel::InMemoryDataError,
    },
    #[snafu(display("The operation is invalid for the schema."))]
    SchemaValue {
        source: schema::datamodel::SchemaValueError,
    },
    #[snafu(display("The requested linear string change could not be represented."))]
    LinearStringDiff { source: text::DiffError },
    #[snafu(display("The requested linear list change could not be represented."))]
    LinearListDiff { source: any_data::list::DiffError },
    #[snafu(display("The operation failed due to a logic error at {location}: {context}"))]
    InternalOperation {
        context: String,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug, Snafu)]
pub enum DecodeValueError {
    #[snafu(display("Field '{field_name}' does not exist."))]
    FieldDoesNotExist { field_name: String },
    #[snafu(display("Cannot decode NULL as {requested_type}."))]
    NullValue { requested_type: &'static str },
    #[snafu(display("Cannot decode {actual_type} as {requested_type}."))]
    TypeMismatch {
        requested_type: &'static str,
        actual_type: Cow<'static, str>,
    },
    #[snafu(display("Cannot convert value {value} from {source_type} to {requested_type}."))]
    ConversionFailed {
        requested_type: &'static str,
        source_type: &'static str,
        value: String,
    },
    #[snafu(display("Cannot decode {requested_type}: {explanation}."))]
    InvalidValue {
        requested_type: &'static str,
        explanation: Cow<'static, str>,
    },
}

/// A marker trait that a type can be extracted from an [[InMemoryFieldValue]].
pub trait Decode<OperationId>: ToOwned {
    fn decode<'a>(
        value: &'a InMemoryFieldValue<OperationId>,
    ) -> Result<Cow<'a, Self>, DecodeValueError>;
}

pub type OperationResult<T> = Result<T, OperationError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationOutcome<T> {
    Applied(T),
    NoChanges,
}

/// Methods to get values out of rows.
pub trait RowOperations<OperationId> {
    /// Get the current value of a field.
    ///
    /// Returns `None` if the field does not exist.
    fn get_field(&self, field_name: &str) -> Option<&InMemoryFieldValue<OperationId>>;

    /// Get the current value of the field with `field_name` converted to `T` (owned or reference, as feasible).
    ///
    /// Returns `Err(DecodeValueError::FieldDoesNotExist)` if the field does not exist.
    fn get_field_value<T>(&self, field_name: &str) -> Result<Cow<'_, T>, DecodeValueError>
    where
        T: ?Sized + Decode<OperationId>;

    /// Get the current value of the field with `field_name` converted to `T` (owned or reference, as feasible).
    ///
    /// Returns `Ok(None)` if the field is `NULL`.
    ///
    /// Returns `Err(DecodeValueError::FieldDoesNotExist)` if the field does not exist.
    fn get_nullable_field_value<T>(
        &self,
        field_name: &str,
    ) -> Result<Option<Cow<'_, T>>, DecodeValueError>
    where
        T: ?Sized + Decode<OperationId>;
}

/// This is equivalent to [[RowOperations]] but operating directly on schema fields.
pub trait FieldOperations<OperationId> {
    /// Get the current value of this field in `row`.
    ///
    /// Panics if `row` does not contain this field (i.e is from a different schema.)
    fn get_from_row<'a, R>(&self, row: &'a R) -> &'a InMemoryFieldValue<OperationId>
    where
        R: RowOperations<OperationId>;

    /// Get the current value of this field in `row` converted to `T` (owned or reference, as feasible).
    fn get_value<'a, R, T>(&self, row: &'a R) -> Result<Cow<'a, T>, DecodeValueError>
    where
        R: RowOperations<OperationId>,
        T: ?Sized + Decode<OperationId>;

    /// Get the current value of the field with `field_name` converted to `T` (owned or reference, as feasible).
    ///
    /// Returns `Ok(None)` if the field is `NULL`.
    fn get_nullable_value<'a, T, R>(
        &self,
        row: &'a R,
    ) -> Result<Option<Cow<'a, T>>, DecodeValueError>
    where
        R: RowOperations<OperationId>,
        T: ?Sized + Decode<OperationId>;
}

/// Operations that can be performed on table with a given [[Schema]].
///
/// All operations apply to the underlying data and return a [[SchemaOperation]] that can be sent
/// over the network to be applied at other nodes.
pub trait TableOperations<RowId, OperationId> {
    /// A representation of a single row in this table.
    type Row<'a>: RowOperations<OperationId>
    where
        Self: 'a;

    /// The schema used by this table.
    fn schema(&self) -> &Schema;

    /// Look up the row identified by `row_id`.
    fn get_row(&self, row_id: &RowId) -> Option<Self::Row<'_>>;

    /// Insert a fresh row identified by `row_id` and with the `initial_values`.
    ///
    /// All fields declared in the schema *must* have initial values or the insert will be rejected.
    /// If any field has more than one value provided, the last one will be used.
    ///
    /// It is crucial to ensure that `operation_id` is globally unique to prevent insert/insert type
    /// conflicts, which are not resolvable.
    fn insert_row<'a, I>(
        &mut self,
        operation_id: OperationId,
        row_id: RowId,
        initial_values: I,
    ) -> OperationResult<SchemaOperation<'_, RowId, OperationId>>
    where
        I: IntoIterator<Item = schema::InitialFieldValue<'a>>;

    /// Change the given `changed_values` in the row identified with `row_id`.
    ///
    /// Unspecified fields will remain unchanged.
    ///
    /// If any field has more than one value provided, the last one will be used.
    fn modify_row<'a, I>(
        &mut self,
        operation_id: OperationId,
        row_id: RowId,
        changed_values: I,
    ) -> OperationResult<OperationOutcome<SchemaOperation<'_, RowId, OperationId>>>
    where
        I: IntoIterator<Item = schema::PendingFieldUpdate<'a>>;

    /// Delete the existing row with `row_id`.
    fn delete_row(
        &mut self,
        operation_id: OperationId,
        row_id: RowId,
    ) -> OperationResult<SchemaOperation<'_, RowId, OperationId>>;
}

#[macro_export]
macro_rules! initial_values {
    ($($field:expr => $value:expr),* $(,)?) => {
        [$(($field).initial($value).unwrap(),)*]
    };
}

#[macro_export]
macro_rules! update_values {
    ($($field:expr => $value:expr),* $(,)?) => {
        [$(($field).set($value).unwrap(),)*]
    };
}
