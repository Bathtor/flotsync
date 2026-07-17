#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
pub use chrono;
use snafu::{Location, prelude::*};
use std::{borrow::Cow, collections::HashMap, fmt, hash::Hash};

pub mod any_data;
#[allow(unused, reason = "Might re-use some already implemented things later.")]
mod linear_data;
pub mod row_values;
pub mod schema;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod text;
pub mod snapshot {
    pub use crate::linear_data::snapshot::*;
}

pub use linear_data::{DataOperation, IdWithIndex, IdWithIndexRange, IntegrityError};
pub use row_values::{
    Decode,
    InMemoryValueData,
    InMemoryValueDataError,
    InMemoryValueDataRowRef,
    InMemoryValueRowMeta,
    ProjectedFieldValue,
    RowOperations,
    RowValueRead,
    RowValues,
};
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
    datamodel::{InMemoryFieldState, NullableBasicValue, SchemaOperation},
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
    InMemoryStateData {
        source: schema::datamodel::InMemoryStateDataError,
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

pub type OperationResult<T> = Result<T, OperationError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationOutcome<T> {
    Applied(T),
    NoChanges,
}

/// Object-safe read-only view over row field state.
pub trait RowStateRead<OperationId> {
    /// Get the current state of a field.
    ///
    /// Returns `None` if the field does not exist.
    fn get_field(&self, field_name: &str) -> Option<&InMemoryFieldState<OperationId>>;
}

/// Owned immutable row state snapshot that can outlive any backing in-memory store.
#[derive(Clone, Debug, PartialEq)]
pub struct OwnedStateRow<OperationId> {
    fields: HashMap<String, InMemoryFieldState<OperationId>>,
}

impl<OperationId> OwnedStateRow<OperationId> {
    #[must_use]
    pub fn new(fields: HashMap<String, InMemoryFieldState<OperationId>>) -> Self {
        Self { fields }
    }
}

impl<OperationId> RowStateRead<OperationId> for OwnedStateRow<OperationId> {
    fn get_field(&self, field_name: &str) -> Option<&InMemoryFieldState<OperationId>> {
        self.fields.get(field_name)
    }
}

impl<OperationId> RowValueRead for OwnedStateRow<OperationId>
where
    OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    fn get_value(&self, field_name: &str) -> Option<ProjectedFieldValue<'_>> {
        self.fields
            .get(field_name)
            .map(InMemoryFieldState::project_value)
    }
}

/// State access helper for schema fields.
pub trait FieldStateReadExt<OperationId> {
    /// Get the current state of this field in `row`.
    ///
    /// Panics if `row` does not contain this field (i.e is from a different schema.)
    fn get_from_row<'a, R>(&self, row: &'a R) -> &'a InMemoryFieldState<OperationId>
    where
        R: RowStateRead<OperationId>;
}

/// Typed value access helper for schema fields.
pub trait FieldValueReadExt {
    /// Get the current value of this field in `row` converted to `T` (owned or reference, as feasible).
    ///
    /// # Errors
    ///
    /// See `DecodeValueError` for failure conditions.
    fn get_value<'a, R, T>(&self, row: &'a R) -> Result<Cow<'a, T>, DecodeValueError>
    where
        R: RowOperations,
        T: ?Sized + Decode;

    /// Get the current value of the field with `field_name` converted to `T` (owned or reference, as feasible).
    ///
    /// Returns `Ok(None)` if the field is `NULL`.
    ///
    /// # Errors
    ///
    /// See `DecodeValueError` for failure conditions.
    fn get_nullable_value<'a, T, R>(
        &self,
        row: &'a R,
    ) -> Result<Option<Cow<'a, T>>, DecodeValueError>
    where
        R: RowOperations,
        T: ?Sized + Decode;
}

/// Operations that can be performed on table with a given [[Schema]].
///
/// All operations apply to the underlying data and return a [[`SchemaOperation`]] that can be sent
/// over the network to be applied at other nodes.
pub trait TableOperations<RowId, OperationId> {
    /// A representation of a single row in this table.
    type Row<'a>: RowOperations
    where
        Self: 'a;

    /// The schema used by this table.
    fn schema(&self) -> &Schema;

    /// Look up the row identified by `row_id`.
    fn get_row(&self, row_id: &RowId) -> Option<Self::Row<'_>>;

    /// Insert a fresh row identified by `row_id` and with the `initial_values`.
    ///
    /// Any field omitted from `initial_values` will use its schema-level default when defined.
    /// If any field has more than one value provided, the last one will be used.
    ///
    /// It is crucial to ensure that `operation_id` is globally unique to prevent insert/insert type
    /// conflicts, which are not resolvable.
    ///
    /// # Errors
    ///
    /// If a field is omitted and has no schema default, the insert will be rejected.
    /// See `OperationError` for failure conditions.
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
    ///
    /// # Errors
    ///
    /// See `OperationError` for failure conditions.
    fn modify_row<'a, I>(
        &mut self,
        operation_id: OperationId,
        row_id: RowId,
        changed_values: I,
    ) -> OperationResult<OperationOutcome<SchemaOperation<'_, RowId, OperationId>>>
    where
        I: IntoIterator<Item = schema::PendingFieldUpdate<'a>>;

    /// Delete the existing row with `row_id`.
    ///
    /// # Errors
    ///
    /// See `OperationError` for failure conditions.
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
