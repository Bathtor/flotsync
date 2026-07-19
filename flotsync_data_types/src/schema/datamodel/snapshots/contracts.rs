//! Snapshot data contracts and visitor traits.

use super::*;

/// Snapshot node payload shapes for history-based CRDT snapshots.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum HistorySnapshotNodeValue {
    LatestValueWins(NullableBasicValue),
    LinearString(String),
    LinearList(PrimitiveValueArray),
}
#[cfg(test)]
impl HistorySnapshotNodeValue {
    pub(super) fn as_ref(&self) -> HistorySnapshotNodeValueRef<'_> {
        match self {
            Self::LatestValueWins(value) => {
                HistorySnapshotNodeValueRef::LatestValueWins(value.as_ref())
            }
            Self::LinearString(value) => HistorySnapshotNodeValueRef::LinearString(value.as_str()),
            Self::LinearList(values) => HistorySnapshotNodeValueRef::LinearList(values.as_ref()),
        }
    }
}

/// Borrowed snapshot node payload shapes for history-based CRDT snapshots.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum HistorySnapshotNodeValueRef<'a> {
    LatestValueWins(NullableBasicValueRef<'a>),
    LinearString(&'a str),
    LinearList(PrimitiveValueArrayRef<'a>),
}

/// Snapshot value shapes for state-based CRDT snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateSnapshotFieldValue {
    MonotonicCounter(CounterValue),
    TotalOrderRegister(PrimitiveValue),
    TotalOrderFiniteStateRegister(NullablePrimitiveValue),
}
impl StateSnapshotFieldValue {
    #[must_use]
    pub fn as_ref(&self) -> StateSnapshotFieldValueRef<'_> {
        match self {
            Self::MonotonicCounter(value) => {
                StateSnapshotFieldValueRef::MonotonicCounter(value.as_ref())
            }
            Self::TotalOrderRegister(value) => {
                StateSnapshotFieldValueRef::TotalOrderRegister(value.as_ref())
            }
            Self::TotalOrderFiniteStateRegister(value) => {
                StateSnapshotFieldValueRef::TotalOrderFiniteStateRegister(value.as_ref())
            }
        }
    }
}

/// Borrowed snapshot value shapes for state-based CRDT snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateSnapshotFieldValueRef<'a> {
    MonotonicCounter(CounterValueRef),
    TotalOrderRegister(PrimitiveValueRef<'a>),
    TotalOrderFiniteStateRegister(NullablePrimitiveValueRef<'a>),
}
impl StateSnapshotFieldValueRef<'_> {
    #[must_use]
    pub fn into_owned(self) -> StateSnapshotFieldValue {
        match self {
            Self::MonotonicCounter(value) => {
                StateSnapshotFieldValue::MonotonicCounter(value.into_owned())
            }
            Self::TotalOrderRegister(value) => {
                StateSnapshotFieldValue::TotalOrderRegister(value.to_owned())
            }
            Self::TotalOrderFiniteStateRegister(value) => {
                StateSnapshotFieldValue::TotalOrderFiniteStateRegister(value.to_owned())
            }
        }
    }
}

/// Visitor for complete-schema snapshots.
///
/// History-backed fields are prepared once and then receive node streams through a
/// `SnapshotSink` implementation that is specialized for that field type.
///
/// Intended flow:
/// 1. `prepare_schema_snapshot_encoder` (or manual `begin`)
/// 2. `state_field` and `prepare_*_field` as needed
/// 3. stream history nodes via `SnapshotSink::begin/node/end` (typically by calling CRDT
///    `encode_snapshot` directly on the prepared field sink)
/// 4. `end`
pub trait SchemaSnapshotEncoder<Id> {
    type Error: snafu::Error + Send + Sync + 'static;

    type LatestValueWinsFieldSink<'a>: for<'value> SnapshotSink<
            IdWithIndex<Id>,
            NullableBasicValueRef<'value>,
            Error = Self::Error,
        >
    where
        Self: 'a,
        Id: 'a;

    type LinearStringFieldSink<'a>: SnapshotSink<IdWithIndex<Id>, str, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    type LinearListFieldSink<'a>: for<'value> SnapshotSink<
            IdWithIndex<Id>,
            PrimitiveValueArrayRef<'value>,
            Error = Self::Error,
        >
    where
        Self: 'a,
        Id: 'a;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn begin(&mut self, field_count: usize) -> Result<(), Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn state_field(
        &mut self,
        field_name: &str,
        value: StateSnapshotFieldValueRef<'_>,
    ) -> Result<(), Self::Error>;

    /// Prepare a sink for one `LatestValueWins` field.
    ///
    /// The returned sink is fed using `SnapshotSink::begin/node/end`.
    ///
    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn prepare_latest_value_wins_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: &NullableBasicDataType,
    ) -> Result<Self::LatestValueWinsFieldSink<'a>, Self::Error>;

    /// Prepare a sink for one `LinearString` field.
    ///
    /// The returned sink is fed using `SnapshotSink::begin/node/end`.
    ///
    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn prepare_linear_string_field<'a>(
        &'a mut self,
        field_name: &str,
    ) -> Result<Self::LinearStringFieldSink<'a>, Self::Error>;

    /// Prepare a sink for one `LinearList` field.
    ///
    /// The returned sink is fed using `SnapshotSink::begin/node/end`.
    ///
    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn prepare_linear_list_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: PrimitiveType,
    ) -> Result<Self::LinearListFieldSink<'a>, Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn end(&mut self) -> Result<(), Self::Error>;
}
