//! Schema snapshot reader sources and decoder contracts.

#[allow(
    clippy::wildcard_imports,
    reason = "The private helper module intentionally shares its parent's local implementation vocabulary."
)]
use super::*;

/// Streaming node source used by snapshot decoders.
///
/// This is the decode counterpart to `SnapshotSink`, yielding one owned node at a time.
pub trait SnapshotNodeSource<Id, Value> {
    type Error;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn next_node(&mut self) -> Result<Option<SnapshotNode<Id, Value>>, Self::Error>;
}

/// Iterator adapter over a `SnapshotNodeSource`.
pub(crate) struct SnapshotNodeSourceIter<'a, Source, Id, Value>
where
    Source: SnapshotNodeSource<Id, Value>,
{
    source: &'a mut Source,
    _marker: PhantomData<fn(Id, Value)>,
}
impl<'a, Source, Id, Value> SnapshotNodeSourceIter<'a, Source, Id, Value>
where
    Source: SnapshotNodeSource<Id, Value>,
{
    pub(crate) fn new(source: &'a mut Source) -> Self {
        Self {
            source,
            _marker: PhantomData,
        }
    }
}
impl<Source, Id, Value> Iterator for SnapshotNodeSourceIter<'_, Source, Id, Value>
where
    Source: SnapshotNodeSource<Id, Value>,
{
    type Item = Result<SnapshotNode<Id, Value>, Source::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.source.next_node() {
            Ok(Some(node)) => Some(Ok(node)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

/// Decoder for one complete row snapshot across schema fields.
///
/// Intended flow:
/// 1. `begin(expected_field_count)`
/// 2. decode each schema field via `decode_state_field` or `prepare_*_field`
/// 3. for history fields, consume node streams lazily via `SnapshotNodeSourceIter`
/// 4. `end`
pub trait SchemaSnapshotDecoder<Id> {
    type Error: snafu::Error + Send + Sync + 'static;

    type LatestValueWinsFieldSource<'a>: SnapshotNodeSource<IdWithIndex<Id>, NullableBasicValue, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    type LinearStringFieldSource<'a>: SnapshotNodeSource<IdWithIndex<Id>, String, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    type LinearListFieldSource<'a>: SnapshotNodeSource<IdWithIndex<Id>, PrimitiveValueArray, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn begin(&mut self, expected_field_count: usize) -> Result<(), Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn decode_state_field(
        &mut self,
        field_name: &str,
        data_type: &ReplicatedDataType,
    ) -> Result<StateSnapshotFieldValue, Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn prepare_latest_value_wins_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: &NullableBasicDataType,
    ) -> Result<Self::LatestValueWinsFieldSource<'a>, Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn prepare_linear_string_field<'a>(
        &'a mut self,
        field_name: &str,
    ) -> Result<Self::LinearStringFieldSource<'a>, Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn prepare_linear_list_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: PrimitiveType,
    ) -> Result<Self::LinearListFieldSource<'a>, Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn end(&mut self) -> Result<(), Self::Error>;
}

/// Dataset-level encoder that streams complete row snapshots.
///
/// Intended flow:
/// 1. `begin(row_count)`
/// 2. repeat for each row index:
///    `begin_row` -> row `SchemaSnapshotEncoder` -> row `end` -> `end_row`
/// 3. `end`
pub trait DataSnapshotEncoder<Id> {
    type Error: snafu::Error + Send + Sync + 'static;

    type RowEncoder<'a>: SchemaSnapshotEncoder<Id, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn begin(&mut self, row_count: usize) -> Result<(), Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn begin_row(&mut self, row_index: usize) -> Result<Self::RowEncoder<'_>, Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn end_row(&mut self, row_index: usize) -> Result<(), Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn end(&mut self) -> Result<(), Self::Error>;
}

/// Dataset-level decoder that streams complete row snapshots lazily.
///
/// Intended flow:
/// 1. `begin()` -> row count
/// 2. repeat for each row index:
///    `begin_row` -> row `SchemaSnapshotDecoder` -> row `end` -> `end_row`
/// 3. `end`
pub trait DataSnapshotDecoder<Id> {
    type Error: snafu::Error + Send + Sync + 'static;

    type RowDecoder<'a>: SchemaSnapshotDecoder<Id, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn begin(&mut self) -> Result<usize, Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn begin_row(&mut self, row_index: usize) -> Result<Self::RowDecoder<'_>, Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn end_row(&mut self, row_index: usize) -> Result<(), Self::Error>;

    /// # Errors
    ///
    /// See `Self::Error` for failure conditions.
    fn end(&mut self) -> Result<(), Self::Error>;
}
