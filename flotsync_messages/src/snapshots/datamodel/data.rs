//! Protobuf adapters for multi-row dataset snapshots.

use super::*;

/// Row-scoped proxy around `ProtoSchemaSnapshotEncoder`.
///
/// This exists to bridge trait-associated lifetimes cleanly for the history field sink types.
pub struct ProtoDataSnapshotRowEncoder<'schema, 'row> {
    inner: &'row mut ProtoSchemaSnapshotEncoder<'schema>,
}

impl<'schema> SchemaSnapshotEncoder<UpdateId> for ProtoDataSnapshotRowEncoder<'schema, '_> {
    type Error = SnapshotAdapterError;

    type LatestValueWinsFieldSink<'a>
        = <ProtoSchemaSnapshotEncoder<'schema> as SchemaSnapshotEncoder<UpdateId>>::LatestValueWinsFieldSink<'a>
    where
        Self: 'a;

    type LinearStringFieldSink<'a>
        = <ProtoSchemaSnapshotEncoder<'schema> as SchemaSnapshotEncoder<UpdateId>>::LinearStringFieldSink<
        'a,
    >
    where
        Self: 'a;

    type LinearListFieldSink<'a>
        =
        <ProtoSchemaSnapshotEncoder<'schema> as SchemaSnapshotEncoder<UpdateId>>::LinearListFieldSink<'a>
    where
        Self: 'a;

    fn begin(&mut self, field_count: usize) -> Result<(), Self::Error> {
        self.inner.begin(field_count)
    }

    fn state_field(
        &mut self,
        field_name: &str,
        value: StateSnapshotFieldValueRef<'_>,
    ) -> Result<(), Self::Error> {
        self.inner.state_field(field_name, value)
    }

    fn prepare_latest_value_wins_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: &NullableBasicDataType,
    ) -> Result<Self::LatestValueWinsFieldSink<'a>, Self::Error> {
        self.inner
            .prepare_latest_value_wins_field(field_name, value_type)
    }

    fn prepare_linear_string_field<'a>(
        &'a mut self,
        field_name: &str,
    ) -> Result<Self::LinearStringFieldSink<'a>, Self::Error> {
        self.inner.prepare_linear_string_field(field_name)
    }

    fn prepare_linear_list_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: PrimitiveType,
    ) -> Result<Self::LinearListFieldSink<'a>, Self::Error> {
        self.inner.prepare_linear_list_field(field_name, value_type)
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        self.inner.end()
    }
}

/// Protobuf-backed implementation of `DataSnapshotEncoder` for dataset snapshots.
pub struct ProtoDataSnapshotEncoder<'schema> {
    schema: &'schema Schema,
    snapshot: proto::DataSnapshot,
    expected_row_count: Option<usize>,
    current_row_index: Option<usize>,
    current_row: Option<ProtoSchemaSnapshotEncoder<'schema>>,
}

impl<'schema> ProtoDataSnapshotEncoder<'schema> {
    /// Create an empty dataset encoder bound to `schema`.
    #[must_use]
    pub fn new(schema: &'schema Schema) -> Self {
        Self {
            schema,
            snapshot: proto::DataSnapshot::default(),
            expected_row_count: None,
            current_row_index: None,
            current_row: None,
        }
    }

    /// # Errors
    ///
    /// See `SnapshotAdapterError` for failure conditions.
    pub fn into_snapshot(self) -> Result<proto::DataSnapshot, SnapshotAdapterError> {
        self.validate_finished()?;
        Ok(self.snapshot)
    }

    fn validate_finished(&self) -> Result<(), SnapshotAdapterError> {
        let expected = self.expected_row_count.context(BeginRequiredSnafu {
            target: "data snapshot encoder",
        })?;
        ensure!(self.current_row.is_none(), RowAlreadyOpenSnafu);
        ensure!(
            self.snapshot.rows.len() == expected,
            FieldCountMismatchSnafu {
                expected,
                actual: self.snapshot.rows.len(),
            }
        );
        Ok(())
    }
}

impl<'schema> flotsync_data_types::schema::datamodel::DataSnapshotEncoder<UpdateId>
    for ProtoDataSnapshotEncoder<'schema>
{
    type Error = SnapshotAdapterError;

    type RowEncoder<'a>
        = ProtoDataSnapshotRowEncoder<'schema, 'a>
    where
        Self: 'a;

    fn begin(&mut self, row_count: usize) -> Result<(), Self::Error> {
        ensure!(
            self.expected_row_count.is_none(),
            AlreadyBegunSnafu {
                target: "data snapshot encoder",
            }
        );
        self.expected_row_count = Some(row_count);
        self.snapshot.rows = Vec::with_capacity(row_count);
        Ok(())
    }

    fn begin_row(&mut self, row_index: usize) -> Result<Self::RowEncoder<'_>, Self::Error> {
        let row_count = self.expected_row_count.context(BeginRequiredSnafu {
            target: "data snapshot encoder",
        })?;
        ensure!(self.current_row.is_none(), RowAlreadyOpenSnafu);
        ensure!(
            row_index < row_count,
            RowOutOfBoundsSnafu {
                row_index,
                row_count,
            }
        );
        self.current_row_index = Some(row_index);
        self.current_row = Some(ProtoSchemaSnapshotEncoder::new(self.schema));
        Ok(ProtoDataSnapshotRowEncoder {
            inner: self.current_row.as_mut().expect("row was inserted"),
        })
    }

    fn end_row(&mut self, row_index: usize) -> Result<(), Self::Error> {
        let open_row = self.current_row_index.context(NoOpenRowSnafu)?;
        ensure!(
            open_row == row_index,
            WrongOpenRowSnafu {
                row_index,
                open_row
            }
        );
        let row = self.current_row.take().context(NoOpenRowSnafu)?;
        self.current_row_index = None;
        self.snapshot.rows.push(row.into_row_snapshot()?);
        Ok(())
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        self.validate_finished()
    }
}

/// Protobuf-backed implementation of `DataSnapshotDecoder` for dataset snapshots.
pub struct ProtoDataSnapshotDecoder {
    rows: Vec<Option<proto::RowSnapshot>>,
    began: bool,
    open_row: Option<usize>,
}

impl ProtoDataSnapshotDecoder {
    /// Create a dataset decoder that consumes `snapshot` destructively.
    pub fn new(snapshot: proto::DataSnapshot) -> Self {
        Self {
            rows: snapshot.rows.into_iter().map(Some).collect(),
            began: false,
            open_row: None,
        }
    }
}

impl flotsync_data_types::schema::datamodel::DataSnapshotDecoder<UpdateId>
    for ProtoDataSnapshotDecoder
{
    type Error = SnapshotAdapterError;

    type RowDecoder<'a>
        = ProtoSchemaSnapshotDecoder
    where
        Self: 'a;

    fn begin(&mut self) -> Result<usize, Self::Error> {
        ensure!(
            !self.began,
            AlreadyBegunSnafu {
                target: "data snapshot decoder"
            }
        );
        self.began = true;
        Ok(self.rows.len())
    }

    fn begin_row(&mut self, row_index: usize) -> Result<Self::RowDecoder<'_>, Self::Error> {
        ensure!(
            self.began,
            BeginRequiredSnafu {
                target: "data snapshot decoder"
            }
        );
        ensure!(self.open_row.is_none(), RowAlreadyOpenSnafu);
        ensure!(
            row_index < self.rows.len(),
            RowOutOfBoundsSnafu {
                row_index,
                row_count: self.rows.len(),
            }
        );
        let row = self.rows[row_index]
            .take()
            .context(RowAlreadyConsumedSnafu { row_index })?;
        self.open_row = Some(row_index);
        ProtoSchemaSnapshotDecoder::new(row)
    }

    fn end_row(&mut self, row_index: usize) -> Result<(), Self::Error> {
        let open_row = self.open_row.context(NoOpenRowSnafu)?;
        ensure!(
            open_row == row_index,
            WrongOpenRowSnafu {
                row_index,
                open_row
            }
        );
        self.open_row = None;
        Ok(())
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        ensure!(
            self.began,
            BeginRequiredSnafu {
                target: "data snapshot decoder"
            }
        );
        ensure!(self.open_row.is_none(), RowAlreadyOpenSnafu);
        let remaining_rows: Vec<usize> = self
            .rows
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.as_ref().map(|_| index))
            .collect();
        ensure!(
            remaining_rows.is_empty(),
            RemainingRowsSnafu {
                row_indices: remaining_rows,
            }
        );
        Ok(())
    }
}
