//! Change publication requests and provider contracts.

use super::*;
use crate::codecs::ReadTokenProtoCodec;
use base64::engine::general_purpose::STANDARD;
use bytes::{BufMut as _, Bytes, BytesMut};
use flotsync_core::SortedArrayMap;
use flotsync_messages::proto::{DecodeProto, EncodeProto};

/// Outer format discriminator for a protobuf read-token payload.
const READ_TOKEN_PROTOBUF_FORMAT_V1: u8 = 1;

/// Write-only row payload submitted by applications.
///
/// For a new row this must contain the initial values required by the dataset
/// schema. For an existing row this is a sparse field patch: fields omitted from
/// `fields` are intentionally left unchanged by `publish_changes`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowValuesPatch {
    /// Desired nullable field values keyed by field name.
    pub fields: HashMap<String, NullableBasicValue>,
}

impl RowValuesPatch {
    #[must_use]
    pub fn new(fields: HashMap<String, NullableBasicValue>) -> Self {
        Self { fields }
    }
}

/// A row-level mutation submitted by an application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowMutation {
    /// Insert a new row or update the provided fields of an existing row.
    Upsert { row_id: RowId, row: RowValuesPatch },
    /// Tombstone an existing row.
    Delete { row_id: RowId },
}

impl RowMutation {
    #[must_use]
    pub fn row_id(&self) -> &RowId {
        match self {
            RowMutation::Upsert { row_id, .. } | RowMutation::Delete { row_id } => row_id,
        }
    }
}

/// Opaque read-position token returned by the replication runtime.
///
/// Applications should store this value alongside their application
/// state and pass it back to [`ReplicationApi::publish_changes`] and other APIs.
#[derive(Clone, PartialEq, Eq)]
pub struct ReadToken {
    // Developer note: the token intentionally hides its group-scoped version
    // vectors from applications. Runtime internals remain responsible for
    // validating group compatibility and advancing the right producer position.
    versions: Arc<ReadTokenVersions>,
}

impl ReadToken {
    pub(crate) fn from_group_versions(groups: HashMap<GroupId, VersionVector>) -> Self {
        let groups =
            SortedArrayMap::try_from_entries(groups).expect("hash map has no duplicate keys");
        Self::from_sorted_group_versions(groups)
    }

    pub(crate) fn from_sorted_group_versions(
        groups: SortedArrayMap<GroupId, VersionVector>,
    ) -> Self {
        Self {
            versions: Arc::new(ReadTokenVersions { groups }),
        }
    }

    /// Encode this token as canonical opaque bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Bytes {
        // 65 bytes is enough to hold the version plus a small 2 group token with a few members
        // per group without reallocating.
        let mut output = BytesMut::with_capacity(65);
        output.put_u8(READ_TOKEN_PROTOBUF_FORMAT_V1);
        ReadTokenProtoCodec::from(&self.versions.groups).encode_proto_into(&mut output);
        output.freeze()
    }

    /// Decode an opaque token previously returned by the replication runtime.
    ///
    /// # Errors
    ///
    /// Returns [`ReadTokenDecodeError`] when `input` is not a structurally valid
    /// read-token encoding supported by this runtime.
    pub fn from_bytes(input: &[u8]) -> Result<Self, ReadTokenDecodeError> {
        let groups = Self::decode_group_versions(input)
            .boxed()
            .context(ReadTokenDecodeSnafu)?;
        Ok(Self::from_sorted_group_versions(groups))
    }

    pub(crate) fn group_version(&self, group_id: &GroupId) -> Option<&VersionVector> {
        self.versions.groups.get(group_id)
    }

    pub(crate) fn group_count(&self) -> usize {
        self.versions.groups.len()
    }

    pub(crate) fn with_group_version(
        &self,
        group_id: GroupId,
        version_vector: VersionVector,
    ) -> Self {
        let mut groups = self.versions.groups.clone();
        groups.insert(group_id, version_vector);
        Self::from_sorted_group_versions(groups)
    }

    pub(crate) fn with_update_applied(&self, group_id: GroupId, update_id: UpdateId) -> Self {
        let Some(group_versions) = self.group_version(&group_id) else {
            return self.clone();
        };
        self.with_group_version(group_id, group_versions.with_update_applied(update_id))
    }

    /// Merge an event or snapshot token into this application read position.
    ///
    /// This is safe for applications that publish locally while listener
    /// events are still queued: the merge keeps the furthest-known version for
    /// every group instead of replacing newer local progress with an older
    /// event token.
    ///
    /// # Panics
    ///
    /// Panics if both tokens contain the same group with incompatible member
    /// counts.
    pub fn merge_applied(&mut self, applied: &ReadToken) {
        let versions = Arc::make_mut(&mut self.versions);
        for (group_id, applied_versions) in applied.versions.groups.iter() {
            if let Some(existing_versions) = versions.groups.get_mut(group_id) {
                *existing_versions = existing_versions.least_upper_bound(applied_versions);
            } else {
                // Group membership changes are rare, so most merges update an
                // existing entry in place and avoid this sorted-vector insertion.
                versions.groups.insert(*group_id, applied_versions.clone());
            }
        }
    }

    /// Decode the outer format discriminator and its selected payload.
    fn decode_group_versions(
        input: &[u8],
    ) -> Result<SortedArrayMap<GroupId, VersionVector>, ReadTokenBytesDecodeError> {
        let Some((&format, payload)) = input.split_first() else {
            return MissingFormatSnafu.fail();
        };
        ensure!(
            format == READ_TOKEN_PROTOBUF_FORMAT_V1,
            UnsupportedFormatSnafu {
                actual: format,
                supported: READ_TOKEN_PROTOBUF_FORMAT_V1,
            }
        );
        let groups = ReadTokenProtoCodec::decode_proto_from_slice(payload)
            .context(InvalidPayloadSnafu)?
            .into_groups();
        Ok(groups)
    }
}

impl std::fmt::Debug for ReadToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            return f
                .debug_struct("ReadToken")
                .field("groups", &self.versions.groups)
                .finish();
        }
        f.debug_struct("ReadToken")
            .field("group_count", &self.group_count())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ReadToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&STANDARD.encode(self.to_bytes()))
    }
}

impl FromStr for ReadToken {
    type Err = ParseReadTokenError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let bytes = STANDARD.decode(input).context(InvalidBase64Snafu)?;
        // Only check for base64 canonicity. Protobuf is more lenient in encoding between versions.
        ensure!(STANDARD.encode(&bytes) == input, NonCanonicalSnafu);
        Self::from_bytes(&bytes).context(InvalidTokenSnafu)
    }
}

/// Failure while decoding opaque read-token bytes.
#[derive(Debug, Snafu)]
#[snafu(display("Read-token bytes were invalid: {source}"))]
pub struct ReadTokenDecodeError {
    /// Structural protobuf or token-format failure.
    source: BoxError,
}

/// Failure while parsing a read token from its canonical string representation.
#[derive(Debug, Snafu)]
pub enum ParseReadTokenError {
    /// The text was not standard Base64.
    #[snafu(display("Read-token text was not valid standard Base64: {source}"))]
    InvalidBase64 { source: base64::DecodeError },
    /// The decoded bytes were not a valid read token.
    #[snafu(display("Read-token text did not contain a valid token: {source}"))]
    InvalidToken { source: ReadTokenDecodeError },
    /// The text decoded successfully but was not the canonical token spelling.
    #[snafu(display("Read-token text was not canonically encoded."))]
    NonCanonical,
}

/// Group-scoped positions hidden behind the public opaque token.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadTokenVersions {
    /// Canonically ordered group vectors used by codecs and runtime operations.
    groups: SortedArrayMap<GroupId, VersionVector>,
}

/// Failure while decoding the outer read-token byte format.
#[derive(Debug, Snafu)]
enum ReadTokenBytesDecodeError {
    /// The bytes omitted the format discriminator.
    #[snafu(display("Read token omitted its format discriminator."))]
    MissingFormat,
    /// The format discriminator is not understood by this runtime.
    #[snafu(display(
        "Read token used unsupported format {actual}; supported format is {supported}."
    ))]
    UnsupportedFormat { actual: u8, supported: u8 },
    /// The version-selected payload was structurally invalid.
    #[snafu(display("Read-token payload was invalid: {source}"))]
    InvalidPayload {
        source: crate::codecs::ReadTokenCodecError,
    },
}

/// Request to publish one local set of row mutations from a known read token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishChangesRequest {
    /// Opaque read position for the application state this change was based on.
    pub read_token: ReadToken,
    /// Row mutations to publish.
    pub changes: Vec<RowMutation>,
}

/// Row-level change emitted by the framework to an application listener.
///
/// `previous` describes how the row relates to the application view before
/// this event. `change` is the operation to apply to obtain the event view.
pub struct RowChange {
    /// Previous-row correspondence and migration evidence for this operation.
    pub previous: PreviousRow,
    /// Operation which updates the application view.
    pub change: RowChangeKind,
}

impl RowChange {
    /// Build an upsert which is unrelated to a group replacement.
    pub(crate) fn ordinary_upsert(row_id: RowId, row: Arc<dyn RowValueRead + Send + Sync>) -> Self {
        Self {
            previous: PreviousRow::NotCompared,
            change: RowChangeKind::Upsert {
                row_id,
                row,
                previous_value_differences: None,
            },
        }
    }

    /// Build a delete which is unrelated to a group replacement.
    pub(crate) fn ordinary_delete(row_id: RowId) -> Self {
        Self {
            previous: PreviousRow::NotCompared,
            change: RowChangeKind::Delete { row_id },
        }
    }

    /// Return the group-scoped identity affected by this operation.
    #[must_use]
    pub fn row_id(&self) -> &RowId {
        self.change.row_id()
    }

    /// Return the complete visible row value for an upsert, or `None` for a delete.
    #[must_use]
    pub fn row(&self) -> Option<&(dyn RowValueRead + Send + Sync)> {
        self.change.row()
    }
}

/// Operation applied to the application view for one [`RowChange`].
pub enum RowChangeKind {
    /// Insert a successor row or replace the visible values of an existing row.
    Upsert {
        /// Row occurrence which becomes visible.
        row_id: RowId,
        /// Complete application-facing value of the visible row.
        row: Arc<dyn RowValueRead + Send + Sync>,
        /// Fields which differ from the visible old-group row.
        ///
        /// `Some` means Flotsync compared both visible row values. An empty
        /// collection means they were identical. `None` means no value
        /// comparison was performed; [`RowChange::previous`] explains why.
        previous_value_differences: Option<Box<[RowFieldDifference]>>,
    },
    /// Remove a row occurrence from the application view.
    Delete {
        /// Row occurrence which is no longer visible.
        row_id: RowId,
    },
}

impl RowChangeKind {
    /// Return the group-scoped identity affected by this operation.
    fn row_id(&self) -> &RowId {
        match self {
            Self::Upsert { row_id, .. } | Self::Delete { row_id } => row_id,
        }
    }

    /// Return the complete visible row value for an upsert, or `None` for a delete.
    fn row(&self) -> Option<&(dyn RowValueRead + Send + Sync)> {
        match self {
            Self::Upsert { row, .. } => Some(row.as_ref()),
            Self::Delete { .. } => None,
        }
    }
}

/// What Flotsync knows about the corresponding row in the old application view.
///
/// During a group replacement, this tells an application whether the same
/// dataset and row key was visible, deleted, or not stored in the old group.
/// When the old group is not available locally, it says that the answer is
/// unknown. Applications can combine this with [`PreviousRowEvidence`] to
/// decide whether old-group work needs to be reconciled with the successor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreviousRow {
    /// This is an update within one group, so there is no old-group comparison.
    NotCompared,
    /// The same dataset and row key was visible in the old group.
    Present {
        /// Group-scoped identity to remove or reconcile from the old view.
        row_id: RowId,
        /// What is known about the row's origin and inclusion in the successor.
        evidence: PreviousRowEvidence,
    },
    /// The old group was inspected and contained no visible corresponding row.
    Absent(PreviousRowAbsence),
    /// The old group is not hosted locally, so past existence and values are unknown.
    Unavailable,
}

impl PreviousRow {
    /// Build evidence for a corresponding old-group row whose latest state is deletion.
    pub(crate) fn tombstoned(row_id: RowId, evidence: PreviousRowEvidence) -> Self {
        Self::Absent(PreviousRowAbsence::Tombstoned { row_id, evidence })
    }

    /// No occurrence for the corresponding key is stored in the inspected old group.
    pub(crate) const NOT_STORED: Self = Self::Absent(PreviousRowAbsence::NotStored);
}

/// Why an inspected old group had no visible row for the corresponding key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreviousRowAbsence {
    /// No occurrence is stored for this key in the locally inspected old group.
    ///
    /// Unlike [`PreviousRow::Unavailable`], Flotsync did inspect the old group.
    /// It found neither a visible row nor a retained deletion for this key.
    NotStored,
    /// The corresponding old-group occurrence exists, but its latest state is deletion.
    Tombstoned {
        /// Group-scoped identity of the deleted old-group occurrence.
        row_id: RowId,
        /// What is known about its creation and latest, deleted state.
        evidence: PreviousRowEvidence,
    },
}

/// Information applications can use to recognise old-group work omitted by a successor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreviousRowEvidence {
    /// Who created the old-group occurrence relative to this application member.
    ///
    /// `None` means the creator is not recorded or cannot be interpreted.
    pub creator: Option<PreviousRowCreator>,
    /// Whether the accepted old-group state used for the successor included creation.
    ///
    /// `None` means retained provenance cannot establish the answer.
    pub creation: Option<AcceptedCutRelation>,
    /// Whether that accepted state included the occurrence's latest stored state.
    ///
    /// For a visible row this is its latest value; for a tombstone it is the
    /// deletion. `None` means retained provenance cannot establish the answer.
    pub last_state: Option<AcceptedCutRelation>,
}

/// Who created an old-group row occurrence relative to the local application member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreviousRowCreator {
    /// The local member created the predecessor occurrence.
    Local,
    /// Another member created the predecessor occurrence.
    Other,
}

/// Whether old-group work was included in the accepted state used for the successor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptedCutRelation {
    /// The successor's accepted old-group state includes this operation or state.
    Included,
    /// The operation or state is newer than the accepted old-group state.
    ///
    /// This work is known locally but is not known to have reached the member
    /// which created the successor, so the application may need to reconcile it.
    NotIncluded,
}

/// One conceptual difference between corresponding application-facing row values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowFieldDifference {
    /// The field exists on both sides with different projected values.
    ValueChanged {
        /// Canonical schema field name.
        field_name: Cow<'static, str>,
    },
}

impl RowFieldDifference {
    /// Return the canonical schema field name represented by this difference.
    #[must_use]
    pub fn field_name(&self) -> &str {
        match self {
            Self::ValueChanged { field_name } => field_name,
        }
    }
}

/// One owned batch emitted by a [`BatchProvider`].
pub trait ProviderBatch: Send + 'static {
    /// Remove previously emitted rows before reusing this allocation.
    fn clear(&mut self);
    /// Return whether the batch currently contains no rows.
    fn is_empty(&self) -> bool;
}

impl<T> ProviderBatch for Vec<T>
where
    T: Send + 'static,
{
    fn clear(&mut self) {
        Vec::clear(self);
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }
}

impl<A> ProviderBatch for SmallVec<A>
where
    A: Array + Send + 'static,
    A::Item: Send + 'static,
{
    fn clear(&mut self) {
        SmallVec::clear(self);
    }

    fn is_empty(&self) -> bool {
        SmallVec::is_empty(self)
    }
}

/// Source for owned batches with explicit end-of-stream signalling.
///
/// `Some(batch)` means at least one row was emitted. `None` means the provider
/// is exhausted. `fill_batch` accepts the previous batch by value so
/// implementations may reuse, move, or hand the allocation to another thread
/// while preparing the next batch.
pub trait BatchProvider: Send {
    type Batch: ProviderBatch;

    /// Allocate a fresh empty batch using this provider's batching policy.
    fn new_batch(&self) -> Self::Batch;

    fn fill_batch(
        &mut self,
        reuse: Self::Batch,
    ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>>;

    fn next_batch(&mut self) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
        let reuse = self.new_batch();
        self.fill_batch(reuse)
    }
}

/// Process all batches from `provider`, reusing each emitted batch allocation.
///
/// `process` receives the batch mutably so it can use concrete batch APIs such
/// as `drain` before the allocation is handed back to the provider.
///
/// # Errors
///
/// Returns provider errors from fetching a batch or processing errors returned
/// by `process`.
pub async fn process_batches<B>(
    provider: &mut dyn BatchProvider<Batch = B>,
    mut process: impl FnMut(&mut B) -> Result<(), RowProviderError>,
) -> Result<(), RowProviderError>
where
    B: ProviderBatch,
{
    let mut batch = provider.new_batch();
    while let Some(mut filled_batch) = provider.fill_batch(batch).await? {
        process(&mut filled_batch)?;
        batch = filled_batch;
    }
    Ok(())
}

/// Batch of row changes emitted by a [`RowProvider`].
pub type RowChangeBatch = SmallVec<[RowChange; 4]>;

/// Source for batched row-level changes carried in `ReplicationEvent::DataChanged`.
pub type RowProvider = dyn BatchProvider<Batch = RowChangeBatch>;
