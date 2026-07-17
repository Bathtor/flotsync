//! Protobuf conversions for group decision and activation payloads.

use crate::{
    api::{
        DatasetId,
        DatasetIdError,
        DatasetSchema,
        GroupInvitation,
        GroupInvitationError,
        GroupInvitationSource,
        GroupSchema,
        GroupSchemaError,
        InitialDatasetValueRows,
        InitialGroupValueRows,
        InitialSnapshot,
        InitialSnapshotMetadata,
        InitialValueRow,
        MigrationId,
        MigrationProposal,
        PendingGroupActivationRecord,
        PendingGroupDecisionRecord,
        RowKey,
        RowValues,
        SchemaSource,
        SnapshotRef,
    },
    codecs::messages::{RuntimeVersionVectorProtoSource, WireVersionVector},
    delivery::wire::{
        group_id_from_wire,
        member_identity_from_wire,
        member_identity_to_wire_format,
    },
    runtime::BoxedError,
};
use flotsync_core::{GroupId, MemberIdentity, versions::VersionVector};
use flotsync_data_types::InMemoryValueDataError;
use flotsync_messages::{
    buffa::MessageField,
    codecs::{
        datamodel::{
            CodecError as DatamodelCodecError,
            decode_nullable_basic_value,
            encode_nullable_basic_value,
        },
        schema::{decode_schema_definition, encode_schema_definition},
    },
    proto::{
        self,
        DecodeProto,
        DecodeProtoOneof,
        DecodeProtoWith,
        EncodeProto,
        EncodeProtoOneof,
        MissingRequiredProto,
        ProtoInputDecodeError,
        RequiredProtoField,
    },
    replication as replication_proto,
    wire as message_wire,
};
use smallvec::SmallVec;
use snafu::prelude::*;
use std::num::NonZeroUsize;
use uuid::Uuid;

/// Generated payload body stored for one pending group work row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PendingGroupPayloadKind {
    /// Payload bytes contain a [`replication_proto::GroupInvitationPayload`].
    GroupInvitation,
    /// Payload bytes contain a [`replication_proto::MigrationProposalPayload`].
    MigrationProposal,
}

/// Store-provided context required to expand compact version vectors.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PendingGroupPayloadDecodeContext {
    /// Member count for the old group when the payload references one.
    pub(crate) old_group_member_count: Option<NonZeroUsize>,
}

/// Failure while decoding or validating a stored pending group payload.
#[derive(Debug, Snafu)]
pub enum PendingGroupPayloadError {
    /// Generated protobuf bytes could not be decoded.
    #[snafu(display("Failed to decode pending group payload."))]
    Decode {
        source: flotsync_messages::buffa::DecodeError,
    },
    /// A required protobuf field was absent.
    #[snafu(display("Pending group payload did not contain required field '{field}'."))]
    MissingRequiredField { field: &'static str },
    /// Decoded invitation fields violated invitation invariants.
    #[snafu(display("Pending group invitation was invalid: {source}"))]
    InvalidInvitation { source: GroupInvitationError },
    /// Group schema payload listed the same dataset more than once.
    #[snafu(display("Pending group schema contained duplicate dataset id '{dataset_id}'."))]
    DuplicateDatasetSchema { dataset_id: DatasetId },
    /// A compact version vector required old-group member-count context.
    #[snafu(display(
        "Pending payload version-vector field '{field}' for group {group_id} needs member-count context."
    ))]
    MissingVersionVectorContext {
        field: &'static str,
        group_id: GroupId,
    },
    /// A wire-format group, member, or similarly encoded value was invalid.
    #[snafu(display("Pending payload wire value was invalid: {source}"))]
    InvalidWireValue { source: BoxedError },
    /// An initial row key was not a valid UUID.
    #[snafu(display("Pending row key field '{field}' was invalid: {source}"))]
    InvalidRowKey {
        field: &'static str,
        source: uuid::Error,
    },
    /// A dataset id string did not satisfy [`DatasetId`] invariants.
    #[snafu(display("Pending dataset id '{value}' was invalid: {source}"))]
    InvalidDatasetId {
        value: String,
        source: DatasetIdError,
    },
    /// A dataset schema payload could not be decoded into a runtime schema.
    #[snafu(display("Pending dataset schema '{dataset_id}' was invalid: {source}"))]
    InvalidDatasetSchema {
        dataset_id: DatasetId,
        source: BoxedError,
    },
    /// An inline initial row field value was not valid for the datamodel codec.
    #[snafu(display("Pending row field '{field_name}' was invalid: {source}"))]
    InvalidRowFieldValue {
        field_name: String,
        source: DatamodelCodecError,
    },
    /// An inline initial dataset referenced a dataset without a decoded schema.
    #[snafu(display("Pending initial dataset '{dataset_id}' has no decoded schema."))]
    MissingInitialDatasetSchema { dataset_id: DatasetId },
    /// An inline initial row did not match its decoded dataset schema.
    #[snafu(display(
        "Pending initial row '{row_key}' in dataset '{dataset_id}' did not match its schema: {source}",
    ))]
    InvalidInitialRowSchema {
        dataset_id: DatasetId,
        row_key: RowKey,
        #[snafu(source(from(InMemoryValueDataError, Box::new)))]
        source: Box<InMemoryValueDataError>,
    },
    /// A version-vector field could not be decoded with the available context.
    #[snafu(display("Pending version-vector field '{field}' was invalid: {reason}"))]
    InvalidVersionVector { field: &'static str, reason: String },
}

impl PendingGroupPayloadError {
    fn missing_required_field(field: &'static str) -> Self {
        Self::MissingRequiredField { field }
    }

    fn invalid_version_vector(field: &'static str, source: &impl std::fmt::Display) -> Self {
        Self::InvalidVersionVector {
            field,
            reason: source.to_string(),
        }
    }
}

impl From<GroupInvitationError> for PendingGroupPayloadError {
    fn from(source: GroupInvitationError) -> Self {
        Self::InvalidInvitation { source }
    }
}

impl From<GroupSchemaError> for PendingGroupPayloadError {
    fn from(source: GroupSchemaError) -> Self {
        match source {
            GroupSchemaError::DuplicateDataset { dataset_id } => {
                Self::DuplicateDatasetSchema { dataset_id }
            }
        }
    }
}

impl proto::FromProtoDecodeError for PendingGroupPayloadError {
    fn from_proto_decode_error(source: flotsync_messages::buffa::DecodeError) -> Self {
        Self::Decode { source }
    }
}

impl MissingRequiredProto for PendingGroupPayloadError {
    type Context = &'static str;

    fn missing_required(field: Self::Context) -> Self {
        Self::missing_required_field(field)
    }
}

/// Encode a pending decision row payload without the SQL key columns.
#[must_use]
pub(crate) fn encode_pending_group_decision_payload(
    record: &PendingGroupDecisionRecord,
) -> (PendingGroupPayloadKind, Vec<u8>) {
    match record {
        PendingGroupDecisionRecord::GroupInvitation(invitation) => (
            PendingGroupPayloadKind::GroupInvitation,
            invitation.encode_proto_to_vec(),
        ),
        PendingGroupDecisionRecord::MigrationProposal(proposal) => (
            PendingGroupPayloadKind::MigrationProposal,
            proposal.encode_proto_to_vec(),
        ),
    }
}

/// Decode a pending decision row payload selected by its SQL work kind.
pub(crate) fn decode_pending_group_decision_payload(
    kind: PendingGroupPayloadKind,
    payload: &[u8],
    context: PendingGroupPayloadDecodeContext,
) -> Result<PendingGroupDecisionRecord, ProtoInputDecodeError<PendingGroupPayloadError>> {
    match kind {
        PendingGroupPayloadKind::GroupInvitation => {
            GroupInvitation::try_decode_proto_from_slice_with(payload, context)
                .map(PendingGroupDecisionRecord::GroupInvitation)
        }
        PendingGroupPayloadKind::MigrationProposal => {
            MigrationProposal::try_decode_proto_from_slice_with(payload, context)
                .map(PendingGroupDecisionRecord::MigrationProposal)
        }
    }
}

/// Encode a pending activation row payload without the SQL key columns.
#[must_use]
pub(crate) fn encode_pending_group_activation_payload(
    record: &PendingGroupActivationRecord,
) -> (PendingGroupPayloadKind, Vec<u8>) {
    match record {
        PendingGroupActivationRecord::GroupInvitation(invitation) => (
            PendingGroupPayloadKind::GroupInvitation,
            invitation.encode_proto_to_vec(),
        ),
        PendingGroupActivationRecord::MigrationProposal(proposal) => (
            PendingGroupPayloadKind::MigrationProposal,
            proposal.encode_proto_to_vec(),
        ),
    }
}

/// Decode a pending activation row payload selected by its SQL work kind.
pub(crate) fn decode_pending_group_activation_payload(
    kind: PendingGroupPayloadKind,
    payload: &[u8],
    context: PendingGroupPayloadDecodeContext,
) -> Result<PendingGroupActivationRecord, ProtoInputDecodeError<PendingGroupPayloadError>> {
    match kind {
        PendingGroupPayloadKind::GroupInvitation => {
            GroupInvitation::try_decode_proto_from_slice_with(payload, context)
                .map(PendingGroupActivationRecord::GroupInvitation)
        }
        PendingGroupPayloadKind::MigrationProposal => {
            MigrationProposal::try_decode_proto_from_slice_with(payload, context)
                .map(PendingGroupActivationRecord::MigrationProposal)
        }
    }
}

impl EncodeProto for GroupInvitation {
    type Proto = replication_proto::GroupInvitationPayload;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::GroupInvitationPayload {
            group_id: message_wire::group_id_to_wire_bytes(self.group_id),
            source: MessageField::some(EncodeProto::encode_proto(&self.source)),
            proposed_members: encode_member_identities(&self.proposed_members),
            dataset_schemas: self
                .group_schema
                .datasets()
                .iter()
                .map(EncodeProto::encode_proto)
                .collect(),
            initial_snapshot: MessageField::some(EncodeProto::encode_proto(&self.initial_snapshot)),
            group_name: self.group_name.clone(),
            message: self.message.clone(),
            ..replication_proto::GroupInvitationPayload::default()
        }
    }
}

impl DecodeProtoWith<PendingGroupPayloadDecodeContext> for GroupInvitation {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::GroupInvitationPayload;

    fn decode_proto_with(
        mut invitation: Self::Proto,
        context: PendingGroupPayloadDecodeContext,
    ) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(&invitation.group_id, "group_invitation.group_id")
            .boxed()
            .context(InvalidWireValueSnafu)?;
        let source = GroupInvitationSource::decode_required_proto_field(
            &mut invitation.source,
            "group_invitation.source",
        )?;
        let proposed_members =
            decode_member_identities(invitation.proposed_members, "group_invitation.members")?;
        let group_schema = decode_group_schema(invitation.dataset_schemas)?;
        let vector_context = VersionVectorDecodeContext::from_group_invitation(
            group_id,
            source,
            proposed_members.len(),
            context,
        )?;
        let snapshot_context = InitialSnapshotDecodeContext {
            version_vector_context: vector_context,
            group_schema: &group_schema,
        };
        let initial_snapshot = match invitation.initial_snapshot.take() {
            Some(snapshot) => InitialSnapshot::decode_proto_with(snapshot, snapshot_context)?,
            None => InitialSnapshot::Empty,
        };
        GroupInvitation::try_new(
            group_id,
            source,
            proposed_members,
            group_schema,
            initial_snapshot,
            invitation.group_name,
            invitation.message,
        )
        .map_err(PendingGroupPayloadError::from)
    }
}

impl DecodeProto for GroupInvitation {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::GroupInvitationPayload;

    fn decode_proto(invitation: Self::Proto) -> Result<Self, Self::Error> {
        // The default context only omits old-group member-count information.
        // Creation invitations and new-group snapshot refs can still expand
        // compact vectors from the proposed member list carried by the payload.
        Self::decode_proto_with(invitation, PendingGroupPayloadDecodeContext::default())
    }
}

impl EncodeProto for GroupInvitationSource {
    type Proto = replication_proto::GroupInvitationSource;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::GroupInvitationSource {
            source: Some(EncodeProtoOneof::encode_proto(self)),
            ..replication_proto::GroupInvitationSource::default()
        }
    }
}

impl DecodeProto for GroupInvitationSource {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::GroupInvitationSource;

    fn decode_proto(source: Self::Proto) -> Result<Self, Self::Error> {
        <Self as DecodeProtoOneof>::decode_required_proto(source.source, "group_invitation.source")
    }
}

impl EncodeProtoOneof for GroupInvitationSource {
    type Proto = replication_proto::group_invitation_source::Source;

    fn encode_proto(&self) -> Self::Proto {
        match self {
            Self::Creation => Self::Proto::Creation(Box::default()),
            Self::Migration { migration_id } => {
                Self::Proto::Migration(Box::new(replication_proto::GroupInvitationMigration {
                    old_group_id: message_wire::group_id_to_wire_bytes(migration_id.old_group_id),
                    new_group_id: message_wire::group_id_to_wire_bytes(migration_id.new_group_id),
                    ..replication_proto::GroupInvitationMigration::default()
                }))
            }
        }
    }
}

impl DecodeProtoOneof for GroupInvitationSource {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::group_invitation_source::Source;

    fn decode_proto(source: Self::Proto) -> Result<Self, Self::Error> {
        match source {
            replication_proto::group_invitation_source::Source::Creation(_) => Ok(Self::Creation),
            replication_proto::group_invitation_source::Source::Migration(migration) => {
                let old_group_id = group_id_from_wire(
                    &migration.old_group_id,
                    "group_invitation.source.migration.old_group_id",
                )
                .boxed()
                .context(InvalidWireValueSnafu)?;
                let new_group_id = group_id_from_wire(
                    &migration.new_group_id,
                    "group_invitation.source.migration.new_group_id",
                )
                .boxed()
                .context(InvalidWireValueSnafu)?;
                Ok(Self::Migration {
                    migration_id: MigrationId {
                        old_group_id,
                        new_group_id,
                    },
                })
            }
        }
    }
}

impl EncodeProto for MigrationProposal {
    type Proto = replication_proto::MigrationProposalPayload;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::MigrationProposalPayload {
            old_group_id: message_wire::group_id_to_wire_bytes(self.migration_id.old_group_id),
            new_group_id: message_wire::group_id_to_wire_bytes(self.migration_id.new_group_id),
            final_versions: MessageField::some(
                RuntimeVersionVectorProtoSource::from(&self.final_versions).encode_proto(),
            ),
            proposed_members: encode_member_identities(&self.proposed_members),
            dataset_schemas: self
                .group_schema
                .datasets()
                .iter()
                .map(EncodeProto::encode_proto)
                .collect(),
            initial_snapshot: MessageField::some(EncodeProto::encode_proto(&self.initial_snapshot)),
            group_name: self.group_name.clone(),
            message: self.message.clone(),
            ..replication_proto::MigrationProposalPayload::default()
        }
    }
}

impl DecodeProtoWith<PendingGroupPayloadDecodeContext> for MigrationProposal {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::MigrationProposalPayload;

    fn decode_proto_with(
        mut proposal: Self::Proto,
        context: PendingGroupPayloadDecodeContext,
    ) -> Result<Self, Self::Error> {
        let old_group_id =
            group_id_from_wire(&proposal.old_group_id, "migration_proposal.old_group_id")
                .boxed()
                .context(InvalidWireValueSnafu)?;
        let new_group_id =
            group_id_from_wire(&proposal.new_group_id, "migration_proposal.new_group_id")
                .boxed()
                .context(InvalidWireValueSnafu)?;
        let proposed_members =
            decode_member_identities(proposal.proposed_members, "migration_proposal.members")?;
        let vector_context = VersionVectorDecodeContext::from_migration_proposal(
            old_group_id,
            new_group_id,
            proposed_members.len(),
            context,
        )?;
        let final_versions = proposal
            .final_versions
            .take_required_proto_field::<PendingGroupPayloadError>(
                "migration_proposal.final_versions",
            )?;
        let final_versions = decode_version_vector(
            final_versions,
            "migration_proposal.final_versions",
            old_group_id,
            vector_context,
        )?;
        let group_schema = decode_group_schema(proposal.dataset_schemas)?;
        let snapshot_context = InitialSnapshotDecodeContext {
            version_vector_context: vector_context,
            group_schema: &group_schema,
        };
        let initial_snapshot = match proposal.initial_snapshot.take() {
            Some(snapshot) => InitialSnapshot::decode_proto_with(snapshot, snapshot_context)?,
            None => InitialSnapshot::Empty,
        };
        Ok(Self {
            migration_id: MigrationId {
                old_group_id,
                new_group_id,
            },
            final_versions,
            proposed_members,
            group_schema,
            initial_snapshot,
            group_name: proposal.group_name,
            message: proposal.message,
        })
    }
}

impl EncodeProto for DatasetSchema {
    type Proto = replication_proto::DatasetSchema;

    fn encode_proto(&self) -> Self::Proto {
        let schema = encode_schema_definition(self.schema.as_schema())
            .expect("group schema should be protobuf encodable");
        replication_proto::DatasetSchema {
            dataset_id: self.dataset_id.to_string(),
            schema: MessageField::some(schema),
            ..replication_proto::DatasetSchema::default()
        }
    }
}

impl DecodeProto for DatasetSchema {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::DatasetSchema;

    fn decode_proto(mut dataset_schema: Self::Proto) -> Result<Self, Self::Error> {
        let dataset_id = DatasetId::try_new(dataset_schema.dataset_id.clone()).context(
            InvalidDatasetIdSnafu {
                value: dataset_schema.dataset_id,
            },
        )?;
        // TODO(flotsync-git-02c): use shared required-field decode once schema
        // definition decoding participates in the protobuf helper traits.
        let Some(schema) = dataset_schema.schema.take() else {
            return Err(PendingGroupPayloadError::missing_required_field(
                "dataset_schema.schema",
            ));
        };
        let schema =
            decode_schema_definition(schema)
                .boxed()
                .context(InvalidDatasetSchemaSnafu {
                    dataset_id: dataset_id.clone(),
                })?;
        Ok(Self {
            dataset_id,
            schema: SchemaSource::from(schema),
        })
    }
}

impl EncodeProto for InitialSnapshot {
    type Proto = replication_proto::InitialSnapshot;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::InitialSnapshot {
            state: Some(EncodeProtoOneof::encode_proto(self)),
            ..replication_proto::InitialSnapshot::default()
        }
    }
}

impl<'schema> DecodeProtoWith<InitialSnapshotDecodeContext<'schema>> for InitialSnapshot {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::InitialSnapshot;

    fn decode_proto_with(
        snapshot: Self::Proto,
        context: InitialSnapshotDecodeContext<'schema>,
    ) -> Result<Self, Self::Error> {
        let state = snapshot.state.ok_or_else(|| {
            PendingGroupPayloadError::missing_required_field("initial_snapshot.state")
        })?;
        match state {
            replication_proto::initial_snapshot::State::Empty(_) => Ok(Self::Empty),
            replication_proto::initial_snapshot::State::Inline(state) => {
                InitialGroupValueRows::decode_proto_with(*state, context.group_schema)
                    .map(Self::Inline)
            }
            replication_proto::initial_snapshot::State::Metadata(metadata) => {
                InitialSnapshotMetadata::decode_proto_with(
                    *metadata,
                    context.version_vector_context,
                )
                .map(Self::Metadata)
            }
        }
    }
}

impl EncodeProtoOneof for InitialSnapshot {
    type Proto = replication_proto::initial_snapshot::State;

    fn encode_proto(&self) -> Self::Proto {
        match self {
            Self::Empty => Self::Proto::Empty(Box::default()),
            Self::Inline(state) => Self::Proto::Inline(state.encode_proto_boxed()),
            Self::Metadata(metadata) => Self::Proto::Metadata(metadata.encode_proto_boxed()),
        }
    }
}

impl EncodeProto for InitialGroupValueRows {
    type Proto = replication_proto::InitialGroupState;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::InitialGroupState {
            datasets: self
                .datasets
                .iter()
                .map(EncodeProto::encode_proto)
                .collect(),
            ..replication_proto::InitialGroupState::default()
        }
    }
}

impl<'schema> DecodeProtoWith<&'schema GroupSchema> for InitialGroupValueRows {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::InitialGroupState;

    fn decode_proto_with(
        state: Self::Proto,
        group_schema: &'schema GroupSchema,
    ) -> Result<Self, Self::Error> {
        let datasets = state
            .datasets
            .into_iter()
            .map(|dataset| InitialDatasetValueRows::decode_proto_with(dataset, group_schema))
            .collect::<Result<_, _>>()?;
        Ok(Self { datasets })
    }
}

impl EncodeProto for InitialDatasetValueRows {
    type Proto = replication_proto::InitialDatasetState;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::InitialDatasetState {
            dataset_id: self.dataset_id.to_string(),
            rows: self.rows.iter().map(EncodeProto::encode_proto).collect(),
            ..replication_proto::InitialDatasetState::default()
        }
    }
}

impl<'schema> DecodeProtoWith<&'schema GroupSchema> for InitialDatasetValueRows {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::InitialDatasetState;

    fn decode_proto_with(
        dataset: Self::Proto,
        group_schema: &'schema GroupSchema,
    ) -> Result<Self, Self::Error> {
        let dataset_id =
            DatasetId::try_new(dataset.dataset_id.clone()).context(InvalidDatasetIdSnafu {
                value: dataset.dataset_id,
            })?;
        let schema =
            group_schema
                .schema(&dataset_id)
                .context(MissingInitialDatasetSchemaSnafu {
                    dataset_id: dataset_id.clone(),
                })?;
        let rows = dataset
            .rows
            .into_iter()
            .map(|row| {
                InitialValueRow::decode_proto_with(
                    row,
                    InitialValueRowDecodeContext {
                        dataset_id: &dataset_id,
                        schema,
                    },
                )
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { dataset_id, rows })
    }
}

impl EncodeProto for InitialValueRow {
    type Proto = replication_proto::InitialRowState;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::InitialRowState {
            row_key: message_wire::uuid_to_wire_bytes(self.row_key.0),
            fields: self
                .row
                .fields()
                .iter()
                .map(|(field_name, value)| {
                    (
                        field_name.clone(),
                        encode_nullable_basic_value(value.as_ref()),
                    )
                })
                .collect(),
            ..replication_proto::InitialRowState::default()
        }
    }
}

impl<'schema> DecodeProtoWith<InitialValueRowDecodeContext<'schema>> for InitialValueRow {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::InitialRowState;

    fn decode_proto_with(
        row: Self::Proto,
        context: InitialValueRowDecodeContext<'schema>,
    ) -> Result<Self, Self::Error> {
        let row_key = Uuid::from_slice(&row.row_key)
            .map(RowKey)
            .context(InvalidRowKeySnafu {
                field: "initial_row_state.row_key",
            })?;
        let fields = row
            .fields
            .into_iter()
            .map(|(field_name, value)| {
                let value =
                    decode_nullable_basic_value(value).context(InvalidRowFieldValueSnafu {
                        field_name: field_name.clone(),
                    })?;
                Ok((field_name, value))
            })
            .collect::<Result<_, PendingGroupPayloadError>>()?;
        let row = RowValues::try_from_fields(context.schema.as_schema(), fields).context(
            InvalidInitialRowSchemaSnafu {
                dataset_id: context.dataset_id.clone(),
                row_key,
            },
        )?;
        Ok(Self { row_key, row })
    }
}

impl EncodeProto for InitialSnapshotMetadata {
    type Proto = replication_proto::InitialSnapshotMetadata;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::InitialSnapshotMetadata {
            primary_ref: MessageField::some(EncodeProto::encode_proto(&self.primary_ref)),
            equivalent_refs: self
                .equivalent_refs
                .iter()
                .map(EncodeProto::encode_proto)
                .collect(),
            record_count: self.record_count,
            ..replication_proto::InitialSnapshotMetadata::default()
        }
    }
}

impl DecodeProtoWith<VersionVectorDecodeContext> for InitialSnapshotMetadata {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::InitialSnapshotMetadata;

    fn decode_proto_with(
        mut metadata: Self::Proto,
        context: VersionVectorDecodeContext,
    ) -> Result<Self, Self::Error> {
        let primary_ref = SnapshotRef::decode_required_proto_field_with(
            &mut metadata.primary_ref,
            "initial_snapshot_metadata.primary_ref",
            SnapshotRefDecodeContext {
                versions_field: "initial_snapshot_metadata.primary_ref.versions",
                version_vector_context: context,
            },
        )?;
        let equivalent_refs = metadata
            .equivalent_refs
            .into_iter()
            .map(|snapshot_ref| {
                SnapshotRef::decode_proto_with(
                    snapshot_ref,
                    SnapshotRefDecodeContext {
                        versions_field: "initial_snapshot_metadata.equivalent_refs.versions",
                        version_vector_context: context,
                    },
                )
            })
            .collect::<Result<SmallVec<[SnapshotRef; 1]>, _>>()?;
        Ok(Self {
            primary_ref,
            equivalent_refs,
            record_count: metadata.record_count,
        })
    }
}

impl EncodeProto for SnapshotRef {
    type Proto = replication_proto::SnapshotRef;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::SnapshotRef {
            group_id: message_wire::group_id_to_wire_bytes(self.group_id),
            versions: MessageField::some(
                RuntimeVersionVectorProtoSource::from(&self.versions).encode_proto(),
            ),
            ..replication_proto::SnapshotRef::default()
        }
    }
}

impl DecodeProtoWith<SnapshotRefDecodeContext> for SnapshotRef {
    type Error = PendingGroupPayloadError;
    type Proto = replication_proto::SnapshotRef;

    fn decode_proto_with(
        mut snapshot_ref: Self::Proto,
        context: SnapshotRefDecodeContext,
    ) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(&snapshot_ref.group_id, "snapshot_ref.group_id")
            .boxed()
            .context(InvalidWireValueSnafu)?;
        let versions = snapshot_ref
            .versions
            .take_required_proto_field::<PendingGroupPayloadError>(context.versions_field)?;
        let versions = decode_version_vector(
            versions,
            context.versions_field,
            group_id,
            context.version_vector_context,
        )?;
        Ok(Self { group_id, versions })
    }
}

/// Context needed to decode one snapshot reference's version vector.
#[derive(Clone, Copy, Debug)]
struct SnapshotRefDecodeContext {
    /// Field path used when reporting an absent or invalid version vector.
    versions_field: &'static str,
    /// Group/member-count information for expanding compact vector encodings.
    version_vector_context: VersionVectorDecodeContext,
}

/// Context needed to decode inline initial snapshot rows against their schemas.
#[derive(Clone, Copy, Debug)]
struct InitialSnapshotDecodeContext<'schema> {
    /// Group/member-count information for expanding compact vector encodings.
    version_vector_context: VersionVectorDecodeContext,
    /// Decoded dataset schemas for validating inline value rows.
    group_schema: &'schema GroupSchema,
}

/// Context needed to decode one initial row against its dataset schema.
#[derive(Clone, Copy, Debug)]
struct InitialValueRowDecodeContext<'schema> {
    /// Dataset that owns the row, used for error context.
    dataset_id: &'schema DatasetId,
    /// Decoded schema for `dataset_id`.
    schema: &'schema SchemaSource,
}

/// Context that expands compact version vectors in pending group payloads.
///
/// TODO(flotsync-git-i20): remove or substantially simplify this once version
/// vector transfer has a clearer split between self-describing and compact
/// context-dependent protobuf forms.
#[derive(Clone, Copy, Debug, Default)]
struct VersionVectorDecodeContext {
    /// Existing group referenced by migration-sourced payload fields.
    old_group_id: Option<GroupId>,
    /// Member count loaded for `old_group_id`, when the store can provide it.
    old_group_member_count: Option<NonZeroUsize>,
    /// Target group being created or entered by the pending work.
    new_group_id: Option<GroupId>,
    /// Member count implied by the proposed target group membership order.
    new_group_member_count: Option<NonZeroUsize>,
}

impl VersionVectorDecodeContext {
    /// Build vector context for an invitation payload after its member list has decoded.
    fn from_group_invitation(
        group_id: GroupId,
        source: GroupInvitationSource,
        proposed_member_count: usize,
        context: PendingGroupPayloadDecodeContext,
    ) -> Result<Self, PendingGroupPayloadError> {
        let new_group_member_count =
            NonZeroUsize::new(proposed_member_count).context(MissingRequiredFieldSnafu {
                field: "group_invitation.members",
            })?;
        let (old_group_id, old_group_member_count) = match source {
            GroupInvitationSource::Creation => (None, None),
            GroupInvitationSource::Migration { migration_id } => (
                Some(migration_id.old_group_id),
                context.old_group_member_count,
            ),
        };
        Ok(Self {
            old_group_id,
            old_group_member_count,
            new_group_id: Some(group_id),
            new_group_member_count: Some(new_group_member_count),
        })
    }

    /// Build vector context for a migration proposal payload after its member list has decoded.
    fn from_migration_proposal(
        old_group_id: GroupId,
        new_group_id: GroupId,
        proposed_member_count: usize,
        context: PendingGroupPayloadDecodeContext,
    ) -> Result<Self, PendingGroupPayloadError> {
        let new_group_member_count =
            NonZeroUsize::new(proposed_member_count).context(MissingRequiredFieldSnafu {
                field: "migration_proposal.members",
            })?;
        Ok(Self {
            old_group_id: Some(old_group_id),
            old_group_member_count: context.old_group_member_count,
            new_group_id: Some(new_group_id),
            new_group_member_count: Some(new_group_member_count),
        })
    }

    /// Return the member count needed to expand a vector field for `group_id`.
    fn member_count_for(
        self,
        group_id: GroupId,
        field: &'static str,
    ) -> Result<NonZeroUsize, PendingGroupPayloadError> {
        if self.new_group_id == Some(group_id) {
            return self
                .new_group_member_count
                .context(MissingVersionVectorContextSnafu { field, group_id });
        }
        if self.old_group_id == Some(group_id) {
            return self
                .old_group_member_count
                .context(MissingVersionVectorContextSnafu { field, group_id });
        }
        MissingVersionVectorContextSnafu { field, group_id }.fail()
    }
}

fn decode_group_schema(
    dataset_schemas: Vec<replication_proto::DatasetSchema>,
) -> Result<GroupSchema, PendingGroupPayloadError> {
    let mut group_schema = GroupSchema::default();
    for dataset_schema in dataset_schemas {
        let dataset_schema = DatasetSchema::decode_proto(dataset_schema)?;
        group_schema
            .insert_checked(dataset_schema)
            .map_err(PendingGroupPayloadError::from)?;
    }
    Ok(group_schema)
}

fn encode_member_identities(
    members: &[MemberIdentity],
) -> Vec<flotsync_messages::discovery::Identifier> {
    members.iter().map(member_identity_to_wire_format).collect()
}

fn decode_member_identities(
    members: Vec<flotsync_messages::discovery::Identifier>,
    field: &'static str,
) -> Result<Vec<MemberIdentity>, PendingGroupPayloadError> {
    members
        .into_iter()
        .map(|member| {
            member_identity_from_wire(member, field)
                .boxed()
                .context(InvalidWireValueSnafu)
        })
        .collect()
}

fn decode_version_vector(
    version_vector: flotsync_messages::versions::VersionVector,
    field: &'static str,
    group_id: GroupId,
    context: VersionVectorDecodeContext,
) -> Result<VersionVector, PendingGroupPayloadError> {
    let wire = WireVersionVector::decode_proto(version_vector)
        .map_err(|source| PendingGroupPayloadError::invalid_version_vector(field, &source))?;
    let member_count = match &wire {
        WireVersionVector::Full(vector) => vector.len(),
        WireVersionVector::Override { .. } | WireVersionVector::Synced { .. } => {
            context.member_count_for(group_id, field)?
        }
    };
    wire.to_runtime(member_count)
        .map_err(|source| PendingGroupPayloadError::invalid_version_vector(field, &source))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{docs_dataset_id, docs_group_schema};

    #[test]
    fn group_invitation_decode_rejects_duplicate_dataset_schemas() {
        let group_id = GroupId(Uuid::from_u128(70_001));
        let invitation = GroupInvitation::new_creation(
            group_id,
            vec![MemberIdentity::from_array(["pending", "alice"])],
            docs_group_schema(),
            InitialSnapshot::Empty,
            None,
            None,
        );
        let mut proto = invitation.encode_proto();
        let duplicated_schema = proto
            .dataset_schemas
            .first()
            .expect("test invitation should contain one schema")
            .clone();
        proto.dataset_schemas.push(duplicated_schema);

        let error = GroupInvitation::decode_proto(proto)
            .expect_err("duplicate dataset schema should be rejected");

        assert!(matches!(
            error,
            PendingGroupPayloadError::DuplicateDatasetSchema { dataset_id }
                if dataset_id == docs_dataset_id()
        ));
    }
}
