use crate::{
    MAX_VERSION_VALUE,
    api::{
        DatasetId,
        DatasetIdError,
        DatasetUpdateRecord,
        GroupInvitation,
        MemberKeyId,
        MigrationProposal,
        ReplicationUpdateRecord,
        Summary,
    },
    codecs::pending_group::{PendingGroupPayloadDecodeContext, PendingGroupPayloadError},
    delivery::wire::{
        WireValueDecodeError,
        group_id_from_wire,
        member_identity_from_wire,
        member_identity_to_wire_format,
    },
};
use borrowize::View;
use flotsync_core::{
    GroupId,
    MemberIdentity,
    member::TrieMap,
    versions::{OverrideVersion, PureVersionVector, UpdateId, VersionVector, VersionVectorGap},
};
use flotsync_messages::{
    buffa::MessageField,
    codecs::datamodel::{CodecError as DatamodelCodecError, decode_update_id, encode_update_id},
    datamodel as datamodel_proto,
    proto::{
        self,
        DecodeProto,
        DecodeProtoOneof,
        DecodeProtoView,
        DecodeProtoViewWith,
        DecodeProtoWith,
        EncodeProto,
        EncodeProtoOneof,
        MissingRequiredProto,
    },
    replication as replication_proto,
    security as security_proto,
    versions as versions_proto,
    wire as message_wire,
};
use flotsync_security::{
    GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
    GROUP_KEY_LENGTH,
    GroupCipherSuite,
    GroupKey,
    KEY_FINGERPRINT_LENGTH,
    KeyFingerprint,
    PublicMemberKeys,
    SecurityError,
};
use flotsync_utils::option_when;
use snafu::prelude::*;
use std::{fmt, num::NonZeroUsize, sync::Arc};
use uuid::Uuid;

mod common;
mod control;
mod encoding;
mod group;
#[cfg(test)]
mod tests;
mod updates;

pub(crate) use common::*;
pub(crate) use control::{RuntimeMessage, WireRuntimeMessage};
pub(crate) use encoding::*;
pub(crate) use group::{
    BootstrapMemberKeyMessage,
    GroupInvitationMessage,
    GroupSetupKey,
    GroupSetupMessage,
    MigrationProposalMessage,
    validate_bootstrap_public_key_bundle_matches_fingerprint,
};
pub(crate) use updates::*;
