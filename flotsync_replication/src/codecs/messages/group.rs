//! Group invitation, setup, and bootstrap codecs.

use super::*;

/// Reliable group invitation plus recipient-protected target-group setup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupInvitationMessage {
    pub(crate) invitation: GroupInvitation,
    group_setup: Arc<GroupSetupMessage>,
}

impl GroupInvitationMessage {
    /// Combine listener-safe invitation data with matching private group setup.
    pub(crate) fn try_new(
        invitation: GroupInvitation,
        group_setup: Arc<GroupSetupMessage>,
    ) -> Result<Self, RuntimeMessageError> {
        ensure!(
            invitation.proposed_members == group_setup.members(),
            GroupSetupMemberMismatchSnafu
        );
        Ok(Self {
            invitation,
            group_setup,
        })
    }

    /// Split the wire message into listener-safe and private setup parts.
    pub(crate) fn into_parts(self) -> (GroupInvitation, Arc<GroupSetupMessage>) {
        (self.invitation, self.group_setup)
    }
}

impl EncodeProto for GroupInvitationMessage {
    type Proto = replication_proto::GroupInvitationPayload;

    fn encode_proto(&self) -> Self::Proto {
        let mut message = self.invitation.encode_proto();
        message.group_setup = MessageField::some(self.group_setup.encode_proto());
        message
    }
}

impl DecodeProto for GroupInvitationMessage {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::GroupInvitationPayload;

    fn decode_proto(mut message: Self::Proto) -> Result<Self, Self::Error> {
        let Some(group_setup) = message.group_setup.take() else {
            return MissingGroupSetupSnafu.fail();
        };
        let group_setup = GroupSetupMessage::decode_proto(group_setup)?;
        let invitation =
            GroupInvitation::decode_proto(message).context(InvalidPendingGroupPayloadSnafu)?;
        Self::try_new(invitation, Arc::new(group_setup))
    }
}

/// Reliable migration proposal plus recipient-protected target-group setup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MigrationProposalMessage {
    pub(crate) proposal: MigrationProposal,
    group_setup: Arc<GroupSetupMessage>,
}

impl MigrationProposalMessage {
    /// Combine listener-safe proposal data with matching private group setup.
    pub(crate) fn try_new(
        proposal: MigrationProposal,
        group_setup: Arc<GroupSetupMessage>,
    ) -> Result<Self, RuntimeMessageError> {
        ensure!(
            proposal.proposed_members == group_setup.members(),
            GroupSetupMemberMismatchSnafu
        );
        Ok(Self {
            proposal,
            group_setup,
        })
    }

    /// Split the wire message into listener-safe and private setup parts.
    pub(crate) fn into_parts(self) -> (MigrationProposal, Arc<GroupSetupMessage>) {
        (self.proposal, self.group_setup)
    }
}

impl EncodeProto for MigrationProposalMessage {
    type Proto = replication_proto::MigrationProposalPayload;

    fn encode_proto(&self) -> Self::Proto {
        let mut message = self.proposal.encode_proto();
        message.group_setup = MessageField::some(self.group_setup.encode_proto());
        message
    }
}

impl DecodeProto for MigrationProposalMessage {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::MigrationProposalPayload;

    fn decode_proto(mut message: Self::Proto) -> Result<Self, Self::Error> {
        let Some(group_setup) = message.group_setup.take() else {
            return MissingGroupSetupSnafu.fail();
        };
        let group_setup = GroupSetupMessage::decode_proto(group_setup)?;
        let proposal =
            MigrationProposal::decode_proto(message).context(InvalidPendingGroupPayloadSnafu)?;
        Self::try_new(proposal, Arc::new(group_setup))
    }
}

/// Private target-group key material carried by an invitation or proposal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupSetupMessage {
    members: Vec<MemberIdentity>,
    member_keys: TrieMap<BootstrapMemberKeyMessage>,
    group_cipher_suite: GroupCipherSuite,
    group_key: GroupSetupKey,
}

impl GroupSetupMessage {
    /// Build setup material whose member list and member-key map cover
    /// exactly the same identities.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeMessageError`] if the group is empty, no member-key
    /// references are present, the member-key map does not match the member
    /// list, or an inline public bundle is bound to a different member.
    pub(crate) fn new(
        members: Vec<MemberIdentity>,
        member_keys: TrieMap<BootstrapMemberKeyMessage>,
        group_cipher_suite: GroupCipherSuite,
        group_key: GroupSetupKey,
    ) -> Result<Self, RuntimeMessageError> {
        validate_bootstrap_member_key_coverage(&members, &member_keys)?;
        Ok(Self {
            members,
            member_keys,
            group_cipher_suite,
            group_key,
        })
    }

    pub(crate) fn members(&self) -> &[MemberIdentity] {
        &self.members
    }

    pub(crate) fn member_keys(&self) -> &TrieMap<BootstrapMemberKeyMessage> {
        &self.member_keys
    }

    /// Return ordered exact member-key references for persisted group metadata.
    pub(crate) fn ordered_member_key_ids(&self) -> Vec<MemberKeyId> {
        self.members
            .iter()
            .map(|member_id| {
                let member_key = self
                    .member_keys
                    .get(member_id)
                    .expect("bootstrap member-key coverage is validated at construction");
                MemberKeyId {
                    member_id: member_id.clone(),
                    fingerprint: member_key.fingerprint(),
                }
            })
            .collect()
    }

    pub(crate) fn group_key(&self) -> &GroupSetupKey {
        &self.group_key
    }
}

impl proto::ProtoCodec for GroupSetupMessage {
    type DecodeError = RuntimeMessageError;
    type Proto = replication_proto::GroupSetup;

    fn to_proto(&self) -> Self::Proto {
        let member_keys = self
            .members
            .iter()
            .map(|member| {
                let member_key = self
                    .member_keys
                    .get(member)
                    .expect("bootstrap member key references must cover every member");
                BootstrapMemberKeyProtoSource {
                    member_id: member,
                    member_key,
                }
                .encode_proto()
            })
            .collect();
        replication_proto::GroupSetup {
            member_keys,
            group_cipher_suite: u32::from(self.group_cipher_suite.as_u16()),
            group_key: self.group_key.to_bytes().to_vec(),
            ..replication_proto::GroupSetup::default()
        }
    }

    fn from_proto(message: Self::Proto) -> Result<Self, Self::DecodeError> {
        if message.member_keys.is_empty() {
            return EmptyGroupSetupSnafu.fail();
        }
        let mut members = Vec::with_capacity(message.member_keys.len());
        let mut member_keys = TrieMap::new();
        for member_key in message.member_keys {
            let entry = BootstrapMemberKeyEntry::decode_proto(member_key)?;
            members.push(entry.member_id.clone());
            member_keys.insert(entry.member_id, entry.member_key);
        }
        let expected = GROUP_CIPHER_SUITE_CHACHA20_POLY1305.as_u16();
        ensure!(
            message.group_cipher_suite == u32::from(expected),
            UnsupportedGroupSetupCipherSuiteSnafu {
                actual: message.group_cipher_suite,
                expected,
            }
        );
        let group_key =
            fixed_bytes_field::<GROUP_KEY_LENGTH>("group_setup.group_key", &message.group_key)?;
        Self::new(
            members,
            member_keys,
            GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
            GroupSetupKey::from_bytes(group_key),
        )
    }
}

impl DecodeProtoView for GroupSetupMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::GroupSetupView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        if message.member_keys.is_empty() {
            return EmptyGroupSetupSnafu.fail();
        }
        let mut members = Vec::with_capacity(message.member_keys.len());
        let mut member_keys = TrieMap::new();
        for member_key in &message.member_keys {
            let entry = BootstrapMemberKeyEntry::decode_proto_view(member_key)?;
            members.push(entry.member_id.clone());
            member_keys.insert(entry.member_id, entry.member_key);
        }
        let expected = GROUP_CIPHER_SUITE_CHACHA20_POLY1305.as_u16();
        ensure!(
            message.group_cipher_suite == u32::from(expected),
            UnsupportedGroupSetupCipherSuiteSnafu {
                actual: message.group_cipher_suite,
                expected,
            }
        );
        let group_key =
            fixed_bytes_field::<GROUP_KEY_LENGTH>("group_setup.group_key", message.group_key)?;
        Self::new(
            members,
            member_keys,
            GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
            GroupSetupKey::from_bytes(group_key),
        )
    }
}

/// Expected key fingerprint and optional inline public bundle for one bootstrap member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BootstrapMemberKeyMessage {
    fingerprint: KeyFingerprint,
    public_keys: Option<PublicMemberKeys>,
}

impl BootstrapMemberKeyMessage {
    /// Build a fingerprint-only bootstrap member-key reference.
    pub(crate) const fn from_fingerprint(fingerprint: KeyFingerprint) -> Self {
        Self {
            fingerprint,
            public_keys: None,
        }
    }

    /// Copy typed public keys into the inline bootstrap member-key representation.
    pub(crate) fn from_public_keys(public_keys: &PublicMemberKeys) -> Self {
        Self {
            fingerprint: public_keys.fingerprint(),
            public_keys: Some(public_keys.clone()),
        }
    }

    pub(crate) const fn fingerprint(&self) -> KeyFingerprint {
        self.fingerprint
    }

    pub(crate) fn public_keys(&self) -> Option<&PublicMemberKeys> {
        self.public_keys.as_ref()
    }
}

/// Borrowed source for encoding one bootstrap member-key entry.
struct BootstrapMemberKeyProtoSource<'a> {
    /// Member identity that owns the referenced public key material.
    member_id: &'a MemberIdentity,
    /// Fingerprint and optional inline public bundle for the member.
    member_key: &'a BootstrapMemberKeyMessage,
}

impl EncodeProto for BootstrapMemberKeyProtoSource<'_> {
    type Proto = replication_proto::BootstrapMemberKey;

    fn encode_proto(&self) -> Self::Proto {
        let public_key_bundle = match self.member_key.public_keys() {
            Some(public_keys) => MessageField::some(public_keys.encode_proto()),
            None => MessageField::none(),
        };
        replication_proto::BootstrapMemberKey {
            member_id: MessageField::some(member_identity_to_wire_format(self.member_id)),
            key_fingerprint: self.member_key.fingerprint().as_ref().to_vec(),
            public_key_bundle,
            ..replication_proto::BootstrapMemberKey::default()
        }
    }
}

/// Decoded bootstrap member-key entry before coverage validation.
struct BootstrapMemberKeyEntry {
    /// Member identity decoded from the entry key field.
    member_id: MemberIdentity,
    /// Key fingerprint and optional inline public bundle decoded for the member.
    member_key: BootstrapMemberKeyMessage,
}

impl DecodeProto for BootstrapMemberKeyEntry {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::BootstrapMemberKey;

    fn decode_proto(mut message: Self::Proto) -> Result<Self, Self::Error> {
        let Some(member_id_wire) = message.member_id.take() else {
            return MissingBootstrapMemberIdSnafu.fail();
        };
        let member_id =
            member_identity_from_wire(member_id_wire, "group_setup.member_keys.member_id")
                .context(InvalidWireValueSnafu {
                    field: "group_setup.member_keys.member_id",
                })?;
        let fingerprint = fixed_bytes_field::<KEY_FINGERPRINT_LENGTH>(
            "group_setup.member_keys.key_fingerprint",
            &message.key_fingerprint,
        )
        .map(KeyFingerprint::from_bytes)?;
        let public_keys = message
            .public_key_bundle
            .take()
            .map(|bundle| decode_bootstrap_public_key_bundle(&member_id, fingerprint, bundle))
            .transpose()?;
        Ok(Self {
            member_id,
            member_key: BootstrapMemberKeyMessage {
                fingerprint,
                public_keys,
            },
        })
    }
}

impl DecodeProtoView for BootstrapMemberKeyEntry {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::BootstrapMemberKeyView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let Some(member_id_wire) = message.member_id.as_option() else {
            return MissingBootstrapMemberIdSnafu.fail();
        };
        let member_id = message_wire::member_identity_from_wire_view(
            member_id_wire,
            "group_setup.member_keys.member_id",
        )
        .map_err(WireValueDecodeError::from)
        .context(InvalidWireValueSnafu {
            field: "group_setup.member_keys.member_id",
        })?;
        let fingerprint = fixed_bytes_field::<KEY_FINGERPRINT_LENGTH>(
            "group_setup.member_keys.key_fingerprint",
            message.key_fingerprint,
        )
        .map(KeyFingerprint::from_bytes)?;
        let public_keys = message
            .public_key_bundle
            .as_option()
            .map(|bundle| decode_bootstrap_public_key_bundle_view(&member_id, fingerprint, bundle))
            .transpose()?;
        Ok(Self {
            member_id,
            member_key: BootstrapMemberKeyMessage {
                fingerprint,
                public_keys,
            },
        })
    }
}

/// Decode and validate one owned inline public key bundle from a bootstrap entry.
fn decode_bootstrap_public_key_bundle(
    member_id: &MemberIdentity,
    expected_fingerprint: KeyFingerprint,
    bundle: security_proto::PublicKeyBundle,
) -> Result<PublicMemberKeys, RuntimeMessageError> {
    let public_keys = PublicMemberKeys::decode_proto_with(bundle, member_id.clone()).context(
        InvalidBootstrapPublicKeyBundleSnafu {
            member_id: member_id.clone(),
        },
    )?;
    validate_bootstrap_public_key_bundle_matches_fingerprint(
        member_id,
        expected_fingerprint,
        &public_keys,
    )?;
    Ok(public_keys)
}

/// Decode and validate one borrowed inline public key bundle from a bootstrap entry.
fn decode_bootstrap_public_key_bundle_view(
    member_id: &MemberIdentity,
    expected_fingerprint: KeyFingerprint,
    bundle: &security_proto::PublicKeyBundleView<'_>,
) -> Result<PublicMemberKeys, RuntimeMessageError> {
    let public_keys = PublicMemberKeys::decode_proto_view_with(bundle, member_id.clone()).context(
        InvalidBootstrapPublicKeyBundleSnafu {
            member_id: member_id.clone(),
        },
    )?;
    validate_bootstrap_public_key_bundle_matches_fingerprint(
        member_id,
        expected_fingerprint,
        &public_keys,
    )?;
    Ok(public_keys)
}

/// Ensure an inline public key bundle derives its advertised bootstrap fingerprint.
pub(crate) fn validate_bootstrap_public_key_bundle_matches_fingerprint(
    member_id: &MemberIdentity,
    expected_fingerprint: KeyFingerprint,
    public_keys: &PublicMemberKeys,
) -> Result<(), RuntimeMessageError> {
    let actual = public_keys.fingerprint();
    ensure!(
        actual == expected_fingerprint,
        BootstrapPublicKeyBundleFingerprintMismatchSnafu {
            member_id: member_id.clone(),
            expected: expected_fingerprint,
            actual,
        }
    );
    Ok(())
}

/// Group key carried in a bootstrap payload.
///
/// Runtime messages are cloned in tests and route handoff, so the key is shared
/// through an `Arc`. The underlying [`GroupKey`] owns zeroisation on final drop.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct GroupSetupKey(Arc<GroupKey>);

impl GroupSetupKey {
    pub(crate) fn from_group_key(group_key: GroupKey) -> Self {
        Self(Arc::new(group_key))
    }

    pub(crate) fn from_bytes(bytes: [u8; GROUP_KEY_LENGTH]) -> Self {
        Self(Arc::new(GroupKey::from_bytes(bytes)))
    }

    pub(crate) fn as_group_key(&self) -> &GroupKey {
        &self.0
    }

    fn to_bytes(&self) -> [u8; GROUP_KEY_LENGTH] {
        self.0.to_bytes()
    }
}

impl fmt::Debug for GroupSetupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("GroupSetupKey").field(&"<redacted>").finish()
    }
}
