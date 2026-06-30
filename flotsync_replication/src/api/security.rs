//! Application-facing security assessment and feedback API types.

use crate::api::{AuthorityScope, MemberKeyId, PermissionDecision};
use flotsync_core::MemberIdentity;
use flotsync_security::{KeyFingerprint, PublicKeyBundle};
use std::{collections::HashSet, fmt};

/// Request to assess one decoded public key bundle against local security state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssessPublicKeyBundleRequest {
    /// Identity-free public key material to assess.
    pub bundle: PublicKeyBundle,
    /// Unique candidate identities the application is considering for this bundle.
    pub candidate_member_ids: HashSet<MemberIdentity>,
    /// Whether assessment may store observed key material before reporting.
    pub material_storage: PublicKeyBundleAssessmentStorage,
}

/// Storage side-effect allowed while assessing a public key bundle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PublicKeyBundleAssessmentStorage {
    /// Assess against current state without writing key material, trust, or blocks.
    ReadOnly,
    /// Store the bundle as observed public key material for every candidate member.
    ///
    /// This does not record trust evidence. It only makes the exact candidate
    /// bindings available to authority scopes whose policy permits stored public
    /// key material without explicit local trust.
    StoreCandidateBindings,
}

/// Security report for one identity-free public key bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicKeyBundleReport {
    /// Fingerprint derived from the bundle's semantic key material.
    pub fingerprint: KeyFingerprint,
    /// Cryptographic schemes validated while decoding the bundle.
    pub schemes: PublicKeyBundleSchemeReport,
    /// Whether this fingerprint is globally blocked in the local store.
    pub globally_blocked: bool,
    /// Stored member-key bindings that already use this fingerprint.
    pub known_bindings: Vec<MemberKeyBindingReport>,
    /// Reports for identities supplied by the application as possible owners.
    pub candidate_members: Vec<CandidateMemberKeyReport>,
}

impl fmt::Display for PublicKeyBundleReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Public key bundle report")?;
        writeln!(f, "fingerprint: {}", self.fingerprint)?;
        writeln!(f, "schemes: {}", self.schemes)?;
        writeln!(f, "globally blocked: {}", yes_no(self.globally_blocked))?;
        if self.known_bindings.is_empty() {
            writeln!(f, "known bindings: none")?;
        } else {
            writeln!(f, "known bindings:")?;
            for binding in &self.known_bindings {
                writeln!(f, "  - {binding}")?;
            }
        }
        if self.candidate_members.is_empty() {
            write!(f, "candidate members: none")?;
        } else {
            writeln!(f, "candidate members:")?;
            for (index, candidate) in self.candidate_members.iter().enumerate() {
                if index + 1 == self.candidate_members.len() {
                    write!(f, "  - {candidate}")?;
                } else {
                    writeln!(f, "  - {candidate}")?;
                }
            }
        }
        Ok(())
    }
}

/// Cryptographic scheme summary for a decoded public key bundle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PublicKeyBundleSchemeReport {
    /// Signing key scheme carried by the bundle's `signing_key` field.
    pub signing: PublicKeyBundleSigningScheme,
    /// Encryption key scheme carried by the bundle's `encryption_key` field.
    pub encryption: PublicKeyBundleEncryptionScheme,
}

impl PublicKeyBundleSchemeReport {
    /// Scheme report for the currently supported public key bundle cryptography.
    pub const SUPPORTED: Self = Self {
        signing: PublicKeyBundleSigningScheme::Ed25519,
        encryption: PublicKeyBundleEncryptionScheme::X25519Hpke,
    };
}

impl fmt::Display for PublicKeyBundleSchemeReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "signing: {}, encryption: {}",
            self.signing, self.encryption
        )
    }
}

/// Supported public signing key schemes for bundle reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PublicKeyBundleSigningScheme {
    /// Ed25519 signing key used for Ed25519ph signatures.
    Ed25519,
}

impl fmt::Display for PublicKeyBundleSigningScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ed25519 => f.write_str("Ed25519"),
        }
    }
}

/// Supported public encryption key schemes for bundle reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PublicKeyBundleEncryptionScheme {
    /// X25519 HPKE KEM public key.
    X25519Hpke,
}

impl fmt::Display for PublicKeyBundleEncryptionScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::X25519Hpke => f.write_str("X25519 HPKE"),
        }
    }
}

/// Local report for one exact stored member-key binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberKeyBindingReport {
    /// Exact member identity plus public-key fingerprint.
    pub key_id: MemberKeyId,
    /// Trust evidence summary for this exact binding.
    pub trust: MemberKeyTrustReport,
    /// Authority decisions derived from current local policy and block state.
    pub authority: Vec<MemberKeyAuthorityReport>,
}

impl fmt::Display for MemberKeyBindingReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "member: {}, fingerprint: {}, trust: {}, authority: ",
            self.key_id.member_id, self.key_id.fingerprint, self.trust
        )?;
        if self.authority.is_empty() {
            f.write_str("none")
        } else {
            for (index, authority) in self.authority.iter().enumerate() {
                if index > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{authority}")?;
            }
            Ok(())
        }
    }
}

/// Assessment report for one candidate identity supplied by an application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CandidateMemberKeyReport {
    /// Candidate identity being assessed.
    pub member_id: MemberIdentity,
    /// Existing exact binding report for this bundle, if already known.
    pub binding_for_bundle: Option<MemberKeyBindingReport>,
    /// Other fingerprints already known locally for this candidate identity.
    pub other_known_fingerprints: Vec<KeyFingerprint>,
}

impl fmt::Display for CandidateMemberKeyReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "member: {}", self.member_id)?;
        match &self.binding_for_bundle {
            Some(binding) => write!(f, "; binding for bundle: {binding}")?,
            None => f.write_str("; binding for bundle: none")?,
        }
        f.write_str("; other known fingerprints: ")?;
        if self.other_known_fingerprints.is_empty() {
            f.write_str("none")
        } else {
            for (index, fingerprint) in self.other_known_fingerprints.iter().enumerate() {
                if index > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{fingerprint}")?;
            }
            Ok(())
        }
    }
}

/// Local trust evidence summary for one exact member-key binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MemberKeyTrustReport {
    /// Whether local trust has been explicitly recorded for this exact binding.
    pub has_local_explicit_trust: bool,
}

impl fmt::Display for MemberKeyTrustReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "local explicit trust: {}",
            yes_no(self.has_local_explicit_trust)
        )
    }
}

/// Authority decision for one exact member-key binding and scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MemberKeyAuthorityReport {
    /// Authority scope that was evaluated.
    pub scope: AuthorityScope,
    /// Permission decision derived from local policy, trust evidence, and block state.
    pub decision: PermissionDecision,
}

impl fmt::Display for MemberKeyAuthorityReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.scope, self.decision)
    }
}

/// Request to record user/application feedback for one public key bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPublicKeyBundleFeedbackRequest {
    /// Identity-free public key material the feedback applies to.
    pub bundle: PublicKeyBundle,
    /// Feedback to persist.
    pub feedback: PublicKeyBundleFeedback,
}

/// Persistable feedback for one public key bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicKeyBundleFeedback {
    /// Trust this bundle for the supplied explicit member identity.
    TrustMember { member_id: MemberIdentity },
    /// Block this key material globally by fingerprint so it is never trusted.
    BlockFingerprint,
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{PermissionDecision, PermissionDenialReason};

    #[test]
    fn public_key_bundle_report_display_includes_cli_summary() {
        let fingerprint = KeyFingerprint::from_bytes([7; 32]);
        let other_fingerprint = KeyFingerprint::from_bytes([9; 32]);
        let key_id = MemberKeyId {
            member_id: MemberIdentity::from_array(["display", "alice"]),
            fingerprint,
        };
        let binding = MemberKeyBindingReport {
            key_id,
            trust: MemberKeyTrustReport {
                has_local_explicit_trust: true,
            },
            authority: vec![
                MemberKeyAuthorityReport {
                    scope: AuthorityScope::ReplicationRuntime,
                    decision: PermissionDecision::Permit,
                },
                MemberKeyAuthorityReport {
                    scope: AuthorityScope::BootstrapActivation,
                    decision: PermissionDecision::Deny(
                        PermissionDenialReason::MissingTrustEvidence,
                    ),
                },
            ],
        };
        let report = PublicKeyBundleReport {
            fingerprint,
            schemes: PublicKeyBundleSchemeReport::SUPPORTED,
            globally_blocked: false,
            known_bindings: vec![binding.clone()],
            candidate_members: vec![CandidateMemberKeyReport {
                member_id: MemberIdentity::from_array(["display", "alice"]),
                binding_for_bundle: Some(binding),
                other_known_fingerprints: vec![other_fingerprint],
            }],
        };

        let display = report.to_string();

        assert!(display.contains("Public key bundle report"));
        assert!(display.contains("schemes: signing: Ed25519, encryption: X25519 HPKE"));
        assert!(display.contains("globally blocked: no"));
        assert!(display.contains("replication runtime=permit"));
        assert!(display.contains("bootstrap activation=deny (missing trust evidence)"));
        assert!(display.contains("other known fingerprints:"));
    }

    #[test]
    fn empty_public_key_bundle_report_display_names_empty_sections() {
        let report = PublicKeyBundleReport {
            fingerprint: KeyFingerprint::from_bytes([11; 32]),
            schemes: PublicKeyBundleSchemeReport::SUPPORTED,
            globally_blocked: true,
            known_bindings: Vec::new(),
            candidate_members: Vec::new(),
        };

        let display = report.to_string();

        assert!(display.contains("globally blocked: yes"));
        assert!(display.contains("known bindings: none"));
        assert!(display.contains("candidate members: none"));
    }
}
