//! White-box tests for security-store trust, key-material, and feedback behaviour.

use super::*;
use crate::{
    SqliteReplicationStore,
    api::{
        MemberKeyTrustEvidenceRecord,
        MemberPublicKeysRecord,
        ReplicationStore,
        security::{
            AssessPublicKeyBundleRequest,
            MemberKeyBindingReport,
            PublicKeyBundleAssessmentStorage,
            PublicKeyBundleFeedback,
            PublicKeyBundleReport,
            RecordPublicKeyBundleFeedbackRequest,
        },
    },
    test_support::test_public_member_keys,
};
use flotsync_core::member::Identifier;
use std::{collections::HashSet, time::Duration};

mod feedback;
mod fixtures;
mod key_material;
mod permissions;
