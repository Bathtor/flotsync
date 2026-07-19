use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use enumset::{EnumSet, EnumSetType};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    member::{Identifier, TrieMap},
    membership::{GroupMembers, GroupMembersError},
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::schema::{
    Schema,
    datamodel::{NullableBasicValue, RowStateSnapshot},
};
use flotsync_security::{
    KeyFingerprint,
    PublicKeyBundle,
    PublicMemberKeys,
    StoreSecretKey,
    load_local_store_secret,
    load_or_create_local_store_secret,
};
use flotsync_utils::{BoxFuture, option_when};
use smallvec::{Array, SmallVec, smallvec};
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    fmt,
    num::NonZeroUsize,
    str::FromStr,
    sync::Arc,
};

mod errors;
mod ids;
pub mod providers;
pub mod security;

pub use errors::*;
pub use flotsync_data_types::{
    Decode,
    DecodeValueError,
    InMemoryFieldState,
    InMemoryValueData,
    InMemoryValueDataRowRef,
    RowOperations,
    RowValueRead,
    RowValues,
    schema::datamodel::SchemaSource,
};
pub use flotsync_security::{LocalStoreSecretProfile, StoreSecretKeyId};
pub use ids::*;

/// Convenience macro to build a [`RowValuesPatch`] inline.
#[macro_export]
macro_rules! row_values {
    ($($field:expr => $value:expr),* $(,)?) => {{
        let mut fields: ::std::collections::HashMap<
            ::std::string::String,
            ::flotsync_data_types::schema::datamodel::NullableBasicValue,
        > = ::std::collections::HashMap::new();
        $(
            fields.insert(($field).to_string(), ($value).into());
        )*
        $crate::api::RowValuesPatch::new(fields)
    }};
}

mod changes;
mod groups;
mod security_material;
mod snapshots;
mod store;
#[cfg(test)]
mod tests;

pub use changes::*;
pub use groups::*;
pub use security_material::*;
pub use snapshots::*;
pub use store::*;
