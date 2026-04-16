use crate::{
    GroupMembers,
    GroupMemberships,
    SharedGroupMemberships,
    api::{providers::VecRowProvider, *},
    delivery::{
        contracts::{GroupBroadcastPortIndication, ReliableDeliveryPortIndication},
        group_broadcast::{
            GroupBroadcastDeliver,
            GroupBroadcastSubmit,
            GroupMessageEnvelope,
            GroupMessageHeader,
        },
        reliable_delivery::{
            ReliableDeliveryDeliver,
            ReliableDeliverySubmit,
            ReliableMessageEnvelope,
            ReliableMessageHeader,
        },
        shared::{
            DeliveryClass,
            DetachedSignature,
            EncryptedPayload,
            MessageId,
            SignatureScheme,
            SignedEnvelopeFooter,
        },
    },
};
use flotsync_core::{
    member::Identifier,
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::Schema;
use flotsync_utils::{BoxFuture, LocalActor, impl_local_actor};
use kompact::prelude::*;
use snafu::prelude::*;
use std::{
    collections::{BTreeMap, HashMap, btree_map::Entry},
    num::NonZeroUsize,
    sync::Arc,
};

mod component;
mod errors;
mod host;
mod in_memory;
mod messages;

use component::*;
use errors::*;
use host::DeliveryRuntimeHost;
use in_memory::*;
use messages::*;

type ApiResult<T> = Result<T, ApiError>;

pub mod handle;
pub use handle::load_replication_runtime;
#[cfg(test)]
pub(crate) use host::DeliveryRuntimeHostTestExt;

#[cfg(test)]
mod tests;
