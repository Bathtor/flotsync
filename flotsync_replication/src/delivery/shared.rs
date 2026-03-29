//! Shared delivery-domain building blocks.
//!
//! These are the protocol-facing and scheduler-facing types that both
//! `GroupBroadcast` and `ReliableDelivery` build on top of.

use crate::api::{GroupId, MemberIdentity};
use bytes::Bytes;
use flotsync_utils::IString;
use std::time::SystemTime;
use uuid::Uuid;

/// Temporary relay identity choice.
///
/// Discovery owns identity verification for peers and relays. Until a later
/// task proves otherwise, relay identities can stay on the same underlying type
/// as peer/member identities.
pub type RelayIdentity = MemberIdentity;

/// Stable message identifier reused across retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub Uuid);

/// Opaque discovery-published route identity.
///
/// Equality on concrete route handles should reduce to this id so the delivery
/// scheduler can build coverage classes without knowing anything about the
/// underlying transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SendRouteId(pub Uuid);

/// Stable identifier for one concrete send operation issued against an opaque
/// discovery-provided route.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RouteSendId(pub Uuid);

/// Relay-issued or locally generated proof that one relay stored one envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelayStoreReceiptId(pub Uuid);

/// Relay mailbox item identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MailboxItemId(pub Uuid);

/// Opaque encrypted/authenticated payload bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedPayload {
    pub ciphertext: Bytes,
}

/// Detached signature scheme reference used in signed envelopes and control
/// messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignatureScheme {
    Ed25519,
    Custom(IString),
}

/// Detached signature bytes carried in plaintext footers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetachedSignature {
    pub scheme: SignatureScheme,
    pub bytes: Bytes,
}

/// Plaintext signed footer for envelope-style messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedEnvelopeFooter {
    pub signature: DetachedSignature,
}

/// Delivery semantics for group-scoped fan-out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeliveryClass {
    Durable,
    BestEffort,
}

/// One logical delivery endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RouteEndpoint {
    Peer(MemberIdentity),
    Relay(RelayIdentity),
}

/// Stable logical route identifier inside one work item.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalRouteId {
    pub endpoint: RouteEndpoint,
}

impl LogicalRouteId {
    pub fn peer(peer: MemberIdentity) -> Self {
        Self {
            endpoint: RouteEndpoint::Peer(peer),
        }
    }

    pub fn relay(relay: RelayIdentity) -> Self {
        Self {
            endpoint: RouteEndpoint::Relay(relay),
        }
    }
}

/// Stable key for one logical route inside one delivery work scope.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum WorkScopeKey {
    Group {
        group_id: GroupId,
        message_id: MessageId,
    },
    Reliable {
        recipient: MemberIdentity,
        message_id: MessageId,
    },
}

/// Full stable key for one active in-memory route record.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StableRouteKey {
    pub scope: WorkScopeKey,
    pub route_id: LogicalRouteId,
}

/// Discovery-owned reachability classes as consumed by the delivery domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReachabilityClass {
    Known,
    Reachable,
    Stale,
}

/// Active route states from the queue model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteActiveState {
    Queued,
    AttemptingDirect {
        send_id: RouteSendId,
    },
    AwaitingRelayStore {
        send_id: RouteSendId,
    },
    PendingRoute {
        retry_after: Option<SystemTime>,
        reason: PendingRouteReason,
    },
}

/// Why a durable route remains pending instead of actively sending right now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingRouteReason {
    ReachabilityUnknown,
    PeerCurrentlyUnreachable,
    RelayCurrentlyUnreachable,
    BackoffInEffect,
    LocalResourcePressure,
    RecoveredAfterRestart,
}

/// Terminal route outcomes from the queue model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteTerminalOutcome {
    Delivered {
        observed_at: SystemTime,
    },
    StoredAtRelay {
        observed_at: SystemTime,
        receipt_id: RelayStoreReceiptId,
    },
    Expired {
        observed_at: SystemTime,
        reason: RouteExpiryReason,
    },
}

/// Why the scheduler decided no further work would be attempted for one route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteExpiryReason {
    BestEffortUnreachable,
    DirectAttemptNack,
    RelayStoreNack,
    RetryBudgetExhausted,
    RetentionDeadlineElapsed,
    SupersededByPolicy,
}

/// Active route state owned by the scheduler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveRouteRecord {
    pub key: StableRouteKey,
    pub state: RouteActiveState,
}

/// Proof that one relay durably stored one envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayStoreConfirmation {
    pub message_id: MessageId,
    pub relay: RelayIdentity,
    pub route_id: LogicalRouteId,
    pub receipt_id: RelayStoreReceiptId,
}
