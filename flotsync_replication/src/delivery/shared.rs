//! Shared delivery-domain building blocks.
//!
//! These are the protocol-facing and scheduler-facing types that both
//! `GroupBroadcast` and `ReliableDelivery` build on top of.

use bytes::Bytes;
use flotsync_core::{GroupId, MemberIdentity};
use flotsync_routes::{RelayIdentity, RouteSendId};
use std::{fmt, time::SystemTime};
use uuid::Uuid;

/// Stable message identifier reused across retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub Uuid);

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "msg#{}", self.0)
    }
}

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

/// Plaintext reliable-delivery payload held inside the local process.
#[derive(Clone, PartialEq, Eq)]
pub struct PlaintextPayload {
    pub bytes: Bytes,
}

impl fmt::Debug for PlaintextPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaintextPayload")
            .field("len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// Detached signature scheme reference used in signed envelopes and control
/// messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignatureScheme {
    Ed25519,
}

/// Signature-only authentication bytes carried by control frames without an
/// encrypted body.
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
    #[must_use]
    pub fn peer(peer: MemberIdentity) -> Self {
        Self {
            endpoint: RouteEndpoint::Peer(peer),
        }
    }

    #[must_use]
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

/// Active route states from the queue model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteActiveState {
    Queued,
    AttemptingDirect {
        send_id: RouteSendId,
    },
    /// One direct send was accepted by route transport and reliable delivery is
    /// now waiting for the semantic recipient ack for the same message id.
    AwaitingRecipientAck,
    AwaitingRelayStore {
        send_id: RouteSendId,
    },
    PendingRoute {
        retry_after: Option<SystemTime>,
        reason: PendingRouteReason,
    },
}

/// Why a retained route remains pending instead of actively sending right now.
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

/// Proof that one relay durably stored one group-broadcast envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupRelayStoreConfirmation {
    pub group_id: GroupId,
    pub message_id: MessageId,
    pub original_sender: MemberIdentity,
    pub relay: RelayIdentity,
    pub route_id: LogicalRouteId,
    pub receipt_id: RelayStoreReceiptId,
}

/// Proof that one relay durably stored one reliable-delivery envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableRelayStoreConfirmation {
    pub message_id: MessageId,
    pub original_sender: MemberIdentity,
    pub recipient: MemberIdentity,
    pub relay: RelayIdentity,
    pub route_id: LogicalRouteId,
    pub receipt_id: RelayStoreReceiptId,
}
