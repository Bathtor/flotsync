//! Internal delivery-domain model for replication network traffic.
//!
//! This module captures the current shared types and actor-style message
//! boundaries for:
//!
//! - group-scoped fan-out via `GroupBroadcast`
//! - recipient-addressed durable delivery via `ReliableDelivery`
//!
//! The current implementation covers the minimal runtime wiring needed for
//! direct group broadcast and reliable delivery. Relay, mailbox, and durable
//! queue integration remain follow-up work.

// TODO(flotsync-t0a): Replace this crate-local mutex with the chosen
// cross-crate socket-test serialisation mechanism.
#[cfg(test)]
pub(crate) static FULL_STACK_TEST_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

pub mod contracts;
pub mod group_broadcast;
pub mod ingress;
pub mod reliable_delivery;
pub mod route_transport;
pub mod shared;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod wire;
