//! Internal delivery-domain model for replication network traffic.
//!
//! This module captures the current shared types and actor-style message
//! boundaries for:
//!
//! - group-scoped fan-out via `GroupBroadcast`
//! - recipient-addressed reliable delivery via `ReliableDelivery`
//!
//! The current implementation covers direct group broadcast and persisted
//! one-to-one reliable delivery. Relay and mailbox integration remain
//! follow-up work.

pub mod contracts;
pub mod group_broadcast;
pub mod ingress;
pub mod reliable_delivery;
pub(crate) mod security;
pub mod shared;
pub(crate) mod wire;
