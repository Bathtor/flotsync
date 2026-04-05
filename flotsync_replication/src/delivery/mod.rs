//! Internal delivery-domain model for replication network traffic.
//!
//! This module captures the current shared types and actor-style message
//! boundaries for:
//!
//! - group-scoped fan-out via `GroupBroadcast`
//! - recipient-addressed durable delivery via `ReliableDelivery`
//!
//! It intentionally stops short of runtime wiring. Follow-up tasks will connect
//! these contracts to discovery, storage, and the Kompact transport layer.

pub mod contracts;
pub mod group_broadcast;
pub mod ingress;
pub mod reliable_delivery;
pub mod route_transport;
pub mod shared;
pub(crate) mod wire;
