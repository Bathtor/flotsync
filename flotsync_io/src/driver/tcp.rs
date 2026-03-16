#[cfg(test)]
use super::runtime::ResourceSnapshot;
use super::{DriverToken, registry::SlotRegistry, runtime::ResourceRecord};
use crate::{
    api::{ConnectionId, ListenerId, TcpCommand},
    errors::{Error, Result},
};

/// TCP-side runtime state owned by the shared driver.
///
/// The current implementation only tracks local handle reservations. Real socket state will be
/// added here when the TCP client and listener tasks land.
#[derive(Debug, Default)]
pub(super) struct TcpRuntimeState {
    listeners: SlotRegistry<ResourceRecord>,
    connections: SlotRegistry<ResourceRecord>,
}

impl TcpRuntimeState {
    pub(super) fn next_listener_slot(&self) -> usize {
        self.listeners.next_slot()
    }

    pub(super) fn next_connection_slot(&self) -> usize {
        self.connections.next_slot()
    }

    pub(super) fn reserve_listener(&mut self, listener_id: ListenerId, token: DriverToken) {
        let slot = self.listeners.reserve(ResourceRecord::new(token));
        debug_assert_eq!(slot, listener_id.0);
    }

    pub(super) fn reserve_connection(&mut self, connection_id: ConnectionId, token: DriverToken) {
        let slot = self.connections.reserve(ResourceRecord::new(token));
        debug_assert_eq!(slot, connection_id.0);
    }

    pub(super) fn release_listener(&mut self, listener_id: ListenerId) -> Result<ResourceRecord> {
        self.listeners
            .remove(listener_id.0)
            .ok_or(Error::UnknownListener { listener_id })
    }

    pub(super) fn release_connection(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<ResourceRecord> {
        self.connections
            .remove(connection_id.0)
            .ok_or(Error::UnknownConnection { connection_id })
    }

    pub(super) fn record_listener_readiness_hit(&mut self, listener_id: ListenerId) {
        if let Some(entry) = self.listeners.get_mut(listener_id.0) {
            entry.readiness_hits += 1;
        }
    }

    pub(super) fn record_connection_readiness_hit(&mut self, connection_id: ConnectionId) {
        if let Some(entry) = self.connections.get_mut(connection_id.0) {
            entry.readiness_hits += 1;
        }
    }

    pub(super) fn handle_command(&mut self, command: TcpCommand) {
        log::debug!(
            "flotsync_io TCP support is not implemented yet; dropping {:?}",
            command
        );
    }

    #[cfg(test)]
    pub(super) fn listener_snapshots(&self) -> Vec<ResourceSnapshot> {
        self.listeners
            .iter()
            .map(|(slot, entry)| ResourceSnapshot {
                slot,
                token: entry.token,
                readiness_hits: entry.readiness_hits,
            })
            .collect()
    }

    #[cfg(test)]
    pub(super) fn connection_snapshots(&self) -> Vec<ResourceSnapshot> {
        self.connections
            .iter()
            .map(|(slot, entry)| ResourceSnapshot {
                slot,
                token: entry.token,
                readiness_hits: entry.readiness_hits,
            })
            .collect()
    }
}
