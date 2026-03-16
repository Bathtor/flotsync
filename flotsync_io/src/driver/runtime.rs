use super::{
    DriverCommand,
    DriverEvent,
    DriverToken,
    registry::{SlotRegistry, readiness_slot_to_token, token_to_readiness_slot},
    tcp::TcpRuntimeState,
    thread::send_reply,
    udp::{UdpRuntimeState, emit_udp_event},
};
use crate::{
    api::{ConnectionId, ListenerId, SocketId, TcpCommand, UdpCommand},
    errors::Result,
};
use mio::Registry;

/// Commands passed across the control plane into the dedicated driver thread.
#[derive(Debug)]
pub(super) enum ControlCommand {
    Dispatch(DriverCommand),
    ReserveListener {
        reply_tx: futures_channel::oneshot::Sender<Result<ListenerId>>,
    },
    ReserveConnection {
        reply_tx: futures_channel::oneshot::Sender<Result<ConnectionId>>,
    },
    ReserveSocket {
        reply_tx: futures_channel::oneshot::Sender<Result<SocketId>>,
    },
    ReleaseListener {
        listener_id: ListenerId,
        reply_tx: futures_channel::oneshot::Sender<Result<()>>,
    },
    ReleaseConnection {
        connection_id: ConnectionId,
        reply_tx: futures_channel::oneshot::Sender<Result<()>>,
    },
    ReleaseSocket {
        socket_id: SocketId,
        reply_tx: futures_channel::oneshot::Sender<Result<()>>,
    },
    #[cfg(test)]
    Snapshot {
        reply_tx: futures_channel::oneshot::Sender<Result<DriverSnapshot>>,
    },
    Stop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ResourceKey {
    Listener(ListenerId),
    Connection(ConnectionId),
    Socket(SocketId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ResourceRecord {
    pub(super) token: DriverToken,
    pub(super) readiness_hits: usize,
}

impl ResourceRecord {
    pub(super) fn new(token: DriverToken) -> Self {
        Self {
            token,
            readiness_hits: 0,
        }
    }
}

/// Driver-owned lookup tables for both caller-facing handles and mio readiness tokens.
///
/// The transport-specific runtime states (`tcp`, `udp`) answer API lookups in their own
/// namespaces. The shared `readiness_keys` registry answers poll-loop lookups from `mio::Token`
/// back to the resource kind that became ready.
///
/// A reservation therefore allocates in two independent spaces:
/// 1. a typed handle slot in the resource-specific registry
/// 2. a readiness slot in `readiness_keys`
///
/// The readiness slot is converted into a public `mio::Token` by adding `1`, because `Token(0)`
/// is reserved for the driver's internal waker. Handle ids and readiness slots may happen to
/// share the same integer value, but that is incidental and never relied on.
#[derive(Debug, Default)]
pub(super) struct DriverRuntimeState {
    readiness_keys: SlotRegistry<ResourceKey>,
    tcp: TcpRuntimeState,
    udp: UdpRuntimeState,
    #[cfg(test)]
    pub(super) test_state: DriverTestState,
}

impl DriverRuntimeState {
    pub(super) fn reserve_listener(&mut self) -> ListenerId {
        let listener_id = ListenerId(self.tcp.next_listener_slot());
        let token = self.reserve_readiness(ResourceKey::Listener(listener_id));
        self.tcp.reserve_listener(listener_id, token);
        listener_id
    }

    pub(super) fn reserve_connection(&mut self) -> ConnectionId {
        let connection_id = ConnectionId(self.tcp.next_connection_slot());
        let token = self.reserve_readiness(ResourceKey::Connection(connection_id));
        self.tcp.reserve_connection(connection_id, token);
        connection_id
    }

    pub(super) fn reserve_socket(&mut self) -> SocketId {
        let socket_id = SocketId(self.udp.next_socket_slot());
        let token = self.reserve_readiness(ResourceKey::Socket(socket_id));
        self.udp.reserve_socket(socket_id, token);
        socket_id
    }

    pub(super) fn release_listener(&mut self, listener_id: ListenerId) -> Result<()> {
        let entry = self.tcp.release_listener(listener_id)?;
        self.release_readiness(entry.token);
        Ok(())
    }

    pub(super) fn release_connection(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
    ) -> Result<()> {
        let entry = self.tcp.release_connection(connection_id, registry)?;
        self.release_readiness(entry.token);
        Ok(())
    }

    pub(super) fn release_socket(
        &mut self,
        socket_id: SocketId,
        registry: &Registry,
    ) -> Result<()> {
        let entry = self.udp.release_socket(socket_id, registry)?;
        self.release_readiness(entry.token);
        Ok(())
    }

    pub(super) fn record_ready_hit(&mut self, token: DriverToken) {
        let Some(key) = self.readiness_key(token) else {
            log::warn!(
                "received readiness for unknown flotsync_io token {}",
                token.0
            );
            return;
        };

        match key {
            ResourceKey::Listener(listener_id) => {
                self.tcp.record_listener_readiness_hit(listener_id)
            }
            ResourceKey::Connection(connection_id) => {
                self.tcp.record_connection_readiness_hit(connection_id)
            }
            ResourceKey::Socket(socket_id) => self.udp.record_socket_readiness_hit(socket_id),
        }
    }

    pub(super) fn handle_udp_readable(
        &mut self,
        socket_id: SocketId,
        registry: &Registry,
        ingress_pool: &crate::pool::IngressPool,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<()> {
        self.udp
            .handle_readable(socket_id, registry, ingress_pool, event_tx)
    }

    pub(super) fn resume_suspended_udp_reads(
        &mut self,
        registry: &Registry,
        ingress_pool: &crate::pool::IngressPool,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<()> {
        self.udp
            .resume_suspended_reads(registry, ingress_pool, event_tx)
    }

    pub(super) fn handle_tcp_ready(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
        ingress_pool: &crate::pool::IngressPool,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
        is_readable: bool,
        is_writable: bool,
    ) -> Result<()> {
        let closed_record = self.tcp.handle_ready(
            connection_id,
            registry,
            ingress_pool,
            event_tx,
            is_readable,
            is_writable,
        )?;
        if let Some(record) = closed_record {
            self.release_readiness(record.token);
        }
        Ok(())
    }

    pub(super) fn resume_suspended_tcp_reads(
        &mut self,
        registry: &Registry,
        ingress_pool: &crate::pool::IngressPool,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<()> {
        self.tcp
            .resume_suspended_reads(registry, ingress_pool, event_tx)
    }

    /// Returns `true` when command processing requested that the driver thread stop.
    pub(super) fn drain_commands(
        &mut self,
        command_rx: &crossbeam_channel::Receiver<ControlCommand>,
        registry: &Registry,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
        udp_send_scratch: &mut [u8],
    ) -> Result<bool> {
        loop {
            let control = match command_rx.try_recv() {
                Ok(control) => control,
                Err(crossbeam_channel::TryRecvError::Empty) => return Ok(false),
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    log::info!("flotsync_io control channel disconnected; stopping driver loop");
                    return Ok(true);
                }
            };

            match control {
                ControlCommand::Dispatch(command) => {
                    #[cfg(test)]
                    self.test_state
                        .processed_commands
                        .push(CommandTrace::from(&command));
                    match command {
                        DriverCommand::Tcp(command) => match command {
                            TcpCommand::Connect {
                                connection_id,
                                remote_addr,
                            } => {
                                self.tcp.handle_connect(
                                    connection_id,
                                    remote_addr,
                                    registry,
                                    event_tx,
                                )?;
                            }
                            TcpCommand::Listen { listener_id, .. } => {
                                log::debug!(
                                    "flotsync_io TCP listener support is not implemented yet; dropping listen command for {}",
                                    listener_id
                                );
                            }
                            TcpCommand::Send {
                                connection_id,
                                transmission_id,
                                payload,
                            } => {
                                let closed_record = self.tcp.handle_send(
                                    connection_id,
                                    transmission_id,
                                    payload,
                                    registry,
                                    event_tx,
                                )?;
                                if let Some(record) = closed_record {
                                    self.release_readiness(record.token);
                                }
                            }
                            TcpCommand::Close {
                                connection_id,
                                abort,
                            } => {
                                let closed_record = self.tcp.handle_close(
                                    connection_id,
                                    abort,
                                    registry,
                                    event_tx,
                                )?;
                                if let Some(record) = closed_record {
                                    self.release_readiness(record.token);
                                }
                            }
                        },
                        DriverCommand::Udp(command) => match command {
                            UdpCommand::Bind {
                                socket_id,
                                local_addr,
                            } => {
                                self.udp
                                    .handle_bind(socket_id, local_addr, registry, event_tx)?;
                            }
                            UdpCommand::Connect {
                                socket_id,
                                remote_addr,
                                local_addr,
                            } => {
                                self.udp.handle_connect(
                                    socket_id,
                                    remote_addr,
                                    local_addr,
                                    registry,
                                    event_tx,
                                )?;
                            }
                            UdpCommand::Send {
                                socket_id,
                                transmission_id,
                                payload,
                                target,
                            } => {
                                self.udp.handle_send(
                                    socket_id,
                                    transmission_id,
                                    payload,
                                    target,
                                    event_tx,
                                    udp_send_scratch,
                                )?;
                            }
                            UdpCommand::Close { socket_id } => {
                                if self.release_socket(socket_id, registry).is_err() {
                                    log::warn!(
                                        "ignored UDP close for unknown socket {}",
                                        socket_id
                                    );
                                } else {
                                    emit_udp_event(
                                        event_tx,
                                        crate::api::UdpEvent::Closed { socket_id },
                                    )?;
                                }
                            }
                        },
                    }
                }
                ControlCommand::ReserveListener { reply_tx } => {
                    let listener_id = self.reserve_listener();
                    send_reply(reply_tx, Ok(listener_id), "listener reservation");
                }
                ControlCommand::ReserveConnection { reply_tx } => {
                    let connection_id = self.reserve_connection();
                    send_reply(reply_tx, Ok(connection_id), "connection reservation");
                }
                ControlCommand::ReserveSocket { reply_tx } => {
                    let socket_id = self.reserve_socket();
                    send_reply(reply_tx, Ok(socket_id), "socket reservation");
                }
                ControlCommand::ReleaseListener {
                    listener_id,
                    reply_tx,
                } => {
                    let result = self.release_listener(listener_id);
                    send_reply(reply_tx, result, "listener release");
                }
                ControlCommand::ReleaseConnection {
                    connection_id,
                    reply_tx,
                } => {
                    let result = self.release_connection(connection_id, registry);
                    send_reply(reply_tx, result, "connection release");
                }
                ControlCommand::ReleaseSocket {
                    socket_id,
                    reply_tx,
                } => {
                    let result = self.release_socket(socket_id, registry);
                    send_reply(reply_tx, result, "socket release");
                }
                #[cfg(test)]
                ControlCommand::Snapshot { reply_tx } => {
                    let snapshot = self.snapshot();
                    send_reply(reply_tx, Ok(snapshot), "driver snapshot");
                }
                ControlCommand::Stop => {
                    log::info!("flotsync_io driver received stop command");
                    return Ok(true);
                }
            }
        }
    }

    pub(super) fn readiness_key(&self, token: DriverToken) -> Option<ResourceKey> {
        let readiness_slot = token_to_readiness_slot(token)?;
        self.readiness_keys.get(readiness_slot).copied()
    }

    fn reserve_readiness(&mut self, key: ResourceKey) -> DriverToken {
        let readiness_slot = self.readiness_keys.reserve(key);
        readiness_slot_to_token(readiness_slot)
    }

    fn release_readiness(&mut self, token: DriverToken) {
        let Some(readiness_slot) = token_to_readiness_slot(token) else {
            return;
        };
        let _ = self.readiness_keys.remove(readiness_slot);
    }

    #[cfg(test)]
    fn snapshot(&self) -> DriverSnapshot {
        DriverSnapshot {
            wakeup_count: self.test_state.wakeup_count,
            poll_iterations: self.test_state.poll_iterations,
            listeners: self.tcp.listener_snapshots(),
            connections: self.tcp.connection_snapshots(),
            sockets: self.udp.socket_snapshots(),
            processed_commands: self.test_state.processed_commands.clone(),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct DriverTestState {
    pub(super) wakeup_count: usize,
    pub(super) poll_iterations: usize,
    pub(super) processed_commands: Vec<CommandTrace>,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(super) struct DriverSnapshot {
    pub(super) wakeup_count: usize,
    pub(super) poll_iterations: usize,
    pub(super) listeners: Vec<ResourceSnapshot>,
    pub(super) connections: Vec<ResourceSnapshot>,
    pub(super) sockets: Vec<ResourceSnapshot>,
    pub(super) processed_commands: Vec<CommandTrace>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ResourceSnapshot {
    pub(super) slot: usize,
    pub(super) token: DriverToken,
    pub(super) readiness_hits: usize,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum CommandTrace {
    TcpConnect(ConnectionId),
    TcpListen(ListenerId),
    TcpSend(ConnectionId),
    TcpClose(ConnectionId),
    UdpBind(SocketId),
    UdpConnect(SocketId),
    UdpSend(SocketId),
    UdpClose(SocketId),
}

#[cfg(test)]
impl From<&DriverCommand> for CommandTrace {
    fn from(command: &DriverCommand) -> Self {
        match command {
            DriverCommand::Tcp(TcpCommand::Connect { connection_id, .. }) => {
                Self::TcpConnect(*connection_id)
            }
            DriverCommand::Tcp(TcpCommand::Listen { listener_id, .. }) => {
                Self::TcpListen(*listener_id)
            }
            DriverCommand::Tcp(TcpCommand::Send { connection_id, .. }) => {
                Self::TcpSend(*connection_id)
            }
            DriverCommand::Tcp(TcpCommand::Close { connection_id, .. }) => {
                Self::TcpClose(*connection_id)
            }
            DriverCommand::Udp(UdpCommand::Bind { socket_id, .. }) => Self::UdpBind(*socket_id),
            DriverCommand::Udp(UdpCommand::Connect { socket_id, .. }) => {
                Self::UdpConnect(*socket_id)
            }
            DriverCommand::Udp(UdpCommand::Send { socket_id, .. }) => Self::UdpSend(*socket_id),
            DriverCommand::Udp(UdpCommand::Close { socket_id }) => Self::UdpClose(*socket_id),
        }
    }
}
