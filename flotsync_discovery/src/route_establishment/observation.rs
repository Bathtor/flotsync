use super::{PeerAnnouncementObservationPort, PeerAnnouncementObserved, RouteEstablishmentConfig};
use crate::{
    kompact_fsm::{State, StateHandled, StateUpdate},
    protocol::decode_peer_bytes,
    services::{
        PeerAnnouncementSocketMaintenance,
        PeerAnnouncementStartupError,
        peer_announcement_bind_options_from_config,
    },
    transform_state_match,
};
use flotsync_io::prelude::{
    IoPayload,
    SocketId,
    UdpIndication,
    UdpLocalBind,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
};
use kompact::prelude::*;

/// Peer-announcement observation socket lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SocketState {
    /// The observer owns socket maintenance and has no live peer-announcement socket.
    Unbound,
    /// A bind request has been sent and the observer is waiting for the matching result.
    Binding {
        /// Request id that must match the eventual bind result.
        request_id: UdpOpenRequestId,
    },
    /// Another component maintains the peer-announcement socket and this observer is waiting to
    /// infer the shared socket id from a matching bind indication.
    WaitingForSocket,
    /// The observer knows the UDP socket id carrying peer-announcement traffic.
    Listening {
        /// Active peer-announcement socket id.
        socket_id: SocketId,
    },
    /// The observer requested socket close and is waiting for the close indication.
    Closing {
        /// Socket id being closed.
        socket_id: SocketId,
    },
}

/// Peer-announcement observation source.
#[derive(ComponentDefinition)]
pub struct PeerAnnouncementObservationComponent {
    /// Kompact component context.
    ctx: ComponentContext<Self>,
    /// Port where decoded peer announcements are published.
    announcement_port: ProvidedPort<PeerAnnouncementObservationPort>,
    /// UDP transport port used for the peer-announcement socket.
    udp_port: RequiredPort<UdpPort>,
    /// Static runtime settings for the route establishment protocol.
    config: RouteEstablishmentConfig,
    /// Whether this observation component maintains the peer-announcement socket.
    socket_maintenance: PeerAnnouncementSocketMaintenance,
    /// Peer-announcement socket lifecycle state.
    state: State<SocketState>,
}

impl PeerAnnouncementObservationComponent {
    /// Build one peer-announcement receiver.
    #[must_use]
    pub fn new(config: RouteEstablishmentConfig) -> Self {
        Self::with_socket_maintenance(config, PeerAnnouncementSocketMaintenance::Maintain)
    }

    /// Build one peer-announcement receiver with explicit socket lifecycle responsibility.
    #[must_use]
    pub fn with_socket_maintenance(
        config: RouteEstablishmentConfig,
        socket_maintenance: PeerAnnouncementSocketMaintenance,
    ) -> Self {
        let state = match socket_maintenance {
            PeerAnnouncementSocketMaintenance::Maintain => SocketState::Unbound,
            PeerAnnouncementSocketMaintenance::Observe => SocketState::WaitingForSocket,
        };
        Self {
            ctx: ComponentContext::uninitialised(),
            announcement_port: ProvidedPort::uninitialised(),
            udp_port: RequiredPort::uninitialised(),
            config,
            socket_maintenance,
            state: State::new(state),
        }
    }

    /// Send the UDP bind request for a maintained peer-announcement socket.
    ///
    /// # Errors
    ///
    /// Returns [`PeerAnnouncementStartupError`] when the Kompact configuration cannot be read.
    fn bind_peer_announcement_socket(
        &mut self,
    ) -> Result<SocketState, PeerAnnouncementStartupError> {
        let request_id = UdpOpenRequestId::new();
        let options = peer_announcement_bind_options_from_config(self.ctx.config())?;
        self.udp_port.trigger(UdpRequest::Bind {
            request_id,
            bind: UdpLocalBind::Exact(self.config.peer_announcement_bind_addr),
            options,
        });
        Ok(SocketState::Binding { request_id })
    }

    /// Decode one peer-announcement payload and publish it to the observation port.
    fn handle_peer_announcement_payload(&mut self, payload: &IoPayload) {
        let bytes = payload.to_vec();
        let peer = match decode_peer_bytes(&bytes) {
            Ok(peer) => peer,
            Err(error) => {
                trace!(
                    self.log(),
                    "ignored malformed peer-announcement observation: {error}"
                );
                return;
            }
        };
        self.announcement_port.trigger(PeerAnnouncementObserved {
            instance_id: peer.instance_id,
            routes: peer.listening_on,
        });
    }

    /// Move the observation socket lifecycle toward a stopped state.
    fn request_stop(&mut self) -> HandlerResult {
        transform_state_match!(self, state, {
            SocketState::Listening { socket_id }
                if self.socket_maintenance == PeerAnnouncementSocketMaintenance::Maintain =>
            {
                self.udp_port.trigger(UdpRequest::Close { socket_id });
                StateUpdate::transition(SocketState::Closing { socket_id })
            }
            SocketState::Binding { .. } => StateUpdate::transition(SocketState::Unbound),
            SocketState::WaitingForSocket | SocketState::Listening { .. } => {
                StateUpdate::transition(SocketState::Unbound)
            }
            state => StateUpdate::ok(state),
        })
    }

    /// Return the current peer-announcement socket lifecycle state.
    #[cfg(test)]
    pub(super) fn socket_state(&self) -> &SocketState {
        self.state.get()
    }
}

impl ComponentLifecycle for PeerAnnouncementObservationComponent {
    fn on_start(&mut self) -> HandlerResult {
        transform_state_match!(self, state, {
            SocketState::Unbound
                if self.socket_maintenance == PeerAnnouncementSocketMaintenance::Maintain =>
            {
                match self.bind_peer_announcement_socket() {
                    Ok(next_state) => StateUpdate::transition(next_state),
                    Err(error) => {
                        error!(
                            self.log(),
                            "route establishment peer-announcement bind setup failed: {error}"
                        );
                        Handled::SHUTDOWN.and_transition(SocketState::Unbound)
                    }
                }
            }
            SocketState::WaitingForSocket => StateUpdate::ok(SocketState::WaitingForSocket),
            state => {
                warn!(
                    self.log(),
                    "route establishment peer-announcement observation started in non-initial state {:?}",
                    state
                );
                StateUpdate::ok(state)
            }
        })
    }

    fn on_stop(&mut self) -> HandlerResult {
        self.request_stop()
    }

    fn on_kill(&mut self) -> HandlerResult {
        self.request_stop()
    }
}

ignore_requests!(
    PeerAnnouncementObservationPort,
    PeerAnnouncementObservationComponent
);

impl Require<UdpPort> for PeerAnnouncementObservationComponent {
    #[allow(
        clippy::too_many_lines,
        reason = "state-first matching keeps the lifecycle transition table local to the port handler"
    )]
    fn handle(&mut self, indication: UdpIndication) -> HandlerResult {
        transform_state_match!(self, state, {
            SocketState::Binding {
                request_id: current,
            } => match indication {
                UdpIndication::Bound {
                    request_id,
                    socket_id,
                    local_addr,
                } if current == request_id => {
                    info!(
                        self.log(),
                        "route establishment peer announcement listening at {} on {}",
                        local_addr,
                        socket_id
                    );
                    StateUpdate::transition(SocketState::Listening { socket_id })
                }
                UdpIndication::BindFailed {
                    request_id,
                    local_addr,
                    reason,
                } if current == request_id => {
                    warn!(
                        self.log(),
                        "route establishment peer-announcement bind failed at {}: {:?}",
                        local_addr,
                        reason
                    );
                    StateUpdate::transition(SocketState::Unbound)
                }
                _ => StateUpdate::ok(SocketState::Binding {
                    request_id: current,
                }),
            },
            SocketState::WaitingForSocket => match indication {
                UdpIndication::Bound {
                    socket_id,
                    local_addr,
                    ..
                } if local_addr.port() == self.config.peer_announcement_bind_addr.port() => {
                    info!(
                        self.log(),
                        "route establishment peer announcement observation inferred socket {} at {}",
                        socket_id,
                        local_addr
                    );
                    StateUpdate::transition(SocketState::Listening { socket_id })
                }
                _ => StateUpdate::ok(SocketState::WaitingForSocket),
            },
            SocketState::Listening {
                socket_id: active_socket,
            } => match indication {
                UdpIndication::Received {
                    socket_id, payload, ..
                } if active_socket == socket_id => {
                    self.handle_peer_announcement_payload(&payload);
                    StateUpdate::ok(SocketState::Listening {
                        socket_id: active_socket,
                    })
                }
                UdpIndication::Closed { socket_id, .. } if active_socket == socket_id => {
                    let next_state = match self.socket_maintenance {
                        PeerAnnouncementSocketMaintenance::Maintain => SocketState::Unbound,
                        PeerAnnouncementSocketMaintenance::Observe => SocketState::WaitingForSocket,
                    };
                    StateUpdate::transition(next_state)
                }
                _ => StateUpdate::ok(SocketState::Listening {
                    socket_id: active_socket,
                }),
            },
            SocketState::Closing {
                socket_id: active_socket,
            } => match indication {
                UdpIndication::Closed { socket_id, .. } if active_socket == socket_id => {
                    StateUpdate::transition(SocketState::Unbound)
                }
                _ => StateUpdate::ok(SocketState::Closing {
                    socket_id: active_socket,
                }),
            },
            SocketState::Unbound => StateUpdate::ok(SocketState::Unbound),
        })
    }
}

impl Actor for PeerAnnouncementObservationComponent {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> HandlerResult {
        unreachable!("Never message type cannot be instantiated")
    }
}
