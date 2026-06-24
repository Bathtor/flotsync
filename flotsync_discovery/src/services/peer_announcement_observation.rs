//! Peer-announcement observer component.

use super::{
    PeerAnnouncementSocketMaintenance,
    PeerAnnouncementStartupError,
    peer_announcement_bind_options_from_config,
};
use crate::protocol::{DiscoveryRoute, decode_peer_bytes};
use flotsync_io::prelude::{
    IoPayload,
    SocketId,
    UdpIndication,
    UdpLocalBind,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
};
use flotsync_utils::{
    kompact_fsm::{State, StateHandled, StateUpdate},
    transform_state_match,
};
use kompact::prelude::*;
use std::net::SocketAddr;
use uuid::Uuid;

/// One decoded plaintext peer announcement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerAnnouncementObserved {
    /// Running process id for the announcing peer instance.
    pub instance_id: Uuid,
    /// Reachability endpoints advertised by this peer instance.
    pub routes: Vec<DiscoveryRoute>,
}

/// Port used by announcement protocols to publish decoded peer announcements.
#[derive(Clone, Copy, Debug, Default)]
pub struct PeerAnnouncementObservationPort;

impl Port for PeerAnnouncementObservationPort {
    type Request = Never;
    type Indication = PeerAnnouncementObserved;
}

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
    /// Local peer-announcement socket address to bind or observe.
    socket_bind_addr: SocketAddr,
    /// Whether this observation component maintains the peer-announcement socket.
    socket_maintenance: PeerAnnouncementSocketMaintenance,
    /// Peer-announcement socket lifecycle state.
    state: State<SocketState>,
}

impl PeerAnnouncementObservationComponent {
    /// Build one peer-announcement receiver.
    #[must_use]
    pub fn new(socket_bind_addr: SocketAddr) -> Self {
        Self::with_socket_maintenance(
            socket_bind_addr,
            PeerAnnouncementSocketMaintenance::Maintain,
        )
    }

    /// Build one peer-announcement receiver with explicit socket lifecycle responsibility.
    #[must_use]
    pub fn with_socket_maintenance(
        socket_bind_addr: SocketAddr,
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
            socket_bind_addr,
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
            bind: UdpLocalBind::Exact(self.socket_bind_addr),
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

    /// Return the UDP port reference used by tests to inject driver indications.
    #[cfg(test)]
    pub(super) fn udp_port(&mut self) -> RequiredRef<UdpPort> {
        self.udp_port.share()
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
                            "peer-announcement observation bind setup failed: {error}"
                        );
                        Handled::SHUTDOWN.and_transition(SocketState::Unbound)
                    }
                }
            }
            SocketState::WaitingForSocket => StateUpdate::ok(SocketState::WaitingForSocket),
            state => {
                warn!(
                    self.log(),
                    "peer-announcement observation started in non-initial state {:?}",
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
                        "peer-announcement observation listening at {} on {}",
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
                        "peer-announcement observation bind failed at {}: {:?}",
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
                } if local_addr.port() == self.socket_bind_addr.port() => {
                    info!(
                        self.log(),
                        "peer-announcement observation inferred socket {} at {}",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_keys;
    use flotsync_io::{
        prelude::{UdpBindOptions, UdpCloseReason},
        test_support::{
            build_test_kompact_system,
            build_test_kompact_system_with,
            eventually_component_state,
            kill_component,
            start_component,
        },
    };
    use flotsync_utils::kompact_testing::{PortTestingExt, PortTestingRefExt};
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    };

    #[test]
    fn observing_peer_announcement_component_infers_socket_id_by_configured_port() {
        let peer_port = 53156;
        let socket_bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), peer_port);
        let system = build_test_kompact_system();
        let component = system.create(move || {
            PeerAnnouncementObservationComponent::with_socket_maintenance(
                socket_bind_addr,
                PeerAnnouncementSocketMaintenance::Observe,
            )
        });
        start_component(&system, &component);

        let udp_port = component.on_definition(PeerAnnouncementObservationComponent::udp_port);
        system.trigger_i(
            UdpIndication::Bound {
                request_id: UdpOpenRequestId::new(),
                socket_id: SocketId(77),
                local_addr: SocketAddr::from(([127, 0, 0, 1], peer_port)),
            },
            &udp_port,
        );
        eventually_component_state(
            Duration::from_secs(1),
            &component,
            |component| {
                component.socket_state()
                    == &SocketState::Listening {
                        socket_id: SocketId(77),
                    }
            },
            "peer-announcement observer should infer the matching socket",
        );

        system.trigger_i(
            UdpIndication::Closed {
                socket_id: SocketId(77),
                remote_addr: None,
                reason: UdpCloseReason::Requested,
            },
            &udp_port,
        );
        eventually_component_state(
            Duration::from_secs(1),
            &component,
            |component| component.socket_state() == &SocketState::WaitingForSocket,
            "peer-announcement observer should return to waiting after close",
        );

        kill_component(&system, component);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn observing_peer_announcement_component_ignores_other_bound_ports() {
        let peer_port = 53157;
        let socket_bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), peer_port);
        let system = build_test_kompact_system();
        let component = system.create(move || {
            PeerAnnouncementObservationComponent::with_socket_maintenance(
                socket_bind_addr,
                PeerAnnouncementSocketMaintenance::Observe,
            )
        });
        start_component(&system, &component);

        let udp_port = component.on_definition(PeerAnnouncementObservationComponent::udp_port);
        system.trigger_i(
            UdpIndication::Bound {
                request_id: UdpOpenRequestId::new(),
                socket_id: SocketId(78),
                local_addr: SocketAddr::from(([127, 0, 0, 1], peer_port + 1)),
            },
            &udp_port,
        );
        eventually_component_state(
            Duration::from_secs(1),
            &component,
            |component| component.socket_state() == &SocketState::WaitingForSocket,
            "peer-announcement observer should ignore unrelated bind ports",
        );

        kill_component(&system, component);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn observing_peer_announcement_maintainer_uses_shared_bind_reuse_config() {
        let peer_port = 53158;
        let socket_bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), peer_port);
        let system = build_test_kompact_system_with(|config| {
            config.set_config_value(&config_keys::PEER_ANNOUNCEMENT_BIND_REUSE_ADDRESS, false);
        });
        let probe = system.create(UdpPort::tester_component_sidecar);
        let probe_ref = probe.actor_ref();
        let component = system.create(move || {
            PeerAnnouncementObservationComponent::with_socket_maintenance(
                socket_bind_addr,
                PeerAnnouncementSocketMaintenance::Maintain,
            )
        });
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let bind = probe_ref
            .observe_request(|request| matches!(request, UdpRequest::Bind { .. }))
            .wait_timeout(Duration::from_secs(1))
            .expect("UDP bind request should be observed")
            .expect("UDP probe should stay live");
        match bind.request() {
            UdpRequest::Bind { bind, options, .. } => {
                assert_eq!(
                    *bind,
                    UdpLocalBind::Exact(SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                        peer_port,
                    )),
                );
                assert_eq!(*options, UdpBindOptions::default().with_socket_reuse(false));
            }
            other => unreachable!("filtered to bind request, got {other:?}"),
        }

        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }
}
