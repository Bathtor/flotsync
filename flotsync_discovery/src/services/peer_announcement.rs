//! UDP peer-announcement sender component.

use crate::{
    DEFAULT_DISCOVERY_PORT,
    SocketPort,
    config_keys,
    endpoint_selection::{EndpointSelection, EndpointSelectionPort},
    kompact::{config::Config, prelude::*},
};
use flotsync_io::prelude::{
    ConfigureFailureReason,
    IoPayload,
    OpenFailureReason,
    SocketId,
    TransmissionId,
    UdpBindOptions,
    UdpIndication,
    UdpLocalBind,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
    UdpSendResult,
    UdpSocketOption,
};
use flotsync_messages::{
    buffa::Message,
    discovery::{Peer, SocketAddress},
    proto::EncodeProto,
};
use itertools::Itertools;
use pnet_datalink::{self as datalink, MacAddr, NetworkInterface};
use snafu::Snafu;
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use uuid::Uuid;

/// Peer-announcement UDP socket lifecycle responsibility for a local component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerAnnouncementSocketMaintenance {
    /// This component binds, configures, closes, and rebinds the peer-announcement socket.
    Maintain,
    /// Another local component maintains the peer-announcement socket.
    Observe,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Options {
    /// Local UDP socket address used by the peer-announcement sender or observer.
    ///
    /// In maintain mode this is the address used for the `UdpRequest::Bind`. In observe mode its
    /// port identifies which shared UDP socket belongs to peer announcements when this component
    /// sees `UdpIndication::Bound`. It does not affect the `Peer` payload's advertised routes.
    ///
    /// Defaults to the unspecified IPv4 address on [`DEFAULT_DISCOVERY_PORT`].
    pub socket_bind_addr: SocketAddr,
    /// Optional UDP port used for broadcast announcement destinations.
    ///
    /// This only controls the destination port placed onto per-interface broadcast addresses. It
    /// does not affect local socket binding, observe-mode socket matching, or the advertised routes
    /// encoded in outgoing `Peer` payloads. `None` is the normal mode and targets
    /// [`Self::socket_bind_addr`]'s port, so the local socket and broadcast destination use the same
    /// discovery port. `Some` is an explicit split-port override for tests, diagnostics, or
    /// transitional deployments.
    ///
    /// Defaults to `None`.
    pub broadcast_target_port: Option<SocketPort>,
    /// Time between periodic announcement attempts after startup or a route update.
    pub announcement_interval: Duration,
    /// Per-announcer instance identifier encoded into outgoing `Peer` messages.
    ///
    /// The default is nil; production callers should provide a real instance id.
    pub instance_id: Uuid,
    /// Whether this component maintains the peer-announcement UDP socket.
    pub socket_maintenance: PeerAnnouncementSocketMaintenance,
}

impl Options {
    pub const DEFAULT: Self = Self {
        socket_bind_addr: SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            DEFAULT_DISCOVERY_PORT.0,
        ),
        broadcast_target_port: None,
        announcement_interval: Duration::from_secs(5),
        instance_id: Uuid::nil(),
        socket_maintenance: PeerAnnouncementSocketMaintenance::Maintain,
    };

    /// Return the local peer-announcement UDP socket address.
    #[must_use]
    pub const fn socket_bind_addr(&self) -> SocketAddr {
        self.socket_bind_addr
    }

    /// Return the destination port used for peer-announcement broadcasts.
    #[must_use]
    pub fn broadcast_target_port(&self) -> SocketPort {
        self.broadcast_target_port
            .unwrap_or_else(|| SocketPort(self.socket_bind_addr.port()))
    }

    /// Replaces the local peer-announcement UDP socket address.
    #[must_use]
    pub fn with_socket_bind_addr(mut self, socket_bind_addr: SocketAddr) -> Self {
        self.socket_bind_addr = socket_bind_addr;
        self
    }

    /// Replaces the broadcast destination port override.
    #[must_use]
    pub fn with_broadcast_target_port(mut self, broadcast_target_port: Option<SocketPort>) -> Self {
        self.broadcast_target_port = broadcast_target_port;
        self
    }

    /// Replaces the current instance id with `instance_id`.
    #[must_use]
    pub fn with_instance_id(mut self, instance_id: Uuid) -> Self {
        self.instance_id = instance_id;
        self
    }

    /// Replaces the current announcement interval with `announcement_interval`.
    #[must_use]
    pub fn with_announcement_interval(mut self, announcement_interval: Duration) -> Self {
        self.announcement_interval = announcement_interval;
        self
    }

    /// Replaces the peer-announcement socket lifecycle responsibility.
    #[must_use]
    pub fn with_socket_maintenance(
        mut self,
        socket_maintenance: PeerAnnouncementSocketMaintenance,
    ) -> Self {
        self.socket_maintenance = socket_maintenance;
        self
    }
}

impl Default for Options {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Default options for the UDP peer-announcement component.
pub const PEER_ANNOUNCEMENT_DEFAULT_OPTIONS: Options = Options::DEFAULT;

/// Startup result for one peer-announcement component instance.
pub type PeerAnnouncementStartupResult = std::result::Result<(), PeerAnnouncementStartupError>;

/// Describes why the UDP announcer could not become ready for periodic broadcasts.
#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
pub enum PeerAnnouncementStartupError {
    #[snafu(display("Could not load peer-announcement configuration {key}: {reason}"))]
    ConfigurationFailed { key: &'static str, reason: String },
    #[snafu(display("Could not bind the peer-announcement socket at {local_addr}: {reason:?}"))]
    BindFailed {
        local_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    #[snafu(display(
        "Could not enable broadcast on peer-announcement socket {socket_id}: {reason:?}"
    ))]
    ConfigureBroadcastFailed {
        socket_id: SocketId,
        reason: ConfigureFailureReason,
    },
    #[snafu(display(
        "Peer-announcement startup was interrupted before the UDP socket became ready"
    ))]
    Interrupted,
}

/// Creates one startup notification signal for a peer-announcement component.
pub fn peer_announcement_startup_signal() -> (
    KPromise<PeerAnnouncementStartupResult>,
    KFuture<PeerAnnouncementStartupResult>,
) {
    promise::<PeerAnnouncementStartupResult>()
}

/// Load UDP bind options for a peer-announcement socket from Kompact config.
///
/// Both peer-announcement senders and observers use this helper when they are
/// configured as the local socket maintainer.
///
/// # Errors
///
/// Returns [`PeerAnnouncementStartupError`] when the socket configuration cannot be read.
pub fn peer_announcement_bind_options_from_config(
    config: &Config,
) -> std::result::Result<UdpBindOptions, PeerAnnouncementStartupError> {
    let socket_reuse = config
        .read_or_default(&config_keys::PEER_ANNOUNCEMENT_BIND_REUSE_ADDRESS)
        .map_err(|error| PeerAnnouncementStartupError::ConfigurationFailed {
            key: config_keys::PEER_ANNOUNCEMENT_BIND_REUSE_ADDRESS.key,
            reason: error.to_string(),
        })?;
    Ok(UdpBindOptions::default().with_socket_reuse(socket_reuse))
}

/// A route advertised in outgoing peer-announcement messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PeerAnnouncementRoute {
    /// A UDP socket address that should accept follow-up discovery messages.
    Udp(SocketAddr),
}

impl EncodeProto for PeerAnnouncementRoute {
    type Proto = SocketAddress;

    fn encode_proto(&self) -> Self::Proto {
        let route = match self {
            Self::Udp(address) => crate::protocol::DiscoveryRoute::Udp(*address),
        };
        route.encode_proto()
    }
}

/// Kompact component that periodically broadcasts one `Peer` announcement over UDP.
#[derive(ComponentDefinition)]
pub struct PeerAnnouncementComponent {
    ctx: ComponentContext<Self>,
    udp_port: RequiredPort<UdpPort>,
    endpoint_selection_port: RequiredPort<EndpointSelectionPort>,
    options: Options,
    startup_promise: Option<KPromise<PeerAnnouncementStartupResult>>,
    state: SocketState,
    broadcast_addresses: HashMap<MacAddr, SocketAddr>,
    advertised_routes: Vec<PeerAnnouncementRoute>,
    next_transmission_id: TransmissionId,
    announcement_timer: Option<ScheduledTimer>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SocketState {
    /// No peer-announcement socket is currently known to this component.
    Closed,
    /// This component requested a maintained socket bind and is waiting for the matching result.
    Opening {
        /// Request id that must match the eventual bind result.
        request_id: UdpOpenRequestId,
    },
    /// The maintained socket is bound and waiting for broadcast sends to be enabled.
    EnablingBroadcast {
        /// Socket that should become the peer-announcement sender.
        socket_id: SocketId,
    },
    /// Another component maintains the socket and this component is waiting to observe its bind.
    WaitingForSocket,
    /// The peer-announcement socket is known and can be used for announcement sends.
    Running {
        /// Active peer-announcement socket id.
        socket_id: SocketId,
    },
    /// This component requested close for a maintained socket and is waiting for confirmation.
    Closing {
        /// Socket id being closed.
        socket_id: SocketId,
    },
}

impl SocketState {
    fn socket_id(self) -> Option<SocketId> {
        match self {
            Self::EnablingBroadcast { socket_id }
            | Self::Running { socket_id }
            | Self::Closing { socket_id } => Some(socket_id),
            Self::Closed | Self::Opening { .. } | Self::WaitingForSocket => None,
        }
    }
}

/// Actor messages accepted by [`PeerAnnouncementComponent`].
#[derive(Debug)]
pub enum PeerAnnouncementMessage {
    /// Delivery result sent by `flotsync_io` for one UDP announcement send.
    SendResult(UdpSendResult),
}

impl PeerAnnouncementComponent {
    #[must_use]
    pub fn with_options(options: Options) -> Self {
        Self::with_optional_startup_promise(options, None)
    }

    #[must_use]
    pub fn with_options_and_startup_promise(
        options: Options,
        startup_promise: KPromise<PeerAnnouncementStartupResult>,
    ) -> Self {
        Self::with_optional_startup_promise(options, Some(startup_promise))
    }

    /// Return a shared reference to the endpoint-selection input port.
    #[must_use]
    pub fn endpoint_selection_port(&mut self) -> RequiredRef<EndpointSelectionPort> {
        self.endpoint_selection_port.share()
    }

    fn with_optional_startup_promise(
        options: Options,
        startup_promise: Option<KPromise<PeerAnnouncementStartupResult>>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp_port: RequiredPort::uninitialised(),
            endpoint_selection_port: RequiredPort::uninitialised(),
            options,
            startup_promise,
            state: SocketState::Closed,
            broadcast_addresses: HashMap::new(),
            advertised_routes: Vec::new(),
            next_transmission_id: TransmissionId::ONE,
            announcement_timer: None,
        }
    }

    fn socket_bind_addr(&self) -> SocketAddr {
        self.options.socket_bind_addr()
    }

    fn matches_peer_announcement_bind_port(&self, local_addr: SocketAddr) -> bool {
        local_addr.port() == self.options.socket_bind_addr().port()
    }

    fn notify_startup_success(&mut self) {
        self.fulfil_startup(Ok(()));
    }

    fn notify_startup_failure(&mut self, error: PeerAnnouncementStartupError) {
        self.fulfil_startup(Err(error));
    }

    fn interrupt_startup(&mut self) {
        if self.startup_promise.is_some() {
            self.notify_startup_failure(PeerAnnouncementStartupError::Interrupted);
        }
    }

    fn fulfil_startup(&mut self, result: PeerAnnouncementStartupResult) {
        let Some(promise) = self.startup_promise.take() else {
            return;
        };
        if promise.fulfil(result).is_err() {
            debug!(
                self.log(),
                "dropping peer-announcement startup notification because the receiver is gone"
            );
        }
    }

    fn begin_startup(&mut self) -> HandlerResult {
        if self.options.socket_maintenance == PeerAnnouncementSocketMaintenance::Observe {
            self.state = SocketState::WaitingForSocket;
            return Handled::OK;
        }

        let bind_options = match self.bind_options() {
            Ok(bind_options) => bind_options,
            Err(error) => {
                error!(self.log(), "{error}");
                self.notify_startup_failure(error);
                return Handled::SHUTDOWN;
            }
        };
        let request_id = UdpOpenRequestId::new();
        self.state = SocketState::Opening { request_id };
        self.udp_port.trigger(UdpRequest::Bind {
            request_id,
            bind: UdpLocalBind::Exact(self.socket_bind_addr()),
            options: bind_options,
        });
        Handled::OK
    }

    fn bind_options(&self) -> std::result::Result<UdpBindOptions, PeerAnnouncementStartupError> {
        peer_announcement_bind_options_from_config(self.ctx.config())
    }

    fn get_active_broadcast_interfaces() -> Vec<NetworkInterface> {
        datalink::interfaces()
            .into_iter()
            .filter(|interface| {
                interface.mac.is_some()
                    && interface.is_up()
                    && !interface.ips.is_empty()
                    && (interface.is_loopback() || interface.is_broadcast())
            })
            .collect()
    }

    fn get_broadcast_address_for_interface(
        &self,
        interface: &NetworkInterface,
    ) -> Option<SocketAddr> {
        if interface.ips.len() > 1 {
            trace!(
                self.log(),
                "Interface {} has {} IP ranges.\n{}\nArbitrarily picking the first IPv4.",
                interface.name,
                interface.ips.len(),
                interface.ips.iter().join(", ")
            );
        }
        interface
            .ips
            .iter()
            .find(|network| network.is_ipv4())
            .map(|network| {
                let broadcast_addr = network.broadcast();
                SocketAddr::new(broadcast_addr, *self.options.broadcast_target_port())
            })
    }

    fn refresh_broadcast_addresses(&mut self) {
        let active_interfaces = Self::get_active_broadcast_interfaces();
        trace!(
            self.log(),
            "There are {} active interfaces: {}",
            active_interfaces.len(),
            active_interfaces
                .iter()
                .map(|interface| &interface.name)
                .join(", ")
        );

        let deactivated_interfaces = self
            .broadcast_addresses
            .keys()
            .filter(|mac| {
                let mac_opt = Some(**mac);
                !active_interfaces
                    .iter()
                    .any(|interface| interface.mac.as_ref() == mac_opt.as_ref())
            })
            .copied()
            .collect_vec();
        for mac in deactivated_interfaces {
            self.broadcast_addresses.remove(&mac);
            debug!(
                self.log(),
                "Removed no-longer available interface with MAC={mac}"
            );
        }

        for interface in active_interfaces {
            let Some(mac) = interface.mac else {
                trace!(
                    self.log(),
                    "Skipping active interface {} because it has no MAC address", interface.name
                );
                continue;
            };
            if let Some(broadcast_address) = self.get_broadcast_address_for_interface(&interface) {
                if let Some(address) = self.broadcast_addresses.get_mut(&mac) {
                    if address != &broadcast_address {
                        *address = broadcast_address;
                        debug!(
                            self.log(),
                            "Updated interface with MAC={mac} to broadcast_address={broadcast_address}"
                        );
                    }
                } else {
                    debug!(
                        self.log(),
                        "Added interface with MAC={mac} and broadcast_address={broadcast_address}"
                    );
                    self.broadcast_addresses.insert(mac, broadcast_address);
                }
            } else {
                trace!(
                    self.log(),
                    "Could not find a broadcast address for interface: {interface}"
                );
            }
        }
    }

    fn broadcast_message(&self) -> Peer {
        Peer {
            instance_uuid: self.options.instance_id.as_bytes().to_vec(),
            listening_on: self
                .advertised_routes
                .iter()
                .copied()
                .map(|route| route.encode_proto())
                .collect(),
            ..Default::default()
        }
    }

    fn encoded_broadcast_message(&self) -> Vec<u8> {
        self.broadcast_message().encode_to_vec()
    }

    fn send_announcement_to_known_targets(&mut self) -> HandlerResult {
        let SocketState::Running { socket_id } = self.state else {
            return Handled::OK;
        };

        if self.advertised_routes.is_empty() {
            trace!(
                self.log(),
                "No advertised peer routes available for peer announcement"
            );
            return Handled::OK;
        }

        let targets: HashSet<SocketAddr> = self.broadcast_addresses.values().copied().collect();
        if targets.is_empty() {
            trace!(
                self.log(),
                "No broadcast targets available for peer announcement"
            );
            return Handled::OK;
        }

        let payload = self.encoded_broadcast_message();
        let reply_to = self
            .actor_ref()
            .recipient_with(PeerAnnouncementMessage::SendResult);

        for target in targets {
            let transmission_id = self.next_transmission_id.take_next();
            self.udp_port.trigger(UdpRequest::Send {
                socket_id,
                transmission_id,
                payload: IoPayload::Bytes(payload.clone().into()),
                target: Some(target),
                reply_to: reply_to.clone(),
            });
        }
        Handled::OK
    }

    fn announce_to_known_targets_and_set_timer(&mut self) -> HandlerResult {
        let handled = self.send_announcement_to_known_targets();
        if matches!(handled.as_ref(), Ok(Handled::Ok)) && !self.advertised_routes.is_empty() {
            self.set_announcement_timer();
        }
        handled
    }

    fn run_announcement_cycle(&mut self) -> HandlerResult {
        self.refresh_broadcast_addresses();
        self.announce_to_known_targets_and_set_timer()
    }

    fn replace_advertised_routes(&mut self, routes: Vec<PeerAnnouncementRoute>) -> HandlerResult {
        self.advertised_routes = routes;
        if self.advertised_routes.is_empty() {
            trace!(
                self.log(),
                "stopping peer announcements because no advertised routes are available"
            );
            self.clear_announcement_timer();
            return Handled::OK;
        }
        if self.broadcast_addresses.is_empty() {
            self.refresh_broadcast_addresses();
        }
        self.announce_to_known_targets_and_set_timer()
    }

    fn set_announcement_timer(&mut self) {
        self.clear_announcement_timer();

        if !matches!(self.state, SocketState::Running { .. }) {
            return;
        }

        let timer = self.schedule_once(
            self.options.announcement_interval,
            move |component, timeout| component.handle_announcement_timeout(&timeout),
        );
        self.announcement_timer = Some(timer);
    }

    fn clear_announcement_timer(&mut self) {
        if let Some(timer) = self.announcement_timer.take() {
            self.cancel_timer(timer);
        }
    }

    fn handle_announcement_timeout(&mut self, actual_timer: &ScheduledTimer) -> HandlerResult {
        let Some(expected_timer) = self.announcement_timer.take() else {
            return Handled::OK;
        };
        if &expected_timer != actual_timer {
            self.announcement_timer = Some(expected_timer);
            return Handled::OK;
        }

        self.run_announcement_cycle()
    }

    fn request_close(&mut self) {
        self.clear_announcement_timer();
        if self.options.socket_maintenance == PeerAnnouncementSocketMaintenance::Observe {
            // Observe mode does not own the socket, so stopping only discards the observed socket
            // id and returns to the initial state used when waiting for its maintainer.
            self.state = SocketState::WaitingForSocket;
            return;
        }
        match self.state {
            SocketState::Closed | SocketState::WaitingForSocket | SocketState::Closing { .. } => {}
            SocketState::Opening { .. } => {
                // The bridge may already have reserved a socket for this open request, but we keep
                // the shared UDP protocol simple and accept that the orphaned socket will persist
                // until the bridge shuts down.
                debug!(
                    self.log(),
                    "Stopping peer announcement component before UDP bind completed"
                );
                self.state = SocketState::Closed;
            }
            SocketState::EnablingBroadcast { socket_id } | SocketState::Running { socket_id } => {
                debug!(
                    self.log(),
                    "UDP close requested for peer announcement socket"
                );
                self.udp_port.trigger(UdpRequest::Close { socket_id });
                self.state = SocketState::Closing { socket_id };
            }
        }
    }

    fn handle_opened_socket(
        &mut self,
        socket_id: SocketId,
        local_addr: SocketAddr,
    ) -> HandlerResult {
        info!(self.log(), "Sending peer announcements from {local_addr}");
        self.state = SocketState::EnablingBroadcast { socket_id };
        self.udp_port.trigger(UdpRequest::Configure {
            socket_id,
            option: UdpSocketOption::Broadcast(true),
        });
        Handled::OK
    }

    fn handle_bind_failed(
        &mut self,
        local_addr: SocketAddr,
        reason: OpenFailureReason,
    ) -> HandlerResult {
        self.notify_startup_failure(PeerAnnouncementStartupError::BindFailed {
            local_addr,
            reason,
        });
        error!(
            self.log(),
            "Could not bind peer announcement socket at {local_addr}: {reason:?}"
        );
        self.state = SocketState::Closed;
        Handled::SHUTDOWN
    }

    fn handle_observed_bound_socket(
        &mut self,
        socket_id: SocketId,
        local_addr: SocketAddr,
    ) -> HandlerResult {
        info!(
            self.log(),
            "Observed peer announcement socket {socket_id} bound at {local_addr}"
        );
        self.state = SocketState::Running { socket_id };
        self.notify_startup_success();
        self.run_announcement_cycle()
    }

    fn handle_broadcast_configured(&mut self, socket_id: SocketId) -> HandlerResult {
        self.state = SocketState::Running { socket_id };
        self.notify_startup_success();
        self.run_announcement_cycle()
    }

    fn handle_broadcast_configure_failed(
        &mut self,
        socket_id: SocketId,
        reason: ConfigureFailureReason,
    ) -> HandlerResult {
        self.notify_startup_failure(PeerAnnouncementStartupError::ConfigureBroadcastFailed {
            socket_id,
            reason,
        });
        error!(
            self.log(),
            "Could not enable broadcast on peer announcement socket {socket_id}: {reason:?}"
        );
        self.request_close();
        Handled::SHUTDOWN
    }

    fn handle_socket_closed(
        &mut self,
        socket_id: SocketId,
        reason: flotsync_io::prelude::UdpCloseReason,
    ) -> HandlerResult {
        if self.state.socket_id() != Some(socket_id) {
            return Handled::OK;
        }
        info!(
            self.log(),
            "Peer announcement UDP socket closed ({reason:?})"
        );
        self.clear_announcement_timer();
        self.state =
            if self.options.socket_maintenance == PeerAnnouncementSocketMaintenance::Observe {
                SocketState::WaitingForSocket
            } else {
                SocketState::Closed
            };
        Handled::OK
    }

    fn handle_udp_indication(&mut self, indication: &UdpIndication) -> HandlerResult {
        match indication {
            UdpIndication::Bound {
                request_id,
                socket_id,
                local_addr,
            } if matches!(self.state, SocketState::Opening { request_id: current } if current == *request_id) => {
                self.handle_opened_socket(*socket_id, *local_addr)
            }
            UdpIndication::BindFailed {
                request_id,
                local_addr,
                reason,
            } if matches!(self.state, SocketState::Opening { request_id: current } if current == *request_id) => {
                self.handle_bind_failed(*local_addr, *reason)
            }
            UdpIndication::Bound {
                socket_id,
                local_addr,
                ..
            } if self.options.socket_maintenance == PeerAnnouncementSocketMaintenance::Observe
                && matches!(self.state, SocketState::WaitingForSocket)
                && self.matches_peer_announcement_bind_port(*local_addr) =>
            {
                self.handle_observed_bound_socket(*socket_id, *local_addr)
            }
            UdpIndication::Configured { socket_id, option }
                if matches!(self.state, SocketState::EnablingBroadcast { socket_id: current } if current == *socket_id)
                    && *option == UdpSocketOption::Broadcast(true) =>
            {
                self.handle_broadcast_configured(*socket_id)
            }
            UdpIndication::ConfigureFailed {
                socket_id,
                option,
                reason,
            } if matches!(self.state, SocketState::EnablingBroadcast { socket_id: current } if current == *socket_id)
                && *option == UdpSocketOption::Broadcast(true) =>
            {
                self.handle_broadcast_configure_failed(*socket_id, *reason)
            }
            UdpIndication::Closed {
                socket_id,
                remote_addr: _,
                reason,
            } => self.handle_socket_closed(*socket_id, *reason),
            _ => Handled::OK,
        }
    }

    fn handle_send_result(&mut self, result: &UdpSendResult) -> HandlerResult {
        let Some(socket_id) = self.state.socket_id() else {
            return Handled::OK;
        };

        match result {
            UdpSendResult::Ack {
                socket_id: result_socket_id,
                transmission_id,
            } if *result_socket_id == socket_id => {
                trace!(
                    self.log(),
                    "Peer announcement send acknowledged as tx#{:x}", transmission_id.0
                );
            }
            UdpSendResult::Nack {
                socket_id: result_socket_id,
                transmission_id,
                reason,
            } if *result_socket_id == socket_id => {
                debug!(
                    self.log(),
                    "Peer announcement send rejected as tx#{:x}: {reason:?}", transmission_id.0
                );
            }
            _ => {}
        }
        Handled::OK
    }
}

impl ComponentLifecycle for PeerAnnouncementComponent {
    fn on_start(&mut self) -> HandlerResult {
        self.begin_startup()
    }

    fn on_stop(&mut self) -> HandlerResult {
        self.interrupt_startup();
        self.request_close();
        Handled::OK
    }

    fn on_kill(&mut self) -> HandlerResult {
        self.interrupt_startup();
        self.request_close();
        Handled::OK
    }
}

impl Require<UdpPort> for PeerAnnouncementComponent {
    fn handle(&mut self, indication: UdpIndication) -> HandlerResult {
        self.handle_udp_indication(&indication)
    }
}

impl Require<EndpointSelectionPort> for PeerAnnouncementComponent {
    fn handle(&mut self, indication: EndpointSelection) -> HandlerResult {
        let routes = indication
            .endpoints
            .into_iter()
            .map(PeerAnnouncementRoute::Udp)
            .collect();
        self.replace_advertised_routes(routes)
    }
}

impl Actor for PeerAnnouncementComponent {
    type Message = PeerAnnouncementMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            PeerAnnouncementMessage::SendResult(result) => self.handle_send_result(&result),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_io::test_support::{
        build_test_kompact_system,
        build_test_kompact_system_with,
        kill_component,
        start_component,
    };
    use flotsync_messages::discovery::{ip_address, socket_address};
    use flotsync_utils::kompact_testing::{
        ObservedRequest,
        PortTesterComponent,
        PortTestingExt,
        PortTestingRefExt,
    };
    use std::{
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        sync::Arc,
    };

    type UdpTesterComponent = PortTesterComponent<UdpPort>;

    fn observe_udp_request<F>(
        probe: &Arc<Component<UdpTesterComponent>>,
        predicate: F,
        context: &'static str,
    ) -> ObservedRequest<UdpPort>
    where
        F: Fn(&UdpRequest) -> bool + Send + 'static,
    {
        probe
            .actor_ref()
            .observe_request(predicate)
            .wait_timeout(Duration::from_secs(1))
            .expect(context)
            .expect("UDP probe should stay live")
    }

    fn observe_udp_request_from<F>(
        probe: &Arc<Component<UdpTesterComponent>>,
        start_index: usize,
        predicate: F,
        context: &'static str,
    ) -> ObservedRequest<UdpPort>
    where
        F: Fn(&UdpRequest) -> bool + Send + 'static,
    {
        probe
            .actor_ref()
            .observe_request_from(start_index, predicate)
            .wait_timeout(Duration::from_secs(1))
            .expect(context)
            .expect("UDP probe should stay live")
    }

    fn expect_no_udp_request(probe: &Arc<Component<UdpTesterComponent>>, reason: &'static str) {
        probe
            .actor_ref()
            .fail_if_request_observed_from(0, Duration::from_millis(100), |_| true)
            .wait_timeout(Duration::from_secs(1))
            .expect("UDP request absence check should complete")
            .expect("UDP probe should stay live")
            .expect(reason);
    }

    fn expect_no_udp_request_from(
        probe: &Arc<Component<UdpTesterComponent>>,
        start_index: usize,
        reason: &'static str,
    ) {
        probe
            .actor_ref()
            .fail_if_request_observed_from(start_index, Duration::from_millis(100), |_| true)
            .wait_timeout(Duration::from_secs(1))
            .expect("UDP request absence check should complete")
            .expect("UDP probe should stay live")
            .expect(reason);
    }

    fn inject_udp_indication(
        probe: &Arc<Component<UdpTesterComponent>>,
        indication: UdpIndication,
    ) {
        probe.actor_ref().inject_indication(indication);
    }

    /// Publish selected discovery endpoints through the component's endpoint-selection port.
    fn trigger_endpoint_selection(
        system: &KompactSystem,
        component: &Arc<Component<PeerAnnouncementComponent>>,
        endpoints: impl IntoIterator<Item = SocketAddr>,
    ) {
        let endpoint_selection_port =
            component.on_definition(|component| component.endpoint_selection_port.share());
        system.trigger_i(
            EndpointSelection::from_endpoints(endpoints),
            &endpoint_selection_port,
        );
    }

    fn ipv4_interface(cidr: &str) -> NetworkInterface {
        NetworkInterface {
            name: "test0".to_string(),
            description: "test interface".to_string(),
            index: 0,
            mac: Some(MacAddr(0, 1, 2, 3, 4, 5)),
            ips: vec![cidr.parse().expect("valid IPv4 network")],
            flags: 0,
        }
    }

    #[test]
    fn peer_announcement_component_binds_and_enables_broadcast() {
        let system = build_test_kompact_system();
        let probe = system.create(UdpPort::tester_component_sidecar);
        let component =
            system.create(|| PeerAnnouncementComponent::with_options(Options::default()));
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let bind_request = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Bind { .. }),
            "UDP bind request should be observed",
        );
        let request_id = match bind_request.request() {
            UdpRequest::Bind {
                request_id,
                bind,
                options,
            } => {
                assert_eq!(
                    *bind,
                    UdpLocalBind::Exact(SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                        *DEFAULT_DISCOVERY_PORT,
                    ))
                );
                assert_eq!(*options, UdpBindOptions::default().with_socket_reuse(true));
                *request_id
            }
            other => unreachable!("filtered to bind request, got {other:?}"),
        };

        inject_udp_indication(
            &probe,
            UdpIndication::Bound {
                request_id,
                socket_id: SocketId(7),
                local_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 41000)),
            },
        );

        let configure_request = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Configure { .. }),
            "UDP configure request should be observed",
        );
        match configure_request.request() {
            UdpRequest::Configure { socket_id, option } => {
                assert_eq!(*socket_id, SocketId(7));
                assert_eq!(*option, UdpSocketOption::Broadcast(true));
            }
            other => unreachable!("filtered to configure request, got {other:?}"),
        }

        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_broadcast_targets_default_to_socket_bind_port() {
        let options = Options::DEFAULT.with_socket_bind_addr(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            53_000,
        )));
        let component = PeerAnnouncementComponent::with_options(options);
        let interface = ipv4_interface("192.168.5.10/24");

        assert_eq!(
            component.get_broadcast_address_for_interface(&interface),
            Some(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(192, 168, 5, 255),
                53_000,
            )))
        );
    }

    #[test]
    fn peer_announcement_broadcast_target_override_changes_only_target_port() {
        let options = Options::DEFAULT
            .with_socket_bind_addr(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::UNSPECIFIED,
                53_000,
            )))
            .with_broadcast_target_port(Some(SocketPort(53_001)));
        let component = PeerAnnouncementComponent::with_options(options);
        let interface = ipv4_interface("192.168.6.10/24");

        assert_eq!(
            component.socket_bind_addr().port(),
            53_000,
            "broadcast target override must not change the local socket bind address"
        );
        assert_eq!(
            component.get_broadcast_address_for_interface(&interface),
            Some(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(192, 168, 6, 255),
                53_001,
            )))
        );
    }

    #[test]
    fn peer_announcement_component_applies_bind_reuse_config_override() {
        let system = build_test_kompact_system_with(|config| {
            config.set_config_value(&config_keys::PEER_ANNOUNCEMENT_BIND_REUSE_ADDRESS, false);
        });
        let probe = system.create(UdpPort::tester_component_sidecar);
        let component =
            system.create(|| PeerAnnouncementComponent::with_options(Options::default()));
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let bind_request = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Bind { .. }),
            "UDP bind request should be observed",
        );
        match bind_request.request() {
            UdpRequest::Bind { options, .. } => {
                assert_eq!(*options, UdpBindOptions::default().with_socket_reuse(false));
            }
            other => unreachable!("filtered to bind request, got {other:?}"),
        }

        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_deduplicates_targets_and_uses_configured_instance_id() {
        let options = Options::DEFAULT
            .with_instance_id(
                Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("valid UUID"),
            )
            .with_announcement_interval(Duration::from_mins(1));
        let expected_instance_id = options.instance_id;

        let system = build_test_kompact_system();
        let probe = system.create(UdpPort::tester_component_sidecar);
        let component =
            system.create(move || PeerAnnouncementComponent::with_options(options.clone()));
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        component.on_definition(|component| {
            component.clear_announcement_timer();
            component.state = SocketState::Running {
                socket_id: SocketId(11),
            };
            component.broadcast_addresses.insert(
                MacAddr(0, 1, 2, 3, 4, 5),
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 255), 52156)),
            );
            component.broadcast_addresses.insert(
                MacAddr(0, 1, 2, 3, 4, 6),
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 255), 52156)),
            );
        });

        trigger_endpoint_selection(
            &system,
            &component,
            [SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(10, 0, 0, 42),
                52157,
            ))],
        );
        let send = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Send { .. }),
            "UDP send request should be observed",
        );
        match send.request() {
            UdpRequest::Send {
                socket_id,
                payload,
                target,
                ..
            } => {
                assert_eq!(*socket_id, SocketId(11));
                assert_eq!(
                    *target,
                    Some(SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::new(192, 168, 1, 255),
                        52156
                    )))
                );

                let payload = payload.to_vec();
                let message = Peer::decode_from_slice(&payload).expect("decode peer announcement");
                assert_eq!(message.instance_uuid, expected_instance_id.as_bytes());
                assert_eq!(message.listening_on.len(), 1);
                assert_udp_route(&message.listening_on[0], &[10, 0, 0, 42], 52157);
            }
            other => unreachable!("filtered to send request, got {other:?}"),
        }
        expect_no_udp_request_from(
            &probe,
            send.index() + 1,
            "duplicate broadcast targets should collapse to one send request",
        );

        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_suppresses_empty_route_announcements() {
        let options = Options::DEFAULT.with_announcement_interval(Duration::from_mins(1));

        let system = build_test_kompact_system();
        let probe = system.create(UdpPort::tester_component_sidecar);
        let component =
            system.create(move || PeerAnnouncementComponent::with_options(options.clone()));
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        component.on_definition(|component| {
            component.clear_announcement_timer();
            component.state = SocketState::Running {
                socket_id: SocketId(12),
            };
            component.broadcast_addresses.insert(
                MacAddr(0, 1, 2, 3, 4, 7),
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 2, 255), 52156)),
            );
        });

        let bind = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Bind { .. }),
            "UDP bind request should be observed",
        );
        assert!(matches!(bind.request(), UdpRequest::Bind { .. }));
        trigger_endpoint_selection(&system, &component, []);
        expect_no_udp_request_from(
            &probe,
            bind.index() + 1,
            "empty route sets should not emit UDP send requests",
        );
        component.on_definition(|component| {
            assert!(
                component.announcement_timer.is_none(),
                "empty route sets should stop the announcement timer"
            );
        });

        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_announces_after_every_route_update() {
        let options = Options::DEFAULT.with_announcement_interval(Duration::from_mins(1));

        let system = build_test_kompact_system();
        let probe = system.create(UdpPort::tester_component_sidecar);
        let component =
            system.create(move || PeerAnnouncementComponent::with_options(options.clone()));
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        component.on_definition(|component| {
            component.clear_announcement_timer();
            component.state = SocketState::Running {
                socket_id: SocketId(13),
            };
            component.broadcast_addresses.insert(
                MacAddr(0, 1, 2, 3, 4, 8),
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 3, 255), 52156)),
            );
        });

        let routes = [SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(10, 0, 0, 43),
            52158,
        ))];
        trigger_endpoint_selection(&system, &component, routes);
        let cursor = assert_peer_send(&probe, 0, SocketId(13));

        trigger_endpoint_selection(&system, &component, routes);
        assert_peer_send(&probe, cursor, SocketId(13));

        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_closes_socket_on_kill() {
        let system = build_test_kompact_system();
        let probe = system.create(UdpPort::tester_component_sidecar);
        let component =
            system.create(|| PeerAnnouncementComponent::with_options(Options::default()));
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        component.on_definition(|component| {
            component.clear_announcement_timer();
            component.state = SocketState::Running {
                socket_id: SocketId(21),
            };
        });

        kill_component(&system, component);

        let close = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Close { .. }),
            "UDP close request should be observed",
        );
        match close.request() {
            UdpRequest::Close { socket_id } => {
                assert_eq!(*socket_id, SocketId(21));
            }
            other => unreachable!("filtered to close request, got {other:?}"),
        }

        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_observe_close_returns_to_waiting_for_socket() {
        let mut component = PeerAnnouncementComponent::with_options(
            Options::DEFAULT.with_socket_maintenance(PeerAnnouncementSocketMaintenance::Observe),
        );
        component.state = SocketState::Running {
            socket_id: SocketId(22),
        };

        component.request_close();

        assert_eq!(component.state, SocketState::WaitingForSocket);
    }

    #[test]
    fn peer_announcement_component_interrupts_startup_waiter_on_kill() {
        let system = build_test_kompact_system();
        let probe = system.create(UdpPort::tester_component_sidecar);
        let (startup_promise, startup_future) = peer_announcement_startup_signal();
        let component = system.create(move || {
            PeerAnnouncementComponent::with_options_and_startup_promise(
                Options::default(),
                startup_promise,
            )
        });
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let bind = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Bind { .. }),
            "UDP bind request should be observed",
        );
        match bind.request() {
            UdpRequest::Bind { .. } => {}
            other => unreachable!("filtered to bind request, got {other:?}"),
        }

        kill_component(&system, component);

        let startup_result = startup_future
            .wait_timeout(Duration::from_millis(100))
            .expect("startup result should complete");
        assert_eq!(
            startup_result,
            Err(PeerAnnouncementStartupError::Interrupted)
        );

        expect_no_udp_request_from(
            &probe,
            bind.index() + 1,
            "stopping during Opening should not emit extra UDP requests",
        );

        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_reports_startup_success_after_broadcast_is_enabled() {
        let system = build_test_kompact_system();
        let probe = system.create(UdpPort::tester_component_sidecar);
        let (startup_promise, startup_future) = peer_announcement_startup_signal();
        let component = system.create(move || {
            PeerAnnouncementComponent::with_options_and_startup_promise(
                Options::default(),
                startup_promise,
            )
        });
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let bind = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Bind { .. }),
            "UDP bind request should be observed",
        );
        let request_id = match bind.request() {
            UdpRequest::Bind { request_id, .. } => *request_id,
            other => unreachable!("filtered to bind request, got {other:?}"),
        };

        inject_udp_indication(
            &probe,
            UdpIndication::Bound {
                request_id,
                socket_id: SocketId(31),
                local_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 41001)),
            },
        );

        let configure = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Configure { .. }),
            "UDP configure request should be observed",
        );
        match configure.request() {
            UdpRequest::Configure { socket_id, option } => {
                assert_eq!(*socket_id, SocketId(31));
                assert_eq!(*option, UdpSocketOption::Broadcast(true));
            }
            other => unreachable!("filtered to configure request, got {other:?}"),
        }

        inject_udp_indication(
            &probe,
            UdpIndication::Configured {
                socket_id: SocketId(31),
                option: UdpSocketOption::Broadcast(true),
            },
        );

        let startup_result = startup_future
            .wait_timeout(Duration::from_millis(100))
            .expect("startup result should complete");
        assert_eq!(startup_result, Ok(()));

        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_observe_startup_waits_for_matching_socket() {
        let system = build_test_kompact_system();
        let probe = system.create(UdpPort::tester_component_sidecar);
        let (startup_promise, startup_future) = peer_announcement_startup_signal();
        let options =
            Options::DEFAULT.with_socket_maintenance(PeerAnnouncementSocketMaintenance::Observe);
        let component = system.create(move || {
            PeerAnnouncementComponent::with_options_and_startup_promise(
                options.clone(),
                startup_promise,
            )
        });
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        component.on_definition(|component| {
            assert_eq!(component.state, SocketState::WaitingForSocket);
            assert!(
                component.startup_promise.is_some(),
                "observe startup should wait for a matching socket"
            );
        });
        expect_no_udp_request(&probe, "observe mode should not bind its own UDP socket");

        inject_udp_indication(
            &probe,
            UdpIndication::Bound {
                request_id: UdpOpenRequestId::new(),
                socket_id: SocketId(33),
                local_addr: SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    *DEFAULT_DISCOVERY_PORT + 1,
                )),
            },
        );
        component.on_definition(|component| {
            assert_eq!(component.state, SocketState::WaitingForSocket);
            assert!(
                component.startup_promise.is_some(),
                "observe startup should ignore other UDP ports"
            );
        });

        inject_udp_indication(
            &probe,
            UdpIndication::Bound {
                request_id: UdpOpenRequestId::new(),
                socket_id: SocketId(34),
                local_addr: SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    *DEFAULT_DISCOVERY_PORT,
                )),
            },
        );

        let startup_result = startup_future
            .wait_timeout(Duration::from_millis(100))
            .expect("startup result should complete");
        assert_eq!(startup_result, Ok(()));

        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_reports_bind_failure_to_startup_waiter() {
        let system = build_test_kompact_system();
        let probe = system.create(UdpPort::tester_component_sidecar);
        let (startup_promise, startup_future) = peer_announcement_startup_signal();
        let component = system.create(move || {
            PeerAnnouncementComponent::with_options_and_startup_promise(
                Options::default(),
                startup_promise,
            )
        });
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let bind = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Bind { .. }),
            "UDP bind request should be observed",
        );
        let request_id = match bind.request() {
            UdpRequest::Bind { request_id, .. } => *request_id,
            other => unreachable!("filtered to bind request, got {other:?}"),
        };

        let failed_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 41002));
        inject_udp_indication(
            &probe,
            UdpIndication::BindFailed {
                request_id,
                local_addr: failed_addr,
                reason: OpenFailureReason::Io(std::io::ErrorKind::AddrInUse),
            },
        );

        let startup_result = startup_future
            .wait_timeout(Duration::from_millis(100))
            .expect("startup result should complete");
        assert_eq!(
            startup_result,
            Err(PeerAnnouncementStartupError::BindFailed {
                local_addr: failed_addr,
                reason: OpenFailureReason::Io(std::io::ErrorKind::AddrInUse),
            })
        );

        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_reports_configure_failure_to_startup_waiter() {
        let system = build_test_kompact_system();
        let probe = system.create(UdpPort::tester_component_sidecar);
        let (startup_promise, startup_future) = peer_announcement_startup_signal();
        let component = system.create(move || {
            PeerAnnouncementComponent::with_options_and_startup_promise(
                Options::default(),
                startup_promise,
            )
        });
        biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let bind = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Bind { .. }),
            "UDP bind request should be observed",
        );
        let request_id = match bind.request() {
            UdpRequest::Bind { request_id, .. } => *request_id,
            other => unreachable!("filtered to bind request, got {other:?}"),
        };

        inject_udp_indication(
            &probe,
            UdpIndication::Bound {
                request_id,
                socket_id: SocketId(32),
                local_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 41003)),
            },
        );
        let _configure = observe_udp_request(
            &probe,
            |request| matches!(request, UdpRequest::Configure { .. }),
            "UDP configure request should be observed",
        );

        inject_udp_indication(
            &probe,
            UdpIndication::ConfigureFailed {
                socket_id: SocketId(32),
                option: UdpSocketOption::Broadcast(true),
                reason: ConfigureFailureReason::Io(std::io::ErrorKind::PermissionDenied),
            },
        );

        let startup_result = startup_future
            .wait_timeout(Duration::from_millis(100))
            .expect("startup result should complete");
        assert_eq!(
            startup_result,
            Err(PeerAnnouncementStartupError::ConfigureBroadcastFailed {
                socket_id: SocketId(32),
                reason: ConfigureFailureReason::Io(std::io::ErrorKind::PermissionDenied),
            })
        );

        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    fn assert_peer_send(
        probe: &Arc<Component<UdpTesterComponent>>,
        start_index: usize,
        expected_socket_id: SocketId,
    ) -> usize {
        let send = observe_udp_request_from(
            probe,
            start_index,
            |request| matches!(request, UdpRequest::Send { .. }),
            "UDP send request should be observed",
        );
        match send.request() {
            UdpRequest::Send { socket_id, .. } => {
                assert_eq!(*socket_id, expected_socket_id);
            }
            other => unreachable!("filtered to send request, got {other:?}"),
        }
        send.index() + 1
    }

    fn assert_udp_route(route: &SocketAddress, expected_ip: &[u8], expected_port: u32) {
        assert_eq!(
            route.protocol.as_known(),
            Some(socket_address::Protocol::PROTOCOL_UDP)
        );
        assert_eq!(route.port, expected_port);

        let address = route.address.as_option().expect("route address is set");
        assert!(
            matches!(
                address.address.as_ref(),
                Some(ip_address::Address::Ipv4Bytes(bytes)) if bytes.as_slice() == expected_ip
            ),
            "expected IPv4 route address {expected_ip:?}, got {:?}",
            address.address
        );
    }
}
