use super::*;
use crate::{SocketPort, kompact::prelude::*};
use flotsync_io::prelude::{
    ConfigureFailureReason,
    IoPayload,
    OpenFailureReason,
    SocketId,
    TransmissionId,
    UdpIndication,
    UdpLocalBind,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
    UdpSendResult,
    UdpSocketOption,
};
use flotsync_messages::{discovery::Peer, protobuf::Message};
use itertools::Itertools;
use pnet::{
    datalink::{self, NetworkInterface},
    util::MacAddr,
};
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct Options {
    pub bind_addr: IpAddr,
    pub bind_port: SocketPort,
    pub port: SocketPort,
    pub announcement_interval: Duration,
    pub instance_id: Uuid,
}

impl Options {
    pub const DEFAULT: Self = Self {
        bind_addr: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        bind_port: SocketPort(0),
        port: SocketPort(52156),
        announcement_interval: Duration::from_secs(5),
        instance_id: Uuid::nil(),
    };

    /// Replaces the current instance id with `instance_id`.
    pub fn with_instance_id(mut self, instance_id: Uuid) -> Self {
        self.instance_id = instance_id;
        self
    }

    /// Replaces the current announcement interval with `announcement_interval`.
    pub fn with_announcement_interval(mut self, announcement_interval: Duration) -> Self {
        self.announcement_interval = announcement_interval;
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Snafu)]
pub enum PeerAnnouncementStartupError {
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

/// Kompact component that periodically broadcasts one `Peer` announcement over UDP.
#[derive(ComponentDefinition)]
pub struct PeerAnnouncementComponent {
    ctx: ComponentContext<Self>,
    udp: RequiredPort<UdpPort>,
    options: Options,
    startup_promise: Option<KPromise<PeerAnnouncementStartupResult>>,
    state: SocketState,
    broadcast_addresses: HashMap<MacAddr, SocketAddr>,
    next_transmission_id: TransmissionId,
    announcement_timer: Option<AnnouncementTimerState>,
    next_timer_generation: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SocketState {
    Closed,
    Opening { request_id: UdpOpenRequestId },
    EnablingBroadcast { socket_id: SocketId },
    Running { socket_id: SocketId },
    Closing { socket_id: SocketId },
}

impl SocketState {
    fn socket_id(self) -> Option<SocketId> {
        match self {
            Self::EnablingBroadcast { socket_id }
            | Self::Running { socket_id }
            | Self::Closing { socket_id } => Some(socket_id),
            Self::Closed | Self::Opening { .. } => None,
        }
    }
}

#[derive(Debug)]
struct AnnouncementTimerState {
    generation: usize,
    timer: ScheduledTimer,
}

#[derive(Debug)]
pub enum PeerAnnouncementMessage {
    SendResult(UdpSendResult),
}

impl PeerAnnouncementComponent {
    pub fn with_options(options: Options) -> Self {
        Self::with_optional_startup_promise(options, None)
    }

    pub fn with_options_and_startup_promise(
        options: Options,
        startup_promise: KPromise<PeerAnnouncementStartupResult>,
    ) -> Self {
        Self::with_optional_startup_promise(options, Some(startup_promise))
    }

    fn with_optional_startup_promise(
        options: Options,
        startup_promise: Option<KPromise<PeerAnnouncementStartupResult>>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: RequiredPort::uninitialised(),
            options,
            startup_promise,
            state: SocketState::Closed,
            broadcast_addresses: HashMap::new(),
            next_transmission_id: TransmissionId::ONE,
            announcement_timer: None,
            next_timer_generation: 1,
        }
    }

    fn bind_address(&self) -> SocketAddr {
        SocketAddr::new(self.options.bind_addr, *self.options.bind_port)
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

    fn begin_startup(&mut self) {
        let request_id = UdpOpenRequestId::new();
        self.state = SocketState::Opening { request_id };
        self.udp.trigger(UdpRequest::Bind {
            request_id,
            bind: UdpLocalBind::Exact(self.bind_address()),
        });
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
                SocketAddr::new(broadcast_addr, *self.options.port)
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
            let mac = interface
                .mac
                .expect("active broadcast interfaces are filtered to have a MAC address");
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
            ..Default::default()
        }
    }

    fn encoded_broadcast_message(&self) -> Result<Vec<u8>> {
        self.broadcast_message()
            .write_to_bytes()
            .context(ProtoSnafu)
    }

    fn send_announcement_to_known_targets(&mut self) -> Handled {
        let SocketState::Running { socket_id } = self.state else {
            return Handled::Ok;
        };

        let targets: HashSet<SocketAddr> = self.broadcast_addresses.values().copied().collect();
        if targets.is_empty() {
            trace!(
                self.log(),
                "No broadcast targets available for peer announcement"
            );
            return Handled::Ok;
        }

        let payload = match self.encoded_broadcast_message() {
            Ok(payload) => payload,
            Err(error) => {
                error!(
                    self.log(),
                    "Could not encode peer announcement broadcast payload: {error}"
                );
                self.request_close();
                return Handled::DieNow;
            }
        };
        let reply_to = self
            .actor_ref()
            .recipient_with(PeerAnnouncementMessage::SendResult);

        for target in targets {
            let transmission_id = self.next_transmission_id.take_next();
            self.udp.trigger(UdpRequest::Send {
                socket_id,
                transmission_id,
                payload: IoPayload::Bytes(payload.clone().into()),
                target: Some(target),
                reply_to: reply_to.clone(),
            });
        }
        Handled::Ok
    }

    fn run_announcement_cycle(&mut self) -> Handled {
        self.refresh_broadcast_addresses();
        let handled = self.send_announcement_to_known_targets();
        if matches!(handled, Handled::Ok) {
            self.arm_announcement_timer();
        }
        handled
    }

    fn arm_announcement_timer(&mut self) {
        self.clear_announcement_timer();

        if !matches!(self.state, SocketState::Running { .. }) {
            return;
        }

        let generation = self.next_timer_generation;
        self.next_timer_generation = self.next_timer_generation.wrapping_add(1);
        let timer = self.schedule_once(self.options.announcement_interval, move |component, _| {
            component.handle_announcement_timeout(generation)
        });
        self.announcement_timer = Some(AnnouncementTimerState { generation, timer });
    }

    fn clear_announcement_timer(&mut self) {
        if let Some(timer) = self.announcement_timer.take() {
            self.cancel_timer(timer.timer);
        }
    }

    fn handle_announcement_timeout(&mut self, generation: usize) -> Handled {
        let Some(timer) = self.announcement_timer.take() else {
            return Handled::Ok;
        };
        if timer.generation != generation {
            self.announcement_timer = Some(timer);
            return Handled::Ok;
        }

        self.run_announcement_cycle()
    }

    fn request_close(&mut self) {
        self.clear_announcement_timer();
        match self.state {
            SocketState::Closed | SocketState::Closing { .. } => {}
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
                self.udp.trigger(UdpRequest::Close { socket_id });
                self.state = SocketState::Closing { socket_id };
            }
        }
    }

    fn handle_udp_indication(&mut self, indication: UdpIndication) -> Handled {
        match indication {
            UdpIndication::Bound {
                request_id,
                socket_id,
                local_addr,
            } if matches!(self.state, SocketState::Opening { request_id: current } if current == request_id) =>
            {
                info!(self.log(), "Sending peer announcements from {local_addr}");
                self.state = SocketState::EnablingBroadcast { socket_id };
                self.udp.trigger(UdpRequest::Configure {
                    socket_id,
                    option: UdpSocketOption::Broadcast(true),
                });
                Handled::Ok
            }
            UdpIndication::BindFailed {
                request_id,
                local_addr,
                reason,
            } if matches!(self.state, SocketState::Opening { request_id: current } if current == request_id) =>
            {
                self.notify_startup_failure(PeerAnnouncementStartupError::BindFailed {
                    local_addr,
                    reason,
                });
                error!(
                    self.log(),
                    "Could not bind peer announcement socket at {local_addr}: {reason:?}"
                );
                self.state = SocketState::Closed;
                Handled::DieNow
            }
            UdpIndication::Configured { socket_id, option }
                if matches!(self.state, SocketState::EnablingBroadcast { socket_id: current } if current == socket_id)
                    && option == UdpSocketOption::Broadcast(true) =>
            {
                self.state = SocketState::Running { socket_id };
                self.notify_startup_success();
                self.run_announcement_cycle()
            }
            UdpIndication::ConfigureFailed {
                socket_id,
                option,
                reason,
            } if matches!(self.state, SocketState::EnablingBroadcast { socket_id: current } if current == socket_id)
                && option == UdpSocketOption::Broadcast(true) =>
            {
                self.notify_startup_failure(
                    PeerAnnouncementStartupError::ConfigureBroadcastFailed { socket_id, reason },
                );
                error!(
                    self.log(),
                    "Could not enable broadcast on peer announcement socket {socket_id}: {reason:?}"
                );
                self.request_close();
                Handled::DieNow
            }
            UdpIndication::Closed {
                socket_id,
                remote_addr: _,
                reason,
            } if self.state.socket_id() == Some(socket_id) => {
                info!(
                    self.log(),
                    "Peer announcement UDP socket closed ({reason:?})"
                );
                self.clear_announcement_timer();
                self.state = SocketState::Closed;
                Handled::Ok
            }
            _ => Handled::Ok,
        }
    }

    fn handle_send_result(&mut self, result: UdpSendResult) -> Handled {
        let Some(socket_id) = self.state.socket_id() else {
            return Handled::Ok;
        };

        match result {
            UdpSendResult::Ack {
                socket_id: result_socket_id,
                transmission_id,
            } if result_socket_id == socket_id => {
                trace!(
                    self.log(),
                    "Peer announcement send acknowledged as tx#{:x}", transmission_id.0
                );
            }
            UdpSendResult::Nack {
                socket_id: result_socket_id,
                transmission_id,
                reason,
            } if result_socket_id == socket_id => {
                debug!(
                    self.log(),
                    "Peer announcement send rejected as tx#{:x}: {reason:?}", transmission_id.0
                );
            }
            _ => {}
        }
        Handled::Ok
    }
}

impl ComponentLifecycle for PeerAnnouncementComponent {
    fn on_start(&mut self) -> Handled {
        self.begin_startup();
        Handled::Ok
    }

    fn on_stop(&mut self) -> Handled {
        self.interrupt_startup();
        self.request_close();
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        self.interrupt_startup();
        self.request_close();
        Handled::Ok
    }
}

impl Require<UdpPort> for PeerAnnouncementComponent {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        self.handle_udp_indication(indication)
    }
}

impl Actor for PeerAnnouncementComponent {
    type Message = PeerAnnouncementMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            PeerAnnouncementMessage::SendResult(result) => self.handle_send_result(result),
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        // Safe assumption for this component: peer announcements only use local actor messages and
        // the local UDP port bridge. Without a network dispatcher in the system, no remote actor
        // message can reach this handler. If that system invariant is broken, we prefer to crash
        // loudly instead of pretending the component supports network traffic.
        unreachable!("PeerAnnouncementComponent does not use Kompact network actor messages");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_io::test_support::{
        build_test_kompact_system,
        init_test_logger,
        kill_component,
        recv_until,
        start_component,
    };
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        sync::mpsc,
    };

    #[derive(ComponentDefinition)]
    struct UdpRequestProbe {
        ctx: ComponentContext<Self>,
        udp: ProvidedPort<UdpPort>,
        requests: mpsc::Sender<UdpRequest>,
    }

    impl UdpRequestProbe {
        fn new(requests: mpsc::Sender<UdpRequest>) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                udp: ProvidedPort::uninitialised(),
                requests,
            }
        }
    }

    ignore_lifecycle!(UdpRequestProbe);

    impl Provide<UdpPort> for UdpRequestProbe {
        fn handle(&mut self, request: UdpRequest) -> Handled {
            self.requests
                .send(request)
                .expect("UDP request receiver must stay live during tests");
            Handled::Ok
        }
    }

    impl Actor for UdpRequestProbe {
        type Message = Never;

        fn receive_local(&mut self, _msg: Self::Message) -> Handled {
            unreachable!("Never type is empty")
        }

        fn receive_network(&mut self, _msg: NetMessage) -> Handled {
            // Test probe helper: this component only participates in local test wiring and never
            // in Kompact network dispatch.
            unreachable!("UdpRequestProbe does not use Kompact network actor messages");
        }
    }

    #[test]
    fn peer_announcement_component_binds_and_enables_broadcast() {
        init_test_logger();

        let system = build_test_kompact_system();
        let (requests_tx, requests_rx) = mpsc::channel();
        let probe = system.create(move || UdpRequestProbe::new(requests_tx));
        let component =
            system.create(|| PeerAnnouncementComponent::with_options(Options::default()));
        let connection = biconnect_components::<UdpPort, _, _>(&probe, &component)
            .expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let request_id = match recv_until(&requests_rx, |request| {
            matches!(request, UdpRequest::Bind { .. })
        }) {
            UdpRequest::Bind { request_id, bind } => {
                assert_eq!(
                    bind,
                    UdpLocalBind::Exact(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0,))
                );
                request_id
            }
            other => unreachable!("filtered to bind request, got {other:?}"),
        };

        probe.on_definition(|probe| {
            probe.udp.trigger(UdpIndication::Bound {
                request_id,
                socket_id: SocketId(7),
                local_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 41000)),
            });
        });

        match recv_until(&requests_rx, |request| {
            matches!(request, UdpRequest::Configure { .. })
        }) {
            UdpRequest::Configure { socket_id, option } => {
                assert_eq!(socket_id, SocketId(7));
                assert_eq!(option, UdpSocketOption::Broadcast(true));
            }
            other => unreachable!("filtered to configure request, got {other:?}"),
        }

        drop(connection);
        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_deduplicates_targets_and_uses_configured_instance_id() {
        init_test_logger();

        let options = Options::DEFAULT
            .with_instance_id(
                Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("valid UUID"),
            )
            .with_announcement_interval(Duration::from_secs(60));
        let expected_instance_id = options.instance_id;

        let system = build_test_kompact_system();
        let (requests_tx, requests_rx) = mpsc::channel();
        let probe = system.create(move || UdpRequestProbe::new(requests_tx));
        let component =
            system.create(move || PeerAnnouncementComponent::with_options(options.clone()));
        let connection = biconnect_components::<UdpPort, _, _>(&probe, &component)
            .expect("connect probe/component");

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

            let handled = component.send_announcement_to_known_targets();
            assert!(matches!(handled, Handled::Ok));
        });

        let send = recv_until(&requests_rx, |request| {
            matches!(request, UdpRequest::Send { .. })
        });
        match send {
            UdpRequest::Send {
                socket_id,
                payload,
                target,
                ..
            } => {
                assert_eq!(socket_id, SocketId(11));
                assert_eq!(
                    target,
                    Some(SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::new(192, 168, 1, 255),
                        52156
                    )))
                );

                let message = Peer::parse_from_bytes(&payload.create_byte_clone())
                    .expect("decode peer announcement");
                assert_eq!(message.instance_uuid, expected_instance_id.as_bytes());
            }
            other => unreachable!("filtered to send request, got {other:?}"),
        }
        assert!(
            requests_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "duplicate broadcast targets should collapse to one send request"
        );

        drop(connection);
        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_closes_socket_on_kill() {
        init_test_logger();

        let system = build_test_kompact_system();
        let (requests_tx, requests_rx) = mpsc::channel();
        let probe = system.create(move || UdpRequestProbe::new(requests_tx));
        let component =
            system.create(|| PeerAnnouncementComponent::with_options(Options::default()));
        let connection = biconnect_components::<UdpPort, _, _>(&probe, &component)
            .expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        component.on_definition(|component| {
            component.clear_announcement_timer();
            component.state = SocketState::Running {
                socket_id: SocketId(21),
            };
        });

        drop(connection);
        kill_component(&system, component);

        match recv_until(&requests_rx, |request| {
            matches!(request, UdpRequest::Close { .. })
        }) {
            UdpRequest::Close { socket_id } => {
                assert_eq!(socket_id, SocketId(21));
            }
            other => unreachable!("filtered to close request, got {other:?}"),
        }

        kill_component(&system, probe);
        system.shutdown().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_interrupts_startup_waiter_on_kill() {
        init_test_logger();

        let system = build_test_kompact_system();
        let (requests_tx, requests_rx) = mpsc::channel();
        let probe = system.create(move || UdpRequestProbe::new(requests_tx));
        let (startup_promise, startup_future) = peer_announcement_startup_signal();
        let component = system.create(move || {
            PeerAnnouncementComponent::with_options_and_startup_promise(
                Options::default(),
                startup_promise,
            )
        });
        let connection = biconnect_components::<UdpPort, _, _>(&probe, &component)
            .expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let _request_id = match recv_until(&requests_rx, |request| {
            matches!(request, UdpRequest::Bind { .. })
        }) {
            UdpRequest::Bind { request_id, .. } => request_id,
            other => unreachable!("filtered to bind request, got {other:?}"),
        };

        drop(connection);
        kill_component(&system, component);

        let startup_result = startup_future
            .wait_timeout(Duration::from_millis(100))
            .expect("startup result should complete");
        assert_eq!(
            startup_result,
            Err(PeerAnnouncementStartupError::Interrupted)
        );

        assert!(
            requests_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "stopping during Opening should not emit extra UDP requests"
        );

        kill_component(&system, probe);
        system.shutdown().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_reports_startup_success_after_broadcast_is_enabled() {
        init_test_logger();

        let system = build_test_kompact_system();
        let (requests_tx, requests_rx) = mpsc::channel();
        let probe = system.create(move || UdpRequestProbe::new(requests_tx));
        let (startup_promise, startup_future) = peer_announcement_startup_signal();
        let component = system.create(move || {
            PeerAnnouncementComponent::with_options_and_startup_promise(
                Options::default(),
                startup_promise,
            )
        });
        let connection = biconnect_components::<UdpPort, _, _>(&probe, &component)
            .expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let request_id = match recv_until(&requests_rx, |request| {
            matches!(request, UdpRequest::Bind { .. })
        }) {
            UdpRequest::Bind { request_id, .. } => request_id,
            other => unreachable!("filtered to bind request, got {other:?}"),
        };

        probe.on_definition(|probe| {
            probe.udp.trigger(UdpIndication::Bound {
                request_id,
                socket_id: SocketId(31),
                local_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 41001)),
            });
        });

        match recv_until(&requests_rx, |request| {
            matches!(request, UdpRequest::Configure { .. })
        }) {
            UdpRequest::Configure { socket_id, option } => {
                assert_eq!(socket_id, SocketId(31));
                assert_eq!(option, UdpSocketOption::Broadcast(true));
            }
            other => unreachable!("filtered to configure request, got {other:?}"),
        }

        probe.on_definition(|probe| {
            probe.udp.trigger(UdpIndication::Configured {
                socket_id: SocketId(31),
                option: UdpSocketOption::Broadcast(true),
            });
        });

        let startup_result = startup_future
            .wait_timeout(Duration::from_millis(100))
            .expect("startup result should complete");
        assert_eq!(startup_result, Ok(()));

        drop(connection);
        kill_component(&system, component);
        kill_component(&system, probe);
        system.shutdown().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_reports_bind_failure_to_startup_waiter() {
        init_test_logger();

        let system = build_test_kompact_system();
        let (requests_tx, requests_rx) = mpsc::channel();
        let probe = system.create(move || UdpRequestProbe::new(requests_tx));
        let (startup_promise, startup_future) = peer_announcement_startup_signal();
        let component = system.create(move || {
            PeerAnnouncementComponent::with_options_and_startup_promise(
                Options::default(),
                startup_promise,
            )
        });
        let connection = biconnect_components::<UdpPort, _, _>(&probe, &component)
            .expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let request_id = match recv_until(&requests_rx, |request| {
            matches!(request, UdpRequest::Bind { .. })
        }) {
            UdpRequest::Bind { request_id, .. } => request_id,
            other => unreachable!("filtered to bind request, got {other:?}"),
        };

        let failed_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 41002));
        probe.on_definition(|probe| {
            probe.udp.trigger(UdpIndication::BindFailed {
                request_id,
                local_addr: failed_addr,
                reason: OpenFailureReason::Io(std::io::ErrorKind::AddrInUse),
            });
        });

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

        drop(connection);
        kill_component(&system, probe);
        system.shutdown().expect("Kompact shutdown");
    }

    #[test]
    fn peer_announcement_component_reports_configure_failure_to_startup_waiter() {
        init_test_logger();

        let system = build_test_kompact_system();
        let (requests_tx, requests_rx) = mpsc::channel();
        let probe = system.create(move || UdpRequestProbe::new(requests_tx));
        let (startup_promise, startup_future) = peer_announcement_startup_signal();
        let component = system.create(move || {
            PeerAnnouncementComponent::with_options_and_startup_promise(
                Options::default(),
                startup_promise,
            )
        });
        let connection = biconnect_components::<UdpPort, _, _>(&probe, &component)
            .expect("connect probe/component");

        start_component(&system, &probe);
        start_component(&system, &component);

        let request_id = match recv_until(&requests_rx, |request| {
            matches!(request, UdpRequest::Bind { .. })
        }) {
            UdpRequest::Bind { request_id, .. } => request_id,
            other => unreachable!("filtered to bind request, got {other:?}"),
        };

        probe.on_definition(|probe| {
            probe.udp.trigger(UdpIndication::Bound {
                request_id,
                socket_id: SocketId(32),
                local_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 41003)),
            });
        });
        let _ = recv_until(&requests_rx, |request| {
            matches!(request, UdpRequest::Configure { .. })
        });

        probe.on_definition(|probe| {
            probe.udp.trigger(UdpIndication::ConfigureFailed {
                socket_id: SocketId(32),
                option: UdpSocketOption::Broadcast(true),
                reason: ConfigureFailureReason::Io(std::io::ErrorKind::PermissionDenied),
            });
        });

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

        drop(connection);
        kill_component(&system, probe);
        system.shutdown().expect("Kompact shutdown");
    }
}
