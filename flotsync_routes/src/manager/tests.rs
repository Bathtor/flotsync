//! Route-transport manager tests.

use super::*;
use crate::{
    RoutePreferenceRank,
    RouteSharingKind,
    SendRouteCandidate,
    test_support::{BoundReservedUdpSocket, TransportHarnessCore},
};
use bytes::Bytes;
use flotsync_io::{
    pool::PayloadWriter,
    prelude::{MAX_UDP_PAYLOAD_BYTES, SocketId, UdpIndication, UdpLocalBind},
    test_support::{
        ReservedSocketKind,
        WAIT_TIMEOUT,
        build_test_kompact_system_with,
        enable_bind_reuse_address,
        eventually_component_state,
        eventually_value,
        localhost,
        set_test_system_label,
        start_component,
    },
};
use flotsync_messages::serialisation::{FlotsyncSerializable, FlotsyncSerializeError, SizeHint};
use flotsync_udpour::{MessageId, ReceiverConfig, SenderConfig, config_keys as udpour_config_keys};
use flotsync_utils::{
    BoxFuture,
    kompact_testing::{PortTestingExt, PortTestingRefExt},
};
use futures_util::FutureExt;
use rand::{RngCore, SeedableRng, rngs::StdRng};
use std::{
    cell::Cell,
    fmt::{self, Display},
    net::SocketAddr,
    num::NonZeroUsize,
    time::Duration,
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
struct TestSendRateControl {
    send_delay: Duration,
    backpressure_retry_delay: Duration,
    max_in_flight_datagrams: usize,
}

impl Default for TestSendRateControl {
    fn default() -> Self {
        Self {
            send_delay: udpour_config_keys::SEND_DELAY
                .default()
                .expect("UDPour send-delay default must exist"),
            backpressure_retry_delay: udpour_config_keys::BACKPRESSURE_RETRY_DELAY
                .default()
                .expect("UDPour backpressure-retry-delay default must exist"),
            max_in_flight_datagrams: udpour_config_keys::MAX_IN_FLIGHT_DATAGRAMS
                .default()
                .expect("UDPour max-in-flight-datagrams default must exist"),
        }
    }
}

struct BytesPayload(Vec<u8>);

impl FlotsyncSerializable for BytesPayload {
    fn serialized_size_hint(&self) -> SizeHint {
        SizeHint::Exact(self.0.len())
    }

    fn serialize_into<'a>(
        &'a self,
        writer: &'a mut flotsync_io::prelude::EgressAsyncWriter,
    ) -> BoxFuture<'a, Result<(), FlotsyncSerializeError>> {
        async move {
            writer
                .write_slice(&self.0)
                .await
                .map_err(|source| FlotsyncSerializeError::Io { source })?;
            Ok(())
        }
        .boxed()
    }
}

struct StartupBufferedDatagramsTimeout<'a> {
    socket_key: UdpSocketKey,
    min_count: usize,
    max_observed: &'a Cell<usize>,
}

impl Display for StartupBufferedDatagramsTimeout<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "timed out waiting for the manager to buffer {} startup datagrams for {:?}; max observed was {}",
            self.min_count,
            self.socket_key,
            self.max_observed.get()
        )
    }
}

struct UdpManagerHarness {
    core: TransportHarnessCore,
    manager: Arc<Component<RouteTransportManager>>,
    manager_ref: ActorRefStrong<TransportRouteTransportMessage>,
}

impl UdpManagerHarness {
    fn new() -> Self {
        Self::with_socket_budgets(
            0,
            0,
            UdpActivationPolicy::OnBind,
            TestSendRateControl::default(),
            udpour_config(),
        )
    }

    fn with_socket_budgets(
        extra_reserved_udp_sockets: usize,
        manager_owned_udp_sockets: usize,
        activation_policy: UdpActivationPolicy,
        send_rate_control: TestSendRateControl,
        udpour_config: UDPourConfig,
    ) -> Self {
        let extra_socket_kinds = vec![ReservedSocketKind::UdpSocket; extra_reserved_udp_sockets];
        Self::with_reserved_socket_kinds_and_manager_budget(
            false,
            &extra_socket_kinds,
            manager_owned_udp_sockets,
            activation_policy,
            send_rate_control,
            udpour_config,
        )
    }

    fn with_reserved_socket_kinds_and_manager_budget(
        reserve_external_udp_socket: bool,
        extra_socket_kinds: &[ReservedSocketKind],
        manager_owned_udp_sockets: usize,
        activation_policy: UdpActivationPolicy,
        send_rate_control: TestSendRateControl,
        udpour_config: UDPourConfig,
    ) -> Self {
        let system = build_manager_test_kompact_system(activation_policy, send_rate_control);
        let core = TransportHarnessCore::with_socket_budgets(
            system,
            udpour_config,
            reserve_external_udp_socket,
            extra_socket_kinds,
            manager_owned_udp_sockets,
        );
        let manager = Arc::clone(core.manager());
        let manager_ref = core.manager_ref();
        core.start();

        Self {
            core,
            manager,
            manager_ref,
        }
    }

    fn with_config(
        activation_policy: UdpActivationPolicy,
        send_rate_control: TestSendRateControl,
        udpour_config: UDPourConfig,
    ) -> Self {
        Self::with_socket_budgets(0, 0, activation_policy, send_rate_control, udpour_config)
    }

    fn with_external_socket(
        activation_policy: UdpActivationPolicy,
        send_rate_control: TestSendRateControl,
        udpour_config: UDPourConfig,
    ) -> Self {
        Self::with_reserved_socket_kinds_and_manager_budget(
            true,
            &[],
            0,
            activation_policy,
            send_rate_control,
            udpour_config,
        )
    }

    fn send(&self, send: TransportRouteTransportSend) -> TransportRouteTransportSubmitResult {
        self.send_async(send)
            .wait_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for route transport submit result")
    }

    fn send_async(
        &self,
        send: TransportRouteTransportSend,
    ) -> KFuture<TransportRouteTransportSubmitResult> {
        self.manager_ref
            .ask_with(|promise| RouteTransportActorMessage::Submit(Ask::new(promise, send)))
    }

    fn register_external_socket(&self, socket_id: SocketId, local_addr: SocketAddr) {
        let registration = ExternalUdpSocketRegistration {
            socket_id,
            local_addr,
        };
        let result = self
            .manager_ref
            .ask_with(|promise| {
                RouteTransportActorMessage::RegisterExternalUdpSocket(Ask::new(
                    promise,
                    registration,
                ))
            })
            .wait_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for external UDP socket registration");
        if let Err(error) = result {
            panic!("external UDP socket registration failed: {error}");
        }
    }

    fn publish_route_endpoint_available(&self, socket_id: SocketId, socket_bound_addr: SocketAddr) {
        let route_endpoint_lifecycle_port = self
            .manager
            .on_definition(RouteTransportManager::route_endpoint_lifecycle_port);
        self.core.system().trigger_i(
            RouteEndpointLifecycle::Available(RouteEndpointBinding {
                socket_id,
                socket_bound_addr,
            }),
            &route_endpoint_lifecycle_port,
        );
    }

    fn publish_route_endpoint_unavailable(
        &self,
        socket_id: SocketId,
        socket_bound_addr: SocketAddr,
    ) {
        let route_endpoint_lifecycle_port = self
            .manager
            .on_definition(RouteTransportManager::route_endpoint_lifecycle_port);
        self.core.system().trigger_i(
            RouteEndpointLifecycle::Unavailable {
                binding: RouteEndpointBinding {
                    socket_id,
                    socket_bound_addr,
                },
                reason: RouteEndpointUnavailableReason::Closed {
                    reason: UdpCloseReason::Requested,
                },
            },
            &route_endpoint_lifecycle_port,
        );
    }

    fn wait_for_send_ack(&self, send: TransportRouteTransportSend) -> TransportRouteKey {
        match self.send(send) {
            RouteTransportSubmitResult::Sent { coverage_key } => coverage_key,
            RouteTransportSubmitResult::SendFailed {
                coverage_key,
                reason,
            } => {
                panic!("unexpected SendFailed for {coverage_key:?}: {reason:?}")
            }
        }
    }

    fn wait_for_send_ack_future(
        future: KFuture<TransportRouteTransportSubmitResult>,
    ) -> TransportRouteKey {
        match future
            .wait_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for route transport send success")
        {
            RouteTransportSubmitResult::Sent { coverage_key } => coverage_key,
            RouteTransportSubmitResult::SendFailed {
                coverage_key,
                reason,
            } => {
                panic!("unexpected SendFailed for {coverage_key:?}: {reason:?}")
            }
        }
    }

    fn wait_for_send_nack(
        future: KFuture<TransportRouteTransportSubmitResult>,
    ) -> (TransportRouteKey, RouteTransportNackReason) {
        match future
            .wait_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for route transport send failure")
        {
            RouteTransportSubmitResult::SendFailed {
                coverage_key,
                reason,
            } => (coverage_key, reason),
            RouteTransportSubmitResult::Sent { coverage_key } => {
                panic!("unexpected Sent result for {coverage_key:?}")
            }
        }
    }

    fn live_udp_socket_count(&self) -> usize {
        self.manager
            .on_definition(|component| component.udp_sockets.len())
    }

    fn dormant_udp_socket_count(&self) -> usize {
        self.manager
            .on_definition(|component| component.udp_dormant_sockets.len())
    }

    fn wait_for_dormant_socket(&self, socket_key: UdpSocketKey) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.manager,
            |component| component.udp_dormant_sockets.contains_key(&socket_key),
            format_args!("timed out waiting for dormant UDP socket {socket_key:?}"),
        );
    }

    fn wait_for_startup_buffered_datagrams(
        &self,
        socket_key: UdpSocketKey,
        min_count: usize,
    ) -> usize {
        // `eventually_value` owns both the polling closure and the deferred
        // failure message. `Cell` lets the closure update the best
        // diagnostic sample while the display helper still reads the final
        // value if the wait times out.
        let max_observed = Cell::new(0usize);
        eventually_value(
            WAIT_TIMEOUT,
            || {
                let (buffered_count, live) = self.manager.on_definition(|component| {
                    (
                        component
                            .udp_starting_sockets
                            .get(&socket_key)
                            .map(|handle| handle.buffered_datagram_count)
                            .or_else(|| {
                                component
                                    .udp_sockets
                                    .get(&socket_key)
                                    .map(|handle| handle.startup_buffered_datagram_count)
                            }),
                        component.udp_sockets.contains_key(&socket_key),
                    )
                });
                if let Some(buffered_count) = buffered_count {
                    max_observed.set(max_observed.get().max(buffered_count));
                    if buffered_count >= min_count {
                        return Some(buffered_count);
                    }
                }
                assert!(
                    !(live && max_observed.get() == 0),
                    "UDPour child became live before any startup datagram was buffered for {socket_key:?}"
                );
                None
            },
            StartupBufferedDatagramsTimeout {
                socket_key,
                min_count,
                max_observed: &max_observed,
            },
        )
    }

    fn wait_for_live_udp_socket(&self, socket_key: UdpSocketKey) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.manager,
            |component| component.udp_sockets.contains_key(&socket_key),
            format_args!("timed out waiting for live UDP socket {socket_key:?}"),
        );
    }

    fn bind_external_socket(&self, bind: UdpLocalBind) -> (SocketId, SocketAddr) {
        self.core.bind_external_socket(bind, WAIT_TIMEOUT)
    }

    fn bind_reserved_udp_socket(&self, reservation_index: usize) -> BoundReservedUdpSocket {
        self.core.bind_reserved_udp_socket(reservation_index)
    }

    fn wait_for_new_bound_socket(&self) -> (SocketId, SocketAddr) {
        self.core.wait_for_new_bound_socket(WAIT_TIMEOUT)
    }

    fn wait_for_bridge_frame_type(&self, socket_id: SocketId, frame_type: u8) {
        let _ = self
                .core
                .observer_rx()
                .recv_matching_or_fail(
                    WAIT_TIMEOUT,
                    |indication| {
                        let UdpIndication::Received {
                            socket_id: indicated_socket_id,
                            payload,
                            ..
                        } = indication
                        else {
                            return false;
                        };
                        *indicated_socket_id == socket_id
                            && payload.to_vec().first().copied() == Some(frame_type)
                    },
                    |indication| match indication {
                        UdpIndication::BindFailed {
                            request_id,
                            local_addr,
                            reason,
                        } => Some(format!(
                            "route-transport UDP bind request {request_id:?} failed at {local_addr} while waiting for frame type 0x{frame_type:02x}: {reason:?}"
                        )),
                        UdpIndication::Closed {
                            socket_id: indicated_socket_id,
                            remote_addr,
                            reason,
                        } if *indicated_socket_id == socket_id => Some(format!(
                            "UDP socket {socket_id:?} closed while waiting for frame type 0x{frame_type:02x}: remote={remote_addr:?}, reason={reason:?}"
                        )),
                        _ => None,
                    },
                );
    }

    fn wait_for_bridge_payload_frames(&self, socket_id: SocketId, min_count: usize) {
        for _ in 0..min_count {
            let _ = self
                    .core
                    .observer_rx()
                    .recv_matching_or_fail(
                        WAIT_TIMEOUT,
                        |indication| {
                            let UdpIndication::Received {
                                socket_id: indicated_socket_id,
                                payload,
                                ..
                            } = indication
                            else {
                                return false;
                            };
                            *indicated_socket_id == socket_id
                                && payload.to_vec().first().copied() == Some(0x01)
                        },
                        |indication| match indication {
                            UdpIndication::BindFailed {
                                request_id,
                                local_addr,
                                reason,
                            } => Some(format!(
                                "route-transport UDP bind request {request_id:?} failed at {local_addr} while waiting for payload frames on {socket_id:?}: {reason:?}"
                            )),
                            UdpIndication::Closed {
                                socket_id: indicated_socket_id,
                                remote_addr,
                                reason,
                            } if *indicated_socket_id == socket_id => Some(format!(
                                "UDP socket {socket_id:?} closed while waiting for payload frames: remote={remote_addr:?}, reason={reason:?}"
                            )),
                            _ => None,
                        },
                    );
        }
    }
}

#[test]
fn udp_manager_sends_payload_over_real_socket() {
    let harness = UdpManagerHarness::with_socket_budgets(
        1,
        1,
        UdpActivationPolicy::OnBind,
        TestSendRateControl::default(),
        udpour_config(),
    );
    let receiver = harness.bind_reserved_udp_socket(0);
    receiver
        .set_read_timeout(Some(WAIT_TIMEOUT))
        .expect("set UDP read timeout");
    let receiver_addr = receiver.local_addr().expect("receiver local addr");

    let route = UdpRouteKey {
        remote_addr: receiver_addr,
        scope: DatagramRouteScope::Unicast,
        local_bind: None,
    };
    let send_id = RouteSendId(Uuid::new_v4());
    let coverage_key = harness.wait_for_send_ack(route_send(
        send_id,
        route,
        b"hello route transport".to_vec(),
    ));
    assert_eq!(coverage_key, TransportRouteKey::Udp(route));
    assert_eq!(harness.live_udp_socket_count(), 1);

    let mut buffer = [0u8; 2048];
    let (len, _source) = receiver
        .recv_from(&mut buffer)
        .expect("receive UDP datagram from managed sender");
    assert!(buffer[..len].ends_with(b"hello route transport"));
}

#[test]
fn udp_manager_delivers_oversized_payload_over_real_udpour_socket() {
    let receiver_harness = UdpManagerHarness::with_external_socket(
        UdpActivationPolicy::OnBind,
        TestSendRateControl::default(),
        udpour_config_with_part_size(256),
    );
    let (receiver_socket_id, receiver_addr) =
        receiver_harness.bind_external_socket(UdpLocalBind::Exact(localhost(0)));
    let receiver_socket_key = UdpSocketKey {
        local_addr: receiver_addr,
    };
    receiver_harness.publish_route_endpoint_available(receiver_socket_id, receiver_addr);
    receiver_harness.wait_for_live_udp_socket(receiver_socket_key);

    let inbound_probe = receiver_harness
        .core
        .system()
        .create(TransportRouteTransportPort::tester_component_sidecar);
    let inbound_probe_ref = inbound_probe.actor_ref();
    biconnect_components::<TransportRouteTransportPort, _, _>(
        &receiver_harness.manager,
        &inbound_probe,
    )
    .expect("connect receiver route transport probe");
    start_component(receiver_harness.core.system(), &inbound_probe);

    let sender_harness = UdpManagerHarness::with_socket_budgets(
        0,
        1,
        UdpActivationPolicy::OnBind,
        TestSendRateControl::default(),
        udpour_config_with_part_size(256),
    );
    let oversized_payload = deterministic_test_payload(MAX_UDP_PAYLOAD_BYTES + 513);
    let oversized_payload_len = oversized_payload.len();
    let route = UdpRouteKey {
        remote_addr: receiver_addr,
        scope: DatagramRouteScope::Unicast,
        local_bind: None,
    };
    let send_id = RouteSendId(Uuid::new_v4());
    let submit = sender_harness.send_async(route_send(send_id, route, oversized_payload.clone()));
    let (_sender_socket_id, sender_addr) = sender_harness.wait_for_new_bound_socket();
    let delivery_future = inbound_probe_ref.observe_indication(move |deliver| {
        deliver.payload.len() == oversized_payload_len
            && deliver.transport.remote_addr == Some(sender_addr)
    });

    assert_eq!(
        UdpManagerHarness::wait_for_send_ack_future(submit),
        TransportRouteKey::Udp(route)
    );
    let observed = delivery_future
        .wait_timeout(WAIT_TIMEOUT)
        .expect("timed out waiting for oversized route-transport delivery")
        .expect("route transport delivery probe should stay live");
    let delivery = observed.indication();
    assert_eq!(delivery.payload.to_vec(), oversized_payload);
    assert_eq!(delivery.transport.remote_addr, Some(sender_addr));
    assert_eq!(
        delivery.transport.route,
        TransportRouteKey::Udp(UdpRouteKey {
            remote_addr: sender_addr,
            scope: DatagramRouteScope::Unicast,
            local_bind: Some(receiver_addr),
        })
    );
}

#[test]
fn udp_manager_reuses_one_socket_for_two_loopback_targets() {
    let harness = UdpManagerHarness::with_socket_budgets(
        2,
        1,
        UdpActivationPolicy::OnBind,
        TestSendRateControl::default(),
        udpour_config(),
    );
    let receiver1 = harness.bind_reserved_udp_socket(0);
    let receiver2 = harness.bind_reserved_udp_socket(1);
    receiver1
        .set_read_timeout(Some(WAIT_TIMEOUT))
        .expect("set first UDP read timeout");
    receiver2
        .set_read_timeout(Some(WAIT_TIMEOUT))
        .expect("set second UDP read timeout");

    let route1 = UdpRouteKey {
        remote_addr: receiver1.local_addr().expect("first receiver addr"),
        scope: DatagramRouteScope::Unicast,
        local_bind: None,
    };
    let route2 = UdpRouteKey {
        remote_addr: receiver2.local_addr().expect("second receiver addr"),
        scope: DatagramRouteScope::Unicast,
        local_bind: None,
    };
    let send_id1 = RouteSendId(Uuid::new_v4());
    let send_id2 = RouteSendId(Uuid::new_v4());

    let submit1 = harness.send_async(route_send(send_id1, route1, b"first target".to_vec()));
    let submit2 = harness.send_async(route_send(send_id2, route2, b"second target".to_vec()));

    assert_eq!(
        UdpManagerHarness::wait_for_send_ack_future(submit1),
        TransportRouteKey::Udp(route1)
    );
    assert_eq!(
        UdpManagerHarness::wait_for_send_ack_future(submit2),
        TransportRouteKey::Udp(route2)
    );
    assert_eq!(harness.live_udp_socket_count(), 1);

    let mut buffer1 = [0u8; 2048];
    let mut buffer2 = [0u8; 2048];
    let (len1, source1) = receiver1
        .recv_from(&mut buffer1)
        .expect("receive first UDP datagram from managed sender");
    let (len2, source2) = receiver2
        .recv_from(&mut buffer2)
        .expect("receive second UDP datagram from managed sender");

    assert!(buffer1[..len1].ends_with(b"first target"));
    assert!(buffer2[..len2].ends_with(b"second target"));
    assert_eq!(source1, source2);
}

#[test]
fn udp_manager_buffers_inbound_datagrams_while_socket_is_starting() {
    let harness = UdpManagerHarness::new();
    let socket_id = SocketId(7);
    let socket_key = UdpSocketKey {
        local_addr: "127.0.0.1:34567".parse().expect("socket key addr"),
    };
    let source: SocketAddr = "127.0.0.1:45678".parse().expect("source addr");

    harness.manager.on_definition(|component| {
        let runtime = component.ctx.system().create({
            let egress_pool = component.bridge.egress_pool().clone();
            let config = component.udpour_config.clone();
            move || UDPourComponent::new(socket_id, egress_pool, config)
        });
        let runtime_ref = runtime
            .actor_ref()
            .hold()
            .expect("child UDPour runtime must still be live after creation");
        let udp_port = runtime.required_ref();
        let transfer_channel = runtime.connect_to_required(component.required_ref());
        component.udp_socket_ids.insert(socket_id, socket_key);
        component.udp_starting_sockets.insert(
            socket_key,
            StartingUdpSocketHandle {
                socket_id,
                runtime,
                runtime_ref,
                udp_port,
                transfer_channel,
                origin: UdpSocketStartOrigin::ManagerOwned,
                queued_sends: Vec::new(),
                buffered_datagram_count: 0,
            },
        );
        component.handle_udp_received(socket_id, source, zero_length_udpour_payload(MessageId(77)));

        let buffered_datagram_count = component
            .udp_starting_sockets
            .get(&socket_key)
            .expect("socket should still be starting")
            .buffered_datagram_count;
        assert_eq!(buffered_datagram_count, 1);
    });
}

#[test]
fn udp_manager_starts_dormant_socket_on_first_inbound_udpour_message() {
    let receiver_harness = UdpManagerHarness::with_external_socket(
        UdpActivationPolicy::OnFirstUse,
        TestSendRateControl::default(),
        udpour_config_with_part_size(64),
    );
    let (receiver_socket_id, receiver_addr) =
        receiver_harness.bind_external_socket(UdpLocalBind::Exact(localhost(0)));
    let receiver_socket_key = UdpSocketKey {
        local_addr: receiver_addr,
    };
    receiver_harness.register_external_socket(receiver_socket_id, receiver_addr);
    receiver_harness.wait_for_dormant_socket(receiver_socket_key);
    assert_eq!(receiver_harness.dormant_udp_socket_count(), 1);
    assert_eq!(receiver_harness.live_udp_socket_count(), 0);

    let sender_harness = UdpManagerHarness::with_socket_budgets(
        0,
        1,
        UdpActivationPolicy::OnBind,
        TestSendRateControl {
            send_delay: Duration::from_millis(10),
            backpressure_retry_delay: Duration::from_millis(10),
            max_in_flight_datagrams: 1,
        },
        udpour_config_with_part_size(64),
    );
    let route = UdpRouteKey {
        remote_addr: receiver_addr,
        scope: DatagramRouteScope::Unicast,
        local_bind: None,
    };
    let send_id = RouteSendId(Uuid::new_v4());
    let multipart_payload = vec![0x5a; 64 * 16];

    let submit = sender_harness.send_async(route_send(send_id, route, multipart_payload));

    let (sender_socket_id, _sender_addr) = sender_harness.wait_for_new_bound_socket();
    let buffered_count =
        receiver_harness.wait_for_startup_buffered_datagrams(receiver_socket_key, 1);
    receiver_harness.wait_for_live_udp_socket(receiver_socket_key);
    receiver_harness.wait_for_bridge_payload_frames(receiver_socket_id, buffered_count + 3);

    assert_eq!(
        UdpManagerHarness::wait_for_send_ack_future(submit),
        TransportRouteKey::Udp(route)
    );
    sender_harness.wait_for_bridge_frame_type(sender_socket_id, 0x81);
}

#[test]
fn udp_manager_does_not_start_udpour_for_unregistered_external_udp_socket() {
    let harness = UdpManagerHarness::with_config(
        UdpActivationPolicy::OnFirstUse,
        TestSendRateControl::default(),
        udpour_config(),
    );
    let socket_id = SocketId(77);
    let local_addr = localhost(34568);
    let source_addr = localhost(45689);
    let socket_key = UdpSocketKey { local_addr };

    harness.manager.on_definition(|component| {
        component.handle_udp_bound(UdpOpenRequestId::new(), socket_id, local_addr);
        component.handle_udp_received(
            socket_id,
            source_addr,
            IoPayload::from(Bytes::from_static(b"peer announcement traffic")),
        );

        assert!(!component.udp_socket_ids.contains_key(&socket_id));
        assert!(!component.udp_dormant_sockets.contains_key(&socket_key));
        assert!(!component.udp_starting_sockets.contains_key(&socket_key));
        assert!(!component.udp_sockets.contains_key(&socket_key));
    });
}

#[test]
fn udp_manager_registers_lifecycle_authorised_route_endpoint() {
    let harness = UdpManagerHarness::with_config(
        UdpActivationPolicy::OnFirstUse,
        TestSendRateControl::default(),
        udpour_config(),
    );
    let lifecycle_probe = harness
        .core
        .system()
        .create(RouteEndpointLifecyclePort::tester_component_sidecar);
    let lifecycle_probe_ref = lifecycle_probe.actor_ref();
    biconnect_components::<RouteEndpointLifecyclePort, _, _>(&harness.manager, &lifecycle_probe)
        .expect("connect accepted route endpoint lifecycle probe");
    start_component(harness.core.system(), &lifecycle_probe);

    let socket_id = SocketId(78);
    let local_addr = localhost(34569);
    let socket_key = UdpSocketKey { local_addr };
    let available_future = lifecycle_probe_ref.observe_indication(move |indication| {
        *indication
            == RouteEndpointLifecycle::Available(RouteEndpointBinding {
                socket_id,
                socket_bound_addr: local_addr,
            })
    });

    harness.publish_route_endpoint_available(socket_id, local_addr);

    eventually_component_state(
        WAIT_TIMEOUT,
        &harness.manager,
        |component| {
            component.udp_socket_ids.get(&socket_id) == Some(&socket_key)
                && component.udp_dormant_sockets.contains_key(&socket_key)
        },
        "route endpoint lifecycle should register a dormant UDP socket",
    );
    let available = available_future
        .wait_timeout(WAIT_TIMEOUT)
        .expect("accepted route endpoint lifecycle should publish availability")
        .expect("accepted lifecycle probe should stay live");

    let unavailable_future =
        lifecycle_probe_ref.observe_indication_from(available.index() + 1, move |indication| {
            *indication
                == RouteEndpointLifecycle::Unavailable {
                    binding: RouteEndpointBinding {
                        socket_id,
                        socket_bound_addr: local_addr,
                    },
                    reason: RouteEndpointUnavailableReason::Closed {
                        reason: UdpCloseReason::Requested,
                    },
                }
        });

    harness.publish_route_endpoint_unavailable(socket_id, local_addr);

    eventually_component_state(
        WAIT_TIMEOUT,
        &harness.manager,
        |component| {
            !component.udp_socket_ids.contains_key(&socket_id)
                && !component.udp_dormant_sockets.contains_key(&socket_key)
                && !component.udp_starting_sockets.contains_key(&socket_key)
                && !component.udp_sockets.contains_key(&socket_key)
        },
        "route endpoint lifecycle should remove the dormant UDP socket",
    );
    unavailable_future
        .wait_timeout(WAIT_TIMEOUT)
        .expect("accepted route endpoint lifecycle should publish unavailability")
        .expect("accepted lifecycle probe should stay live");
}

#[test]
fn udp_manager_restores_dormant_external_socket_after_activation_failure() {
    let harness = UdpManagerHarness::with_config(
        UdpActivationPolicy::OnFirstUse,
        TestSendRateControl::default(),
        udpour_config(),
    );
    let local_addr = localhost(34567);
    let socket_key = UdpSocketKey { local_addr };
    let socket_id = SocketId(77);
    let route = UdpRouteKey {
        remote_addr: localhost(45678),
        scope: DatagramRouteScope::Unicast,
        local_bind: Some(local_addr),
    };
    let send_id = RouteSendId(Uuid::new_v4());
    let (promise, submit) = promise::<TransportRouteTransportSubmitResult>();

    harness.manager.on_definition(|component| {
        component.pending_sends.insert(
            send_id,
            PendingRouteSend {
                send: route_send(send_id, route, b"retry me".to_vec()),
                submit_promise: Some(promise),
            },
        );
        component.udp_socket_ids.insert(socket_id, socket_key);
        component.handle_udp_socket_activation_failed(
            socket_key,
            socket_id,
            UdpSocketStartOrigin::ExternalDormant,
            vec![QueuedUdpSend { send_id, route }],
            &ConnectionFailureReason::TimedOut,
        );
    });

    harness.wait_for_dormant_socket(socket_key);
    assert_eq!(harness.dormant_udp_socket_count(), 1);
    let (coverage_key, reason) = UdpManagerHarness::wait_for_send_nack(submit);
    assert_eq!(coverage_key, TransportRouteKey::Udp(route));
    assert_eq!(reason, RouteTransportNackReason::RouteUnavailable);
}

fn route_send(
    send_id: RouteSendId,
    route: UdpRouteKey,
    bytes: Vec<u8>,
) -> TransportRouteTransportSend {
    RouteTransportSend {
        send_id,
        route: SendRouteCandidate {
            coverage_key: TransportRouteKey::Udp(route),
            sharing: RouteSharingKind::Exclusive,
            preference_rank: RoutePreferenceRank::UNRANKED,
        },
        payload: Arc::new(BytesPayload(bytes)),
    }
}

/// Build deterministic pseudo-random bytes so payload round-trips prove exact content.
fn deterministic_test_payload(len: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(0xa5a5_1f2d_3c4b_5968);
    let mut bytes = vec![0; len];
    rng.fill_bytes(&mut bytes);
    bytes
}

fn udpour_config() -> UDPourConfig {
    udpour_config_with_part_size(1024)
}

fn udpour_config_with_part_size(max_part_payload_len: usize) -> UDPourConfig {
    let sender = SenderConfig {
        max_part_payload_len: NonZeroUsize::new(max_part_payload_len)
            .expect("test UDPour config must use a non-zero part payload length"),
        retention_timeout: Duration::from_secs(1),
        id_reuse_cooldown: Duration::from_millis(100),
        eager_ack_cleanup: false,
    };
    let receiver = ReceiverConfig {
        max_need_parts_frame_len: 1024,
        repair_interval: Duration::from_millis(100),
        give_up_timeout: Duration::from_secs(1),
        delivered_tombstone_timeout: Duration::ZERO,
    };
    UDPourConfig::new(sender, receiver).expect("valid datagram config")
}

fn build_manager_test_kompact_system(
    activation_policy: UdpActivationPolicy,
    send_rate_control: TestSendRateControl,
) -> KompactSystem {
    build_test_kompact_system_with(|config| {
        set_test_system_label(config, "route-transport-manager-test-system");
        enable_bind_reuse_address(config);
        let activation_policy_value = match activation_policy {
            UdpActivationPolicy::OnBind => 0usize,
            UdpActivationPolicy::OnFirstUse => 1usize,
        };
        config.set_config_value(&config_keys::UDP_ACTIVATION_POLICY, activation_policy_value);
        config.set_config_value(
            &udpour_config_keys::SEND_DELAY,
            send_rate_control.send_delay,
        );
        config.set_config_value(
            &udpour_config_keys::BACKPRESSURE_RETRY_DELAY,
            send_rate_control.backpressure_retry_delay,
        );
        config.set_config_value(
            &udpour_config_keys::MAX_IN_FLIGHT_DATAGRAMS,
            send_rate_control.max_in_flight_datagrams,
        );
    })
}

fn zero_length_udpour_payload(message_id: MessageId) -> IoPayload {
    let mut bytes = Vec::with_capacity(20);
    bytes.push(0x01);
    bytes.push(1);
    bytes.push(0);
    bytes.push(0);
    bytes.extend_from_slice(&message_id.0.to_be_bytes());
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    IoPayload::from(Bytes::from(bytes))
}
