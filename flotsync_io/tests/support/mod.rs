#![allow(dead_code)]

use bytes::Bytes;
use flotsync_io::prelude::*;
use kompact::prelude::*;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, OnceLock, mpsc},
    thread,
    time::{Duration, Instant},
};

pub const WAIT_TIMEOUT: Duration = Duration::from_secs(2);

pub fn init_test_logger() {
    static LOGGER: OnceLock<()> = OnceLock::new();
    LOGGER.get_or_init(|| {
        let _ = simple_logger::SimpleLogger::new()
            .with_level(log::LevelFilter::Trace)
            .init();
    });
}

pub fn localhost(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
}

pub fn payload_bytes(payload: IoPayload) -> Bytes {
    match payload {
        IoPayload::Lease(lease) => lease.create_byte_clone(),
        IoPayload::Bytes(bytes) => bytes,
        _ => panic!("unexpected future IoPayload variant in integration test"),
    }
}

pub fn wait_for_driver_request<T>(mut request: DriverRequest<T>) -> T {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        match request.try_receive() {
            Ok(Some(reply)) => return reply,
            Ok(None) => {}
            Err(error) => panic!("driver request failed: {error}"),
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for driver request reply");
        }
        thread::sleep(Duration::from_millis(1));
    }
}

pub fn wait_for_driver_event(
    driver: &IoDriver,
    mut predicate: impl FnMut(&DriverEvent) -> bool,
) -> DriverEvent {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        match driver.try_next_event() {
            Ok(Some(event)) if predicate(&event) => return event,
            Ok(Some(other)) => {
                log::debug!(
                    "ignoring unrelated driver event while waiting in integration test: {:?}",
                    other
                );
            }
            Ok(None) => {}
            Err(error) => panic!("driver event retrieval failed: {error}"),
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for driver event");
        }
        thread::sleep(Duration::from_millis(1));
    }
}

pub fn assert_no_driver_event(driver: &IoDriver, duration: Duration) {
    let deadline = Instant::now() + duration;
    loop {
        match driver.try_next_event() {
            Ok(Some(event)) => panic!("unexpected driver event while expecting silence: {event:?}"),
            Ok(None) => {}
            Err(error) => panic!("driver event retrieval failed: {error}"),
        }
        if Instant::now() >= deadline {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

pub fn recv_until<T>(rx: &mpsc::Receiver<T>, mut predicate: impl FnMut(&T) -> bool) -> T {
    loop {
        let value = rx
            .recv_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for integration-test event");
        if predicate(&value) {
            return value;
        }
    }
}

pub fn start_component<C>(system: &KompactSystem, component: &Arc<Component<C>>)
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    system
        .start_notify(component)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("component start");
}

pub fn kill_component<C>(system: &KompactSystem, component: Arc<Component<C>>)
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    system
        .kill_notify(component)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("component kill");
}

#[derive(ComponentDefinition)]
pub struct UdpObserver {
    ctx: ComponentContext<Self>,
    pub udp: RequiredPort<UdpPort>,
    indications: mpsc::Sender<UdpIndication>,
}

impl UdpObserver {
    pub fn new(indications: mpsc::Sender<UdpIndication>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: RequiredPort::uninitialised(),
            indications,
        }
    }
}

ignore_lifecycle!(UdpObserver);

impl Require<UdpPort> for UdpObserver {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        self.indications
            .send(indication)
            .expect("UDP indication receiver must stay live during integration tests");
        Handled::Ok
    }
}

impl Actor for UdpObserver {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("Never type is empty")
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("UDP observer test component does not use network actor messages")
    }
}

#[derive(ComponentDefinition)]
pub struct UdpSendResultProbe {
    ctx: ComponentContext<Self>,
    results: mpsc::Sender<UdpSendResult>,
}

impl UdpSendResultProbe {
    pub fn new(results: mpsc::Sender<UdpSendResult>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            results,
        }
    }
}

ignore_lifecycle!(UdpSendResultProbe);

impl Actor for UdpSendResultProbe {
    type Message = UdpSendResult;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.results
            .send(msg)
            .expect("UDP send result receiver must stay live during integration tests");
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("UDP send result probe does not use network actor messages")
    }
}

#[derive(ComponentDefinition)]
pub struct TcpSessionEventProbe {
    ctx: ComponentContext<Self>,
    events: mpsc::Sender<TcpSessionEvent>,
}

impl TcpSessionEventProbe {
    pub fn new(events: mpsc::Sender<TcpSessionEvent>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            events,
        }
    }
}

ignore_lifecycle!(TcpSessionEventProbe);

impl Actor for TcpSessionEventProbe {
    type Message = TcpSessionEvent;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.events
            .send(msg)
            .expect("TCP session event receiver must stay live during integration tests");
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("TCP session probe does not use network actor messages")
    }
}

#[derive(ComponentDefinition)]
pub struct TcpListenerEventProbe {
    ctx: ComponentContext<Self>,
    events: mpsc::Sender<TcpListenerEvent>,
}

impl TcpListenerEventProbe {
    pub fn new(events: mpsc::Sender<TcpListenerEvent>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            events,
        }
    }
}

ignore_lifecycle!(TcpListenerEventProbe);

impl Actor for TcpListenerEventProbe {
    type Message = TcpListenerEvent;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.events
            .send(msg)
            .expect("TCP listener event receiver must stay live during integration tests");
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("TCP listener probe does not use network actor messages")
    }
}
