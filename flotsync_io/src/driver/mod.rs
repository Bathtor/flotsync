//! Single-threaded mio driver core and control plane.

use crate::{
    api::{ConnectionId, ListenerId, SocketId, TcpCommand, TcpEvent, UdpCommand, UdpEvent},
    errors::{
        CreateDriverPollSnafu,
        CreateDriverWakerSnafu,
        DriverWakeSnafu,
        Error,
        Result,
        SpawnDriverThreadSnafu,
    },
    pool::{EgressPool, IngressPool, IoBufferConfig, IoBufferPools, PoolAvailabilityNotifier},
};
use mio::{Poll, Token, Waker};
use snafu::ResultExt;
use std::{
    borrow::Cow,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll as TaskPoll},
    thread::JoinHandle,
    time::Duration,
};

mod registry;
mod runtime;
mod tcp;
mod thread;
mod udp;

use runtime::ControlCommand;
#[cfg(test)]
use runtime::{CommandTrace, DriverSnapshot};
use thread::{panic_payload_to_string, run_driver_thread};

const WAKE_TOKEN: Token = Token(0);
const DEFAULT_EVENTS_CAPACITY: usize = 1024;
const DEFAULT_THREAD_NAME: &str = "flotsync-io-driver";

/// Driver-owned registration token space reserved for mio resources.
///
/// `Token(0)` is reserved for the driver's internal [`mio::Waker`]. Resource registrations start
/// at `Token(1)`.
pub type DriverToken = Token;

/// Configuration for the dedicated single-threaded mio driver runtime.
#[derive(Clone, Debug)]
pub struct DriverConfig {
    /// Human-readable thread name used for the dedicated driver thread.
    pub thread_name: Cow<'static, str>,
    /// Shared ingress and egress pool configuration owned by this driver instance.
    pub buffer_config: IoBufferConfig,
    /// Maximum number of readiness events buffered per `poll` call.
    pub events_capacity: usize,
    /// Optional timeout passed to `mio::Poll::poll`.
    ///
    /// `None` means the driver blocks until either a registered resource becomes ready or the
    /// control-plane waker fires.
    pub poll_timeout: Option<Duration>,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            thread_name: Cow::Borrowed(DEFAULT_THREAD_NAME),
            buffer_config: IoBufferConfig::default(),
            events_capacity: DEFAULT_EVENTS_CAPACITY,
            poll_timeout: None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct DriverThreadConfig {
    events_capacity: usize,
    poll_timeout: Option<Duration>,
}

impl From<&DriverConfig> for DriverThreadConfig {
    fn from(config: &DriverConfig) -> Self {
        Self {
            events_capacity: config.events_capacity,
            poll_timeout: config.poll_timeout,
        }
    }
}

/// Awaitable reply handle for control-plane requests that execute on the driver thread.
///
/// The request is already enqueued when this value is returned. Callers can either `await` it or
/// poll it opportunistically with [`DriverRequest::try_receive`] in synchronous contexts.
#[derive(Debug)]
pub struct DriverRequest<T> {
    receiver: futures_channel::oneshot::Receiver<Result<T>>,
}

impl<T> DriverRequest<T> {
    fn new(receiver: futures_channel::oneshot::Receiver<Result<T>>) -> Self {
        Self { receiver }
    }

    /// Attempts to retrieve the completed driver reply without blocking.
    ///
    /// `Ok(None)` means the driver has not produced a reply yet.
    pub fn try_receive(&mut self) -> Result<Option<T>> {
        match self.receiver.try_recv() {
            Ok(Some(reply)) => reply.map(Some),
            Ok(None) => Ok(None),
            Err(_) => Err(Error::DriverResponseChannelClosed),
        }
    }
}

impl<T> Future for DriverRequest<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> TaskPoll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            TaskPoll::Ready(Ok(reply)) => TaskPoll::Ready(reply),
            TaskPoll::Ready(Err(_)) => TaskPoll::Ready(Err(Error::DriverResponseChannelClosed)),
            TaskPoll::Pending => TaskPoll::Pending,
        }
    }
}

/// Shared handle for the dedicated mio driver thread.
pub struct IoDriver {
    config: DriverConfig,
    buffers: IoBufferPools,
    command_tx: crossbeam_channel::Sender<ControlCommand>,
    event_rx: crossbeam_channel::Receiver<DriverEvent>,
    waker: Arc<Waker>,
    handle: Option<JoinHandle<Result<()>>>,
}

impl fmt::Debug for IoDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoDriver")
            .field("config", &self.config)
            .field("running", &self.handle.is_some())
            .finish()
    }
}

impl IoDriver {
    /// Starts the dedicated mio driver thread and waits until the runtime is ready.
    pub fn start(config: DriverConfig) -> Result<Self> {
        log::info!(
            "starting flotsync_io driver thread '{}'",
            config.thread_name
        );

        let poll = Poll::new().context(CreateDriverPollSnafu)?;
        let waker = Waker::new(poll.registry(), WAKE_TOKEN).context(CreateDriverWakerSnafu)?;
        let shared_waker = Arc::new(waker);
        let runtime_waker = Arc::clone(&shared_waker);
        let notifier_waker = Arc::clone(&shared_waker);
        let buffers = IoBufferPools::new_with_ingress_notifier(
            config.buffer_config.clone(),
            PoolAvailabilityNotifier::new(move || {
                let wake_result = notifier_waker.wake();
                if let Err(error) = wake_result {
                    log::error!(
                        "failed to wake flotsync_io driver thread after ingress capacity returned: {}",
                        error
                    );
                }
            }),
        )?;
        let runner_ingress_pool = buffers.ingress();

        let (command_tx, command_rx) = crossbeam_channel::unbounded();
        let (event_tx, event_rx) = crossbeam_channel::unbounded();
        let (startup_tx, startup_rx) = std::sync::mpsc::sync_channel(1);

        let thread_config = DriverThreadConfig::from(&config);
        let builder = std::thread::Builder::new().name(config.thread_name.to_string());
        let handle = builder
            .spawn(move || {
                run_driver_thread(
                    thread_config,
                    poll,
                    command_rx,
                    event_tx,
                    startup_tx,
                    runner_ingress_pool,
                )
            })
            .context(SpawnDriverThreadSnafu)?;

        let startup = startup_rx
            .recv()
            .map_err(|_| Error::DriverResponseChannelClosed)?;
        startup?;

        log::info!("flotsync_io driver thread '{}' started", config.thread_name);

        Ok(Self {
            config,
            buffers,
            command_tx,
            event_rx,
            waker: runtime_waker,
            handle: Some(handle),
        })
    }

    /// Returns the runtime configuration used by this driver instance.
    pub fn config(&self) -> &DriverConfig {
        &self.config
    }

    /// Returns the shared ingress and egress pool handles owned by this driver instance.
    pub fn buffers(&self) -> &IoBufferPools {
        &self.buffers
    }

    /// Returns the shared ingress pool handle owned by this driver instance.
    pub fn ingress_pool(&self) -> IngressPool {
        self.buffers.ingress()
    }

    /// Returns the shared egress pool handle owned by this driver instance.
    pub fn egress_pool(&self) -> EgressPool {
        self.buffers.egress()
    }

    /// Queues a listener-handle reservation on the driver thread.
    ///
    /// Reserving a handle performs only local driver bookkeeping. The driver allocates a
    /// [`ListenerId`] in the listener handle table and a unique shared `mio::Token` in the
    /// readiness table, but it does not yet bind or register a socket. The returned request
    /// resolves once that bookkeeping is installed on the driver thread.
    pub fn reserve_listener(&self) -> Result<DriverRequest<ListenerId>> {
        self.request(|reply_tx| ControlCommand::ReserveListener { reply_tx })
    }

    /// Queues a connection-handle reservation on the driver thread.
    ///
    /// Reserving a handle performs only local driver bookkeeping. The driver allocates a
    /// [`ConnectionId`] in the connection handle table and a unique shared `mio::Token` in the
    /// readiness table, but it does not yet create or register a socket. The returned request
    /// resolves once that bookkeeping is installed on the driver thread.
    pub fn reserve_connection(&self) -> Result<DriverRequest<ConnectionId>> {
        self.request(|reply_tx| ControlCommand::ReserveConnection { reply_tx })
    }

    /// Queues a UDP socket-handle reservation on the driver thread.
    ///
    /// Reserving a handle performs only local driver bookkeeping. The driver allocates a
    /// [`SocketId`] in the socket handle table and a unique shared `mio::Token` in the readiness
    /// table, but it does not yet bind or register a socket. The returned request resolves once
    /// that bookkeeping is installed on the driver thread.
    pub fn reserve_socket(&self) -> Result<DriverRequest<SocketId>> {
        self.request(|reply_tx| ControlCommand::ReserveSocket { reply_tx })
    }

    /// Queues release of a previously reserved listener handle.
    ///
    /// Once the request resolves, the driver has returned both the typed handle slot and the
    /// shared readiness token slot to their internal LIFO free lists.
    pub fn release_listener(&self, listener_id: ListenerId) -> Result<DriverRequest<()>> {
        self.request(|reply_tx| ControlCommand::ReleaseListener {
            listener_id,
            reply_tx,
        })
    }

    /// Queues release of a previously reserved connection handle.
    ///
    /// Once the request resolves, the driver has returned both the typed handle slot and the
    /// shared readiness token slot to their internal LIFO free lists.
    pub fn release_connection(&self, connection_id: ConnectionId) -> Result<DriverRequest<()>> {
        self.request(|reply_tx| ControlCommand::ReleaseConnection {
            connection_id,
            reply_tx,
        })
    }

    /// Queues release of a previously reserved UDP socket handle.
    ///
    /// Once the request resolves, the driver has returned both the typed handle slot and the
    /// shared readiness token slot to their internal LIFO free lists.
    pub fn release_socket(&self, socket_id: SocketId) -> Result<DriverRequest<()>> {
        self.request(|reply_tx| ControlCommand::ReleaseSocket {
            socket_id,
            reply_tx,
        })
    }

    /// Enqueues a transport command for processing on the dedicated driver thread.
    pub fn dispatch(&self, command: DriverCommand) -> Result<()> {
        self.send_control(ControlCommand::Dispatch(command))
    }

    /// Attempts to retrieve the next emitted transport event without blocking.
    pub fn try_next_event(&self) -> Result<Option<DriverEvent>> {
        match self.event_rx.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Err(Error::DriverEventChannelClosed)
            }
        }
    }

    /// Stops the driver thread and waits for it to exit.
    pub fn shutdown(mut self) -> Result<()> {
        self.shutdown_inner()
    }

    fn send_control(&self, command: ControlCommand) -> Result<()> {
        let send_result = self.command_tx.send(command);
        if send_result.is_err() {
            log::error!(
                "flotsync_io driver thread '{}' command channel is closed",
                self.config.thread_name
            );
            return Err(Error::DriverCommandChannelClosed);
        }

        let wake_result = self.waker.wake().context(DriverWakeSnafu);
        if let Err(error) = &wake_result {
            log::error!(
                "failed to wake flotsync_io driver thread '{}': {}",
                self.config.thread_name,
                error
            );
        }
        wake_result
    }

    fn request<T>(
        &self,
        make_command: impl FnOnce(futures_channel::oneshot::Sender<Result<T>>) -> ControlCommand,
    ) -> Result<DriverRequest<T>> {
        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
        let command = make_command(reply_tx);
        self.send_control(command)?;
        Ok(DriverRequest::new(reply_rx))
    }

    fn shutdown_inner(&mut self) -> Result<()> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        log::info!(
            "shutting down flotsync_io driver thread '{}'",
            self.config.thread_name
        );

        let stop_result = self.send_control(ControlCommand::Stop);
        let join_result = handle.join();

        match join_result {
            Ok(Ok(())) => {
                stop_result?;
                log::info!(
                    "flotsync_io driver thread '{}' stopped cleanly",
                    self.config.thread_name
                );
                Ok(())
            }
            Ok(Err(error)) => {
                log::error!(
                    "flotsync_io driver thread '{}' exited with error: {}",
                    self.config.thread_name,
                    error
                );
                Err(error)
            }
            Err(payload) => {
                let message = panic_payload_to_string(payload);
                log::error!(
                    "flotsync_io driver thread '{}' panicked: {}",
                    self.config.thread_name,
                    message
                );
                Err(Error::DriverThreadPanicked)
            }
        }
    }

    #[cfg(test)]
    fn snapshot(&self) -> Result<DriverSnapshot> {
        let request = self.request(|reply_tx| ControlCommand::Snapshot { reply_tx })?;
        wait_for_request(request)
    }
}

impl Drop for IoDriver {
    fn drop(&mut self) {
        match self.shutdown_inner() {
            Ok(()) => {
                log::debug!(
                    "flotsync_io driver '{}' drop completed with a clean shutdown",
                    self.config.thread_name
                );
            }
            Err(error) => {
                log::error!(
                    "flotsync_io driver '{}' drop failed during shutdown: {}",
                    self.config.thread_name,
                    error
                );
            }
        }
    }
}

/// Commands entering the mio-backed driver core.
#[derive(Clone, Debug)]
pub enum DriverCommand {
    /// Forwards a TCP command from a bridge or client-facing adapter into the shared driver.
    Tcp(TcpCommand),
    /// Forwards a UDP command from a bridge or client-facing adapter into the shared driver.
    Udp(UdpCommand),
}

/// Events emitted by the mio-backed driver core.
#[derive(Clone, Debug)]
pub enum DriverEvent {
    /// Emits a TCP-facing event produced by the shared driver.
    Tcp(TcpEvent),
    /// Emits a UDP-facing event produced by the shared driver.
    Udp(UdpEvent),
}

#[cfg(test)]
pub(super) fn wait_for_request<T>(mut request: DriverRequest<T>) -> Result<T> {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);

    loop {
        if let Some(reply) = request.try_receive()? {
            return Ok(reply);
        }

        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for flotsync_io driver request reply");
        }

        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{api::IoPayload, test_support::init_test_logger};
    use bytes::Bytes;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn resolve_request<T>(request: Result<DriverRequest<T>>) -> T {
        let request = request.expect("enqueue driver request");
        wait_for_request(request).expect("resolve driver request")
    }

    #[test]
    fn driver_starts_and_shuts_down() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let snapshot = driver.snapshot().expect("snapshot");

        assert_eq!(snapshot.listeners.len(), 0);
        assert_eq!(snapshot.connections.len(), 0);
        assert_eq!(snapshot.sockets.len(), 0);
        assert!(snapshot.poll_iterations >= 1);

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn wakeup_path_processes_control_plane_commands() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let before = driver.snapshot().expect("snapshot before").wakeup_count;

        let _listener_id = resolve_request(driver.reserve_listener());
        let after = driver.snapshot().expect("snapshot after").wakeup_count;

        assert!(after >= before + 2);

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn resource_and_token_slots_are_reused_after_release() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");

        let first_connection = resolve_request(driver.reserve_connection());
        let first_snapshot = driver.snapshot().expect("snapshot after first reservation");
        let first_token = first_snapshot.connections[0].token;

        resolve_request(driver.release_connection(first_connection));

        let second_connection = resolve_request(driver.reserve_connection());
        let second_snapshot = driver
            .snapshot()
            .expect("snapshot after second reservation");
        let second_token = second_snapshot.connections[0].token;

        assert_eq!(first_connection, second_connection);
        assert_eq!(first_token, second_token);

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn driver_commands_are_drained_in_fifo_order() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let connection_id = resolve_request(driver.reserve_connection());
        let socket_id = resolve_request(driver.reserve_socket());

        driver
            .dispatch(DriverCommand::Tcp(TcpCommand::Connect {
                connection_id,
                remote_addr: localhost(9001),
            }))
            .expect("dispatch connect");
        driver
            .dispatch(DriverCommand::Tcp(TcpCommand::Send {
                connection_id,
                transmission_id: crate::api::TransmissionId(0),
                payload: IoPayload::Bytes(Bytes::from_static(b"hello")),
            }))
            .expect("dispatch send");
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Close { socket_id }))
            .expect("dispatch close");

        let snapshot = driver.snapshot().expect("snapshot after commands");
        assert_eq!(
            snapshot.processed_commands,
            vec![
                CommandTrace::TcpConnect(connection_id),
                CommandTrace::TcpSend(connection_id),
                CommandTrace::UdpClose(socket_id),
            ]
        );

        driver.shutdown().expect("driver shuts down");
    }
}
