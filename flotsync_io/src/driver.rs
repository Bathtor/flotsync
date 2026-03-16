//! Single-threaded mio driver core and control plane.

use crate::{
    api::{
        ConnectionId,
        IoPayload,
        ListenerId,
        MAX_UDP_PAYLOAD_BYTES,
        SendFailureReason,
        SocketId,
        TcpCommand,
        TcpEvent,
        UdpCommand,
        UdpEvent,
    },
    errors::{
        CreateDriverPollSnafu,
        CreateDriverWakerSnafu,
        DriverPollSnafu,
        DriverWakeSnafu,
        Error,
        Result,
        SpawnDriverThreadSnafu,
    },
    pool::{EgressPool, IngressPool, IoBufferConfig, IoBufferPools, PoolAvailabilityNotifier},
};
use bytes::Buf;
use flotsync_utils::option_when;
use mio::{Events, Interest, Poll, Registry, Token, Waker, net::UdpSocket as MioUdpSocket};
use snafu::ResultExt;
use std::{
    borrow::Cow,
    fmt,
    future::Future,
    io,
    io::ErrorKind,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll as TaskPoll},
    thread::{self, JoinHandle},
    time::Duration,
};

const WAKE_TOKEN: Token = Token(0);
const FIRST_RESOURCE_TOKEN_INDEX: usize = 1;
const DEFAULT_EVENTS_CAPACITY: usize = 1024;
const DEFAULT_THREAD_NAME: &str = "flotsync-io-driver";

/// Driver-owned registration token space reserved for mio resources.
///
/// `Token(0)` is reserved for the driver's internal [`mio::Waker`]. Resource registrations start
/// at [`FIRST_RESOURCE_TOKEN_INDEX`].
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
struct DriverThreadConfig {
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
        let builder = thread::Builder::new().name(config.thread_name.to_string());
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

#[derive(Debug)]
enum ControlCommand {
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
enum ResourceKey {
    Listener(ListenerId),
    Connection(ConnectionId),
    Socket(SocketId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ResourceRecord {
    token: DriverToken,
    readiness_hits: usize,
}

impl ResourceRecord {
    fn new(token: DriverToken) -> Self {
        Self {
            token,
            readiness_hits: 0,
        }
    }
}

#[derive(Debug)]
struct UdpSocketEntry {
    record: ResourceRecord,
    socket: Option<MioUdpSocket>,
    local_addr: Option<SocketAddr>,
    remote_addr: Option<SocketAddr>,
    read_suspended: bool,
    registered: bool,
}

impl UdpSocketEntry {
    fn new(record: ResourceRecord) -> Self {
        Self {
            record,
            socket: None,
            local_addr: None,
            remote_addr: None,
            read_suspended: false,
            registered: false,
        }
    }

    /// Returns whether this entry currently owns a live OS UDP socket handle.
    fn is_open(&self) -> bool {
        self.socket.is_some()
    }
}

/// Dense slot registry backed by a vector plus a LIFO free-list.
///
/// Released slots are pushed onto `free` and reused from the top, so this behaves like a stack of
/// available slots rather than a full queue or set of indices.
#[derive(Debug)]
struct SlotRegistry<T> {
    entries: Vec<Option<T>>,
    free: Vec<usize>,
}

impl<T> Default for SlotRegistry<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            free: Vec::new(),
        }
    }
}

impl<T> SlotRegistry<T> {
    fn next_slot(&self) -> usize {
        self.free.last().copied().unwrap_or(self.entries.len())
    }

    fn reserve(&mut self, value: T) -> usize {
        if let Some(slot) = self.free.pop() {
            self.entries[slot] = Some(value);
            return slot;
        }

        let slot = self.entries.len();
        self.entries.push(Some(value));
        slot
    }

    fn remove(&mut self, slot: usize) -> Option<T> {
        if slot >= self.entries.len() {
            return None;
        }

        let value = self.entries[slot].take();
        if value.is_some() {
            self.free.push(slot);
        }
        value
    }

    fn get(&self, slot: usize) -> Option<&T> {
        self.entries.get(slot).and_then(Option::as_ref)
    }

    fn get_mut(&mut self, slot: usize) -> Option<&mut T> {
        self.entries.get_mut(slot).and_then(Option::as_mut)
    }

    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = (usize, &T)> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(slot, entry)| entry.as_ref().map(|value| (slot, value)))
    }
}

/// Driver-owned lookup tables for both caller-facing handles and mio readiness tokens.
///
/// The typed handle registries (`listeners`, `connections`, `sockets`) answer API lookups in
/// their own namespaces. The shared `readiness_keys` registry answers poll-loop lookups from
/// `mio::Token` back to the resource kind that became ready.
///
/// A reservation therefore allocates in two independent spaces:
/// 1. a typed handle slot in the resource-specific registry
/// 2. a readiness slot in `readiness_keys`
///
/// The readiness slot is converted into a public `mio::Token` by adding
/// [`FIRST_RESOURCE_TOKEN_INDEX`], because `Token(0)` is reserved for the driver's internal
/// waker. Handle ids and readiness slots may happen to share the same integer value, but that is
/// incidental and never relied on.
#[derive(Debug, Default)]
struct DriverRuntimeState {
    listeners: SlotRegistry<ResourceRecord>,
    connections: SlotRegistry<ResourceRecord>,
    sockets: SlotRegistry<UdpSocketEntry>,
    readiness_keys: SlotRegistry<ResourceKey>,
    #[cfg(test)]
    test_state: DriverTestState,
}

impl DriverRuntimeState {
    fn reserve_listener(&mut self) -> ListenerId {
        let listener_id = ListenerId(self.listeners.next_slot());
        let token = self.reserve_readiness(ResourceKey::Listener(listener_id));
        let slot = self.listeners.reserve(ResourceRecord::new(token));
        debug_assert_eq!(slot, listener_id.0);
        listener_id
    }

    fn reserve_connection(&mut self) -> ConnectionId {
        let connection_id = ConnectionId(self.connections.next_slot());
        let token = self.reserve_readiness(ResourceKey::Connection(connection_id));
        let slot = self.connections.reserve(ResourceRecord::new(token));
        debug_assert_eq!(slot, connection_id.0);
        connection_id
    }

    fn reserve_socket(&mut self) -> SocketId {
        let socket_id = SocketId(self.sockets.next_slot());
        let token = self.reserve_readiness(ResourceKey::Socket(socket_id));
        let slot = self
            .sockets
            .reserve(UdpSocketEntry::new(ResourceRecord::new(token)));
        debug_assert_eq!(slot, socket_id.0);
        socket_id
    }

    fn release_listener(&mut self, listener_id: ListenerId) -> Result<()> {
        let entry = self
            .listeners
            .remove(listener_id.0)
            .ok_or(Error::UnknownListener { listener_id })?;
        self.release_readiness(entry.token);
        Ok(())
    }

    fn release_connection(&mut self, connection_id: ConnectionId) -> Result<()> {
        let entry = self
            .connections
            .remove(connection_id.0)
            .ok_or(Error::UnknownConnection { connection_id })?;
        self.release_readiness(entry.token);
        Ok(())
    }

    fn release_socket(&mut self, socket_id: SocketId, registry: &Registry) -> Result<()> {
        let entry = self
            .sockets
            .remove(socket_id.0)
            .ok_or(Error::UnknownSocket { socket_id })?;
        if let Some(mut socket) = entry.socket {
            if entry.registered {
                let deregister_result = registry.deregister(&mut socket);
                if let Err(error) = deregister_result {
                    log::warn!(
                        "failed to deregister UDP socket {} during release: {}",
                        socket_id,
                        error
                    );
                }
            }
        }
        self.release_readiness(entry.record.token);
        Ok(())
    }

    fn record_ready_hit(&mut self, token: DriverToken) {
        let Some(key) = self.readiness_key(token) else {
            log::warn!(
                "received readiness for unknown flotsync_io token {}",
                token.0
            );
            return;
        };

        match key {
            ResourceKey::Listener(listener_id) => {
                if let Some(entry) = self.listeners.get_mut(listener_id.0) {
                    entry.readiness_hits += 1;
                }
            }
            ResourceKey::Connection(connection_id) => {
                if let Some(entry) = self.connections.get_mut(connection_id.0) {
                    entry.readiness_hits += 1;
                }
            }
            ResourceKey::Socket(socket_id) => {
                if let Some(entry) = self.sockets.get_mut(socket_id.0) {
                    entry.record.readiness_hits += 1;
                }
            }
        }
    }

    fn handle_udp_bind(
        &mut self,
        socket_id: SocketId,
        local_addr: SocketAddr,
        registry: &Registry,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<()> {
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            return emit_udp_event(
                event_tx,
                UdpEvent::BindFailed {
                    socket_id,
                    local_addr,
                    error_kind: ErrorKind::NotFound,
                },
            );
        };

        if entry.is_open() {
            return emit_udp_event(
                event_tx,
                UdpEvent::BindFailed {
                    socket_id,
                    local_addr,
                    error_kind: ErrorKind::AlreadyExists,
                },
            );
        }

        let mut socket = match bind_udp_socket(local_addr) {
            Ok(socket) => socket,
            Err(error) => {
                return emit_udp_event(
                    event_tx,
                    UdpEvent::BindFailed {
                        socket_id,
                        local_addr,
                        error_kind: error.kind(),
                    },
                );
            }
        };

        let register_result =
            registry.register(&mut socket, entry.record.token, Interest::READABLE);
        if let Err(error) = register_result {
            return emit_udp_event(
                event_tx,
                UdpEvent::BindFailed {
                    socket_id,
                    local_addr,
                    error_kind: error.kind(),
                },
            );
        }

        let actual_local_addr = match socket.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                log::warn!(
                    "failed to query local address for UDP socket {} after bind: {}",
                    socket_id,
                    error
                );
                local_addr
            }
        };

        entry.socket = Some(socket);
        entry.local_addr = Some(actual_local_addr);
        entry.remote_addr = None;
        entry.read_suspended = false;
        entry.registered = true;

        emit_udp_event(
            event_tx,
            UdpEvent::Bound {
                socket_id,
                local_addr: actual_local_addr,
            },
        )
    }

    fn handle_udp_connect(
        &mut self,
        socket_id: SocketId,
        remote_addr: SocketAddr,
        local_addr: Option<SocketAddr>,
        registry: &Registry,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<()> {
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            return emit_udp_event(
                event_tx,
                UdpEvent::ConnectFailed {
                    socket_id,
                    local_addr,
                    remote_addr,
                    error_kind: ErrorKind::NotFound,
                },
            );
        };

        if entry.is_open() {
            return emit_udp_event(
                event_tx,
                UdpEvent::ConnectFailed {
                    socket_id,
                    local_addr,
                    remote_addr,
                    error_kind: ErrorKind::AlreadyExists,
                },
            );
        }

        let bind_addr = local_addr.unwrap_or_else(|| default_udp_bind_addr(remote_addr));
        let mut socket = match bind_udp_socket(bind_addr) {
            Ok(socket) => socket,
            Err(error) => {
                return emit_udp_event(
                    event_tx,
                    UdpEvent::ConnectFailed {
                        socket_id,
                        local_addr,
                        remote_addr,
                        error_kind: error.kind(),
                    },
                );
            }
        };

        let connect_result = socket.connect(remote_addr);
        if let Err(error) = connect_result {
            return emit_udp_event(
                event_tx,
                UdpEvent::ConnectFailed {
                    socket_id,
                    local_addr,
                    remote_addr,
                    error_kind: error.kind(),
                },
            );
        }

        let register_result =
            registry.register(&mut socket, entry.record.token, Interest::READABLE);
        if let Err(error) = register_result {
            return emit_udp_event(
                event_tx,
                UdpEvent::ConnectFailed {
                    socket_id,
                    local_addr,
                    remote_addr,
                    error_kind: error.kind(),
                },
            );
        }

        let actual_local_addr = match socket.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                log::warn!(
                    "failed to query local address for connected UDP socket {}: {}",
                    socket_id,
                    error
                );
                bind_addr
            }
        };

        entry.socket = Some(socket);
        entry.local_addr = Some(actual_local_addr);
        entry.remote_addr = Some(remote_addr);
        entry.read_suspended = false;
        entry.registered = true;

        emit_udp_event(
            event_tx,
            UdpEvent::Connected {
                socket_id,
                local_addr: actual_local_addr,
                remote_addr,
            },
        )
    }

    fn handle_udp_send(
        &mut self,
        socket_id: SocketId,
        transmission_id: crate::api::TransmissionId,
        payload: IoPayload,
        target: Option<SocketAddr>,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
        udp_send_scratch: &mut [u8],
    ) -> Result<()> {
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            return emit_udp_event(
                event_tx,
                UdpEvent::SendNack {
                    transmission_id,
                    reason: SendFailureReason::Closed,
                },
            );
        };

        if !entry.is_open() {
            return emit_udp_event(
                event_tx,
                UdpEvent::SendNack {
                    transmission_id,
                    reason: SendFailureReason::InvalidState,
                },
            );
        }

        let send_target = match (entry.remote_addr, target) {
            (Some(_), Some(_)) => {
                return emit_udp_event(
                    event_tx,
                    UdpEvent::SendNack {
                        transmission_id,
                        reason: SendFailureReason::UnexpectedTargetForConnectedSocket,
                    },
                );
            }
            (Some(_), None) => UdpSendTarget::Connected,
            (None, Some(target)) => UdpSendTarget::Unconnected(target),
            (None, None) => {
                return emit_udp_event(
                    event_tx,
                    UdpEvent::SendNack {
                        transmission_id,
                        reason: SendFailureReason::MissingTargetForUnconnectedSocket,
                    },
                );
            }
        };

        let payload_len = payload.len();
        if payload_len > MAX_UDP_PAYLOAD_BYTES {
            return emit_udp_event(
                event_tx,
                UdpEvent::SendNack {
                    transmission_id,
                    reason: SendFailureReason::MessageTooLarge,
                },
            );
        }

        let send_result = {
            let socket = entry
                .socket
                .as_mut()
                .expect("open UDP socket missing handle");
            send_udp_payload(socket, payload, send_target, udp_send_scratch)
        };
        match send_result {
            Ok(bytes_sent) if bytes_sent == payload_len => {
                emit_udp_event(event_tx, UdpEvent::SendAck { transmission_id })
            }
            Ok(bytes_sent) => {
                log::error!(
                    "UDP socket {} sent {} of {} requested bytes for transmission {}",
                    socket_id,
                    bytes_sent,
                    payload_len,
                    transmission_id
                );
                emit_udp_event(
                    event_tx,
                    UdpEvent::SendNack {
                        transmission_id,
                        reason: SendFailureReason::IoError,
                    },
                )
            }
            Err(error) => emit_udp_event(
                event_tx,
                UdpEvent::SendNack {
                    transmission_id,
                    reason: SendFailureReason::from(&error),
                },
            ),
        }
    }

    fn handle_udp_close(
        &mut self,
        socket_id: SocketId,
        registry: &Registry,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<()> {
        if self.release_socket(socket_id, registry).is_err() {
            log::warn!("ignored UDP close for unknown socket {}", socket_id);
            return Ok(());
        }

        emit_udp_event(event_tx, UdpEvent::Closed { socket_id })
    }

    fn handle_udp_readable(
        &mut self,
        socket_id: SocketId,
        registry: &Registry,
        ingress_pool: &IngressPool,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<()> {
        loop {
            let mut ingress_buffer = match ingress_pool.try_acquire()? {
                Some(buffer) => buffer,
                None => {
                    let suspended = self.suspend_udp_read(socket_id, registry);
                    if suspended {
                        emit_udp_event(event_tx, UdpEvent::ReadSuspended { socket_id })?;
                    }
                    return Ok(());
                }
            };

            let recv_result = {
                let Some(entry) = self.sockets.get_mut(socket_id.0) else {
                    log::debug!(
                        "ignoring stale UDP readability for unknown socket {}",
                        socket_id
                    );
                    return Ok(());
                };
                let Some(socket) = entry.socket.as_mut() else {
                    log::debug!(
                        "ignoring stale UDP readability for socket {} without OS handle",
                        socket_id
                    );
                    return Ok(());
                };
                if let Some(remote_addr) = entry.remote_addr {
                    let recv_result = socket.recv(ingress_buffer.writable());
                    recv_result.map(|bytes_read| (bytes_read, remote_addr))
                } else {
                    socket.recv_from(ingress_buffer.writable())
                }
            };

            match recv_result {
                Ok((0, source)) => {
                    drop(ingress_buffer);
                    emit_udp_event(
                        event_tx,
                        UdpEvent::Received {
                            socket_id,
                            source,
                            payload: IoPayload::Bytes(bytes::Bytes::new()),
                        },
                    )?;
                }
                Ok((bytes_read, source)) => {
                    let lease = ingress_buffer.commit(bytes_read)?;
                    emit_udp_event(
                        event_tx,
                        UdpEvent::Received {
                            socket_id,
                            source,
                            payload: IoPayload::Lease(lease),
                        },
                    )?;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    return Ok(());
                }
                Err(error) => {
                    log::error!("UDP socket {} recv failed: {}", socket_id, error);
                    return Ok(());
                }
            }
        }
    }

    /// Returns `true` when the socket transitioned from readable to driver-suspended.
    fn suspend_udp_read(&mut self, socket_id: SocketId, registry: &Registry) -> bool {
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            return false;
        };
        if entry.read_suspended || !entry.registered {
            return false;
        }

        let Some(socket) = entry.socket.as_mut() else {
            return false;
        };

        let deregister_result = registry.deregister(socket);
        if let Err(error) = deregister_result {
            log::error!(
                "failed to suspend UDP socket {} by deregistering it: {}",
                socket_id,
                error
            );
            return false;
        }

        entry.read_suspended = true;
        entry.registered = false;
        true
    }

    fn resume_suspended_udp_reads(
        &mut self,
        registry: &Registry,
        ingress_pool: &IngressPool,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<()> {
        if !ingress_pool.has_available_capacity()? {
            return Ok(());
        }

        let suspended_sockets: Vec<SocketId> = self
            .sockets
            .entries
            .iter()
            .enumerate()
            .filter_map(|(slot, entry_opt)| {
                entry_opt.as_ref().and_then(|entry| {
                    option_when!(entry.read_suspended && entry.is_open(), SocketId(slot))
                })
            })
            .collect();

        for socket_id in suspended_sockets {
            let resumed = self.resume_udp_read(socket_id, registry);
            if resumed {
                emit_udp_event(event_tx, UdpEvent::ReadResumed { socket_id })?;
            }
        }

        Ok(())
    }

    /// Returns `true` when the socket transitioned from driver-suspended back to readable.
    fn resume_udp_read(&mut self, socket_id: SocketId, registry: &Registry) -> bool {
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            return false;
        };
        if !entry.read_suspended {
            return false;
        }

        let Some(socket) = entry.socket.as_mut() else {
            return false;
        };

        let register_result = registry.register(socket, entry.record.token, Interest::READABLE);
        if let Err(error) = register_result {
            log::error!(
                "failed to resume UDP socket {} by re-registering it: {}",
                socket_id,
                error
            );
            return false;
        }

        entry.read_suspended = false;
        entry.registered = true;
        true
    }

    /// Returns `true` when command processing requested that the driver thread stop.
    fn drain_commands(
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
                        DriverCommand::Tcp(command) => {
                            log::debug!(
                                "UDP core does not yet implement TCP command {:?}",
                                command
                            );
                        }
                        DriverCommand::Udp(command) => match command {
                            UdpCommand::Bind {
                                socket_id,
                                local_addr,
                            } => {
                                self.handle_udp_bind(socket_id, local_addr, registry, event_tx)?;
                            }
                            UdpCommand::Connect {
                                socket_id,
                                remote_addr,
                                local_addr,
                            } => {
                                self.handle_udp_connect(
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
                                self.handle_udp_send(
                                    socket_id,
                                    transmission_id,
                                    payload,
                                    target,
                                    event_tx,
                                    udp_send_scratch,
                                )?;
                            }
                            UdpCommand::Close { socket_id } => {
                                self.handle_udp_close(socket_id, registry, event_tx)?;
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
                    let result = self.release_connection(connection_id);
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

    fn readiness_key(&self, token: DriverToken) -> Option<ResourceKey> {
        let readiness_slot = token_to_readiness_slot(token)?;
        self.readiness_keys.get(readiness_slot).copied()
    }

    #[cfg(test)]
    fn snapshot(&self) -> DriverSnapshot {
        DriverSnapshot {
            wakeup_count: self.test_state.wakeup_count,
            poll_iterations: self.test_state.poll_iterations,
            listeners: self
                .listeners
                .iter()
                .map(|(slot, entry)| ResourceSnapshot {
                    slot,
                    token: entry.token,
                    readiness_hits: entry.readiness_hits,
                })
                .collect(),
            connections: self
                .connections
                .iter()
                .map(|(slot, entry)| ResourceSnapshot {
                    slot,
                    token: entry.token,
                    readiness_hits: entry.readiness_hits,
                })
                .collect(),
            sockets: self
                .sockets
                .iter()
                .map(|(slot, entry)| ResourceSnapshot {
                    slot,
                    token: entry.record.token,
                    readiness_hits: entry.record.readiness_hits,
                })
                .collect(),
            processed_commands: self.test_state.processed_commands.clone(),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct DriverTestState {
    wakeup_count: usize,
    poll_iterations: usize,
    processed_commands: Vec<CommandTrace>,
}

#[derive(Clone, Copy, Debug)]
enum UdpSendTarget {
    Connected,
    Unconnected(SocketAddr),
}

fn default_udp_bind_addr(remote_addr: SocketAddr) -> SocketAddr {
    match remote_addr {
        SocketAddr::V4(_) => SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)),
    }
}

fn emit_udp_event(
    event_tx: &crossbeam_channel::Sender<DriverEvent>,
    event: UdpEvent,
) -> Result<()> {
    let send_result = event_tx.send(DriverEvent::Udp(event));
    if send_result.is_err() {
        return Err(Error::DriverEventChannelClosed);
    }
    Ok(())
}

fn bind_udp_socket(local_addr: SocketAddr) -> io::Result<MioUdpSocket> {
    MioUdpSocket::bind(local_addr)
}

fn send_udp_payload(
    socket: &mut MioUdpSocket,
    payload: IoPayload,
    target: UdpSendTarget,
    udp_send_scratch: &mut [u8],
) -> io::Result<usize> {
    match payload {
        IoPayload::Bytes(bytes) => send_udp_slice(socket, bytes.as_ref(), target),
        IoPayload::Lease(lease) => {
            let mut cursor = lease.cursor();
            if cursor.chunk().len() == cursor.remaining() {
                return send_udp_slice(socket, cursor.chunk(), target);
            }

            // mio UDP sends only accept a single contiguous slice, so multi-segment payloads are
            // linearised into a fixed driver-owned scratch buffer before the syscall.
            let mut written = 0;
            while cursor.remaining() > 0 {
                let chunk = cursor.chunk();
                let chunk_len = chunk.len();
                let next_written = written + chunk_len;
                debug_assert!(next_written <= udp_send_scratch.len());
                udp_send_scratch[written..next_written].copy_from_slice(chunk);
                cursor.advance(chunk_len);
                written = next_written;
            }
            send_udp_slice(socket, &udp_send_scratch[..written], target)
        }
    }
}

fn send_udp_slice(
    socket: &mut MioUdpSocket,
    bytes: &[u8],
    target: UdpSendTarget,
) -> io::Result<usize> {
    match target {
        UdpSendTarget::Connected => socket.send(bytes),
        UdpSendTarget::Unconnected(target) => socket.send_to(bytes, target),
    }
}

fn run_driver_thread(
    config: DriverThreadConfig,
    mut poll: Poll,
    command_rx: crossbeam_channel::Receiver<ControlCommand>,
    event_tx: crossbeam_channel::Sender<DriverEvent>,
    startup_tx: std::sync::mpsc::SyncSender<Result<()>>,
    ingress_pool: IngressPool,
) -> Result<()> {
    let mut state = DriverRuntimeState::default();
    let mut events = Events::with_capacity(config.events_capacity.max(1));
    // This scratch buffer is stack-allocated on purpose: 1472 bytes is small for a dedicated
    // driver thread stack, avoids a permanent heap allocation in the hot path, and matches the
    // current conservative UDP payload cap. Revisit this if we raise the payload ceiling
    // materially or add jumbo-frame support.
    let mut udp_send_scratch = [0_u8; MAX_UDP_PAYLOAD_BYTES];

    log::debug!(
        "flotsync_io driver thread entering poll loop with event capacity {}",
        events.capacity()
    );

    if startup_tx.send(Ok(())).is_err() {
        log::warn!("flotsync_io driver startup receiver was dropped before readiness signal");
    }

    loop {
        #[cfg(test)]
        {
            state.test_state.poll_iterations += 1;
        }

        poll.poll(&mut events, config.poll_timeout)
            .context(DriverPollSnafu)?;

        'event_loop: for event in &events {
            if event.token() == WAKE_TOKEN {
                #[cfg(test)]
                {
                    state.test_state.wakeup_count += 1;
                }
                let should_stop = state.drain_commands(
                    &command_rx,
                    poll.registry(),
                    &event_tx,
                    &mut udp_send_scratch,
                )?;
                state.resume_suspended_udp_reads(poll.registry(), &ingress_pool, &event_tx)?;
                if should_stop {
                    log::info!("flotsync_io driver thread leaving poll loop");
                    return Ok(());
                }
                continue 'event_loop;
            }

            state.record_ready_hit(event.token());
            let Some(key) = state.readiness_key(event.token()) else {
                continue 'event_loop;
            };
            if event.is_readable() {
                if let ResourceKey::Socket(socket_id) = key {
                    state.handle_udp_readable(
                        socket_id,
                        poll.registry(),
                        &ingress_pool,
                        &event_tx,
                    )?;
                }
            }
        }
    }
}

fn send_reply<T>(
    reply_tx: futures_channel::oneshot::Sender<Result<T>>,
    reply: Result<T>,
    operation: &str,
) {
    if reply_tx.send(reply).is_err() {
        log::debug!(
            "dropping flotsync_io {} reply because the receiver was already gone",
            operation
        );
    }
}

fn readiness_slot_to_token(slot: usize) -> DriverToken {
    Token(slot + FIRST_RESOURCE_TOKEN_INDEX)
}

fn token_to_readiness_slot(token: DriverToken) -> Option<usize> {
    token.0.checked_sub(FIRST_RESOURCE_TOKEN_INDEX)
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "non-string panic payload".to_string(),
        },
    }
}

#[cfg(test)]
fn wait_for_request<T>(mut request: DriverRequest<T>) -> Result<T> {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);

    loop {
        if let Some(reply) = request.try_receive()? {
            return Ok(reply);
        }

        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for flotsync_io driver request reply");
        }

        thread::sleep(Duration::from_millis(1));
    }
}

/*
 * TEST HELPERS
 */

#[cfg(test)]
#[derive(Clone, Debug)]
struct DriverSnapshot {
    wakeup_count: usize,
    poll_iterations: usize,
    listeners: Vec<ResourceSnapshot>,
    connections: Vec<ResourceSnapshot>,
    sockets: Vec<ResourceSnapshot>,
    processed_commands: Vec<CommandTrace>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ResourceSnapshot {
    slot: usize,
    token: DriverToken,
    readiness_hits: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg(test)]
enum CommandTrace {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::{IoPayload, MAX_UDP_PAYLOAD_BYTES, SendFailureReason, TransmissionId, UdpEvent},
        pool::{IoBufferConfig, IoPoolConfig},
        test_support::init_test_logger,
    };
    use bytes::Bytes;
    use std::{
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        time::{Duration, Instant},
    };

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn resolve_request<T>(request: Result<DriverRequest<T>>) -> T {
        let request = request.expect("enqueue driver request");
        wait_for_request(request).expect("resolve driver request")
    }

    fn wait_for_event(driver: &IoDriver) -> DriverEvent {
        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            let event = driver.try_next_event().expect("read driver event");
            if let Some(event) = event {
                return event;
            }

            if Instant::now() >= deadline {
                panic!("timed out waiting for flotsync_io driver event");
            }

            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn payload_bytes(payload: IoPayload) -> Bytes {
        match payload {
            IoPayload::Lease(lease) => lease.create_byte_clone(),
            IoPayload::Bytes(bytes) => bytes,
        }
    }

    fn bind_udp_socket(driver: &IoDriver, socket_id: SocketId) -> SocketAddr {
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Bind {
                socket_id,
                local_addr: localhost(0),
            }))
            .expect("dispatch UDP bind");

        loop {
            let event = wait_for_event(driver);
            match event {
                DriverEvent::Udp(UdpEvent::Bound {
                    socket_id: bound_socket_id,
                    local_addr,
                }) if bound_socket_id == socket_id => return local_addr,
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for bind: {:?}",
                        other
                    );
                }
            }
        }
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
                transmission_id: TransmissionId(0),
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

    #[test]
    fn udp_unconnected_bind_send_and_receive_work() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let receiver_id = resolve_request(driver.reserve_socket());
        let sender_id = resolve_request(driver.reserve_socket());

        let receiver_addr = bind_udp_socket(&driver, receiver_id);
        let sender_addr = bind_udp_socket(&driver, sender_id);
        let transmission_id = TransmissionId(1);

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id,
                payload: IoPayload::Bytes(Bytes::from_static(b"hello")),
                target: Some(receiver_addr),
            }))
            .expect("dispatch UDP send");

        let mut saw_ack = false;
        let mut received_payload = None;
        while !saw_ack || received_payload.is_none() {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::SendAck {
                    transmission_id: ack_id,
                }) if ack_id == transmission_id => {
                    saw_ack = true;
                }
                DriverEvent::Udp(UdpEvent::Received {
                    socket_id,
                    source,
                    payload,
                }) if socket_id == receiver_id => {
                    assert_eq!(source, sender_addr);
                    received_payload = Some(payload_bytes(payload));
                }
                other => {
                    log::debug!("ignoring unrelated UDP event: {:?}", other);
                }
            }
        }

        assert_eq!(received_payload.expect("received payload"), b"hello"[..]);

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_connected_send_and_receive_work() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let receiver_id = resolve_request(driver.reserve_socket());
        let sender_id = resolve_request(driver.reserve_socket());

        let receiver_addr = bind_udp_socket(&driver, receiver_id);

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Connect {
                socket_id: sender_id,
                remote_addr: receiver_addr,
                local_addr: None,
            }))
            .expect("dispatch UDP connect");

        let sender_addr = loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Connected {
                    socket_id,
                    local_addr,
                    remote_addr,
                }) if socket_id == sender_id => {
                    assert_eq!(remote_addr, receiver_addr);
                    break local_addr;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP connect: {:?}",
                        other
                    );
                }
            }
        };

        let transmission_id = TransmissionId(2);
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id,
                payload: IoPayload::Bytes(Bytes::from_static(b"world")),
                target: None,
            }))
            .expect("dispatch connected UDP send");

        let mut saw_ack = false;
        let mut received_payload = None;
        while !saw_ack || received_payload.is_none() {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::SendAck {
                    transmission_id: ack_id,
                }) if ack_id == transmission_id => {
                    saw_ack = true;
                }
                DriverEvent::Udp(UdpEvent::Received {
                    socket_id,
                    source,
                    payload,
                }) if socket_id == receiver_id => {
                    assert_eq!(source, sender_addr);
                    received_payload = Some(payload_bytes(payload));
                }
                other => {
                    log::debug!("ignoring unrelated UDP event: {:?}", other);
                }
            }
        }

        assert_eq!(received_payload.expect("received payload"), b"world"[..]);

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_missing_target_for_unconnected_socket_is_nacked() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let socket_id = resolve_request(driver.reserve_socket());
        bind_udp_socket(&driver, socket_id);

        let transmission_id = TransmissionId(3);
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id,
                transmission_id,
                payload: IoPayload::Bytes(Bytes::from_static(b"oops")),
                target: None,
            }))
            .expect("dispatch invalid UDP send");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::SendNack {
                    transmission_id: nack_id,
                    reason,
                }) if nack_id == transmission_id => {
                    assert_eq!(reason, SendFailureReason::MissingTargetForUnconnectedSocket);
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP SendNack: {:?}",
                        other
                    );
                }
            }
        }

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_unexpected_target_for_connected_socket_is_nacked() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let receiver_id = resolve_request(driver.reserve_socket());
        let sender_id = resolve_request(driver.reserve_socket());

        let receiver_addr = bind_udp_socket(&driver, receiver_id);

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Connect {
                socket_id: sender_id,
                remote_addr: receiver_addr,
                local_addr: None,
            }))
            .expect("dispatch UDP connect");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Connected { socket_id, .. })
                    if socket_id == sender_id =>
                {
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP connect: {:?}",
                        other
                    );
                }
            }
        }

        let transmission_id = TransmissionId(4);
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id,
                payload: IoPayload::Bytes(Bytes::from_static(b"oops")),
                target: Some(receiver_addr),
            }))
            .expect("dispatch invalid connected UDP send");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::SendNack {
                    transmission_id: nack_id,
                    reason,
                }) if nack_id == transmission_id => {
                    assert_eq!(
                        reason,
                        SendFailureReason::UnexpectedTargetForConnectedSocket
                    );
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP SendNack: {:?}",
                        other
                    );
                }
            }
        }

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_read_suspends_and_resumes_when_ingress_capacity_returns() {
        init_test_logger();

        let pool_config = IoPoolConfig {
            chunk_size: MAX_UDP_PAYLOAD_BYTES,
            initial_chunk_count: 1,
            max_chunk_count: 2,
            encode_buf_min_free_space: 64,
        };
        let driver_config = DriverConfig {
            buffer_config: IoBufferConfig {
                ingress: pool_config.clone(),
                egress: pool_config,
            },
            ..DriverConfig::default()
        };
        let driver = IoDriver::start(driver_config).expect("driver starts");
        let receiver_id = resolve_request(driver.reserve_socket());
        let sender_id = resolve_request(driver.reserve_socket());

        let receiver_addr = bind_udp_socket(&driver, receiver_id);
        bind_udp_socket(&driver, sender_id);

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id: TransmissionId(10),
                payload: IoPayload::Bytes(Bytes::from_static(b"first")),
                target: Some(receiver_addr),
            }))
            .expect("dispatch first UDP send");

        let first_lease = loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Received {
                    socket_id,
                    payload: IoPayload::Lease(lease),
                    ..
                }) if socket_id == receiver_id => break lease,
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for first UDP receive: {:?}",
                        other
                    );
                }
            }
        };

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id: TransmissionId(11),
                payload: IoPayload::Bytes(Bytes::from_static(b"second")),
                target: Some(receiver_addr),
            }))
            .expect("dispatch second UDP send");

        let second_lease = loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Received {
                    socket_id,
                    payload: IoPayload::Lease(lease),
                    ..
                }) if socket_id == receiver_id => break lease,
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for second UDP receive: {:?}",
                        other
                    );
                }
            }
        };

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id: TransmissionId(12),
                payload: IoPayload::Bytes(Bytes::from_static(b"third")),
                target: Some(receiver_addr),
            }))
            .expect("dispatch third UDP send");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::ReadSuspended { socket_id })
                    if socket_id == receiver_id =>
                {
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for ReadSuspended: {:?}",
                        other
                    );
                }
            }
        }

        drop(first_lease);
        drop(second_lease);

        let mut saw_resume = false;
        let mut third_payload = None;
        while !saw_resume || third_payload.is_none() {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::ReadResumed { socket_id })
                    if socket_id == receiver_id =>
                {
                    saw_resume = true;
                }
                DriverEvent::Udp(UdpEvent::Received {
                    socket_id, payload, ..
                }) if socket_id == receiver_id => {
                    third_payload = Some(payload_bytes(payload));
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for ReadResumed/Received: {:?}",
                        other
                    );
                }
            }
        }

        assert_eq!(third_payload.expect("third payload"), b"third"[..]);

        driver.shutdown().expect("driver shuts down");
    }
}
