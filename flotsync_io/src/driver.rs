//! Single-threaded mio driver core and control plane.

use crate::{
    api::{ConnectionId, ListenerId, SocketId, TcpCommand, TcpEvent, UdpCommand, UdpEvent},
    errors::{
        CreateDriverPollSnafu,
        CreateDriverWakerSnafu,
        DriverPollSnafu,
        DriverWakeSnafu,
        Error,
        Result,
        SpawnDriverThreadSnafu,
    },
    pool::{EgressPool, IngressPool, IoBufferConfig, IoBufferPools},
};
use mio::{Events, Poll, Token, Waker};
use snafu::ResultExt;
use std::{
    borrow::Cow,
    fmt,
    future::Future,
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

        let buffers = IoBufferPools::new(config.buffer_config.clone())?;
        let poll = Poll::new().context(CreateDriverPollSnafu)?;
        let waker = Waker::new(poll.registry(), WAKE_TOKEN).context(CreateDriverWakerSnafu)?;
        let shared_waker = Arc::new(waker);
        let runtime_waker = Arc::clone(&shared_waker);

        let (command_tx, command_rx) = crossbeam_channel::unbounded();
        let (event_tx, event_rx) = crossbeam_channel::unbounded();
        let (startup_tx, startup_rx) = std::sync::mpsc::sync_channel(1);

        let thread_config = DriverThreadConfig::from(&config);
        let builder = thread::Builder::new().name(config.thread_name.to_string());
        let handle = builder
            .spawn(move || run_driver_thread(thread_config, poll, command_rx, event_tx, startup_tx))
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
    sockets: SlotRegistry<ResourceRecord>,
    readiness_keys: SlotRegistry<ResourceKey>,
    wakeup_count: usize,
    poll_iterations: usize,
    #[cfg(test)]
    processed_commands: Vec<CommandTrace>,
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
        let slot = self.sockets.reserve(ResourceRecord::new(token));
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

    fn release_socket(&mut self, socket_id: SocketId) -> Result<()> {
        let entry = self
            .sockets
            .remove(socket_id.0)
            .ok_or(Error::UnknownSocket { socket_id })?;
        self.release_readiness(entry.token);
        Ok(())
    }

    fn handle_ready(&mut self, token: DriverToken) {
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
                    entry.readiness_hits += 1;
                }
            }
        }
    }

    fn drain_commands(
        &mut self,
        command_rx: &crossbeam_channel::Receiver<ControlCommand>,
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
                    self.processed_commands.push(CommandTrace::from(&command));
                    #[cfg(not(test))]
                    let _ = command;
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
                    let result = self.release_socket(socket_id);
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
            wakeup_count: self.wakeup_count,
            poll_iterations: self.poll_iterations,
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
                    token: entry.token,
                    readiness_hits: entry.readiness_hits,
                })
                .collect(),
            processed_commands: self.processed_commands.clone(),
        }
    }
}

fn run_driver_thread(
    config: DriverThreadConfig,
    mut poll: Poll,
    command_rx: crossbeam_channel::Receiver<ControlCommand>,
    _event_tx: crossbeam_channel::Sender<DriverEvent>,
    startup_tx: std::sync::mpsc::SyncSender<Result<()>>,
) -> Result<()> {
    let mut state = DriverRuntimeState::default();
    let mut events = Events::with_capacity(config.events_capacity.max(1));

    log::debug!(
        "flotsync_io driver thread entering poll loop with event capacity {}",
        events.capacity()
    );

    if startup_tx.send(Ok(())).is_err() {
        log::warn!("flotsync_io driver startup receiver was dropped before readiness signal");
    }

    loop {
        state.poll_iterations += 1;

        poll.poll(&mut events, config.poll_timeout)
            .context(DriverPollSnafu)?;

        for event in &events {
            if event.token() == WAKE_TOKEN {
                state.wakeup_count += 1;
                let should_stop = state.drain_commands(&command_rx)?;
                if should_stop {
                    log::info!("flotsync_io driver thread leaving poll loop");
                    return Ok(());
                }
                continue;
            }

            state.handle_ready(event.token());
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
            DriverCommand::Udp(UdpCommand::Send { socket_id, .. }) => Self::UdpSend(*socket_id),
            DriverCommand::Udp(UdpCommand::Close { socket_id }) => Self::UdpClose(*socket_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::{IoPayload, TransmissionId},
        test_support::init_test_logger,
    };
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
}
