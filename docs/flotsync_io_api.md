# FlotSync Freeform I/O API (v1 Draft)

## Scope

v1 adds Kompact-native freeform network I/O for raw bytes over UDP and TCP.

## Constraints

- Backend is `mio` (no Tokio runtime dependency).
- Single driver thread for correctness-first behavior.
- Shared ingress and egress buffer pools with drop-based lease return.
- Explicit backpressure signaling (ack/nack/suspend/resume), not bounded queues.
- Inbound TCP listener acceptance must remain application-directed; accepted sockets may not start
  delivering bytes before the application chooses a session owner or rejects them.

## Non-Goals (v1)

- HTTP.
- Multi-threaded I/O sharding.
- `WriteFile`/sendfile support (API should stay extensible for this).
- Making the raw core API and the Kompact-facing API identical.

## Layering

`flotsync_io` deliberately has two API layers:

1. Raw core API
   - transport-oriented
   - local-handle based (`SocketId`, `ConnectionId`, `ListenerId`)
   - suitable for the internal `mio` driver and non-Kompact tests

2. Kompact-facing API
   - shaped around Kompact communication patterns
   - UDP uses typed ports
   - TCP uses manager/session actor-style communication

The raw core API is not the user-facing Kompact API. The Kompact layer adapts the driver to
something that fits Kompact semantics better.

## High-Level Model

- Raw `IoDriver`
  - owns `mio::Poll`, socket tables, readiness state, and shared pools
  - runs on a dedicated OS thread
  - emits transport events through a pluggable event sink

- `IoDriverComponent`
  - one heavy shared Kompact component per `KompactSystem`
  - owns the raw `IoDriver`
  - owns bridge/session routing state inside Kompact-land

- `IoBridge`
  - one lightweight per-client Kompact component
  - provides shared UDP capability via typed ports
  - acts as TCP manager for opening listeners and outbound sessions

- `TcpListener`
  - one Kompact actor/component endpoint per open TCP listener
  - emits pending inbound-session decisions instead of pre-owned session actors

- `TcpSession`
  - one Kompact actor/component endpoint per live TCP connection
  - represents the session identity instead of repeatedly passing `ConnectionId`

## Raw Core API Shape

The raw core API remains close to the current driver-facing types:

```rust
pub struct ListenerId(pub usize);
pub struct ConnectionId(pub usize);
pub struct SocketId(pub usize);
pub struct TransmissionId(pub usize);
```

```rust
pub enum IoPayload {
    Lease(IoLease),
    Bytes(bytes::Bytes),
}
```

- `IoLease` is an immutable, cloneable shared payload handle.
- Reading is done via a separate `IoCursor`, so the same payload can be replayed many times.

### Raw Send Outcome Semantics

Raw driver send outcomes must always carry the local socket/connection handle in addition to the
`TransmissionId`.

This avoids any requirement that multiple clients coordinate `TransmissionId` values globally.

Recommended shape:

```rust
TcpEvent::SendAck {
    connection_id: ConnectionId,
    transmission_id: TransmissionId,
}

TcpEvent::SendNack {
    connection_id: ConnectionId,
    transmission_id: TransmissionId,
    reason: SendFailureReason,
}

UdpEvent::SendAck {
    socket_id: SocketId,
    transmission_id: TransmissionId,
}

UdpEvent::SendNack {
    socket_id: SocketId,
    transmission_id: TransmissionId,
    reason: SendFailureReason,
}
```

## Kompact UDP API

UDP is modeled as a shared capability and therefore maps well to a typed `Port`.

### Surface

```rust
pub struct UdpPort;
impl Port for UdpPort {
    type Request = UdpRequest;
    type Indication = UdpIndication;
}
```

```rust
pub enum UdpRequest {
    Bind {
        socket_id: SocketId,
        local_addr: SocketAddr,
    },
    Connect {
        socket_id: SocketId,
        remote_addr: SocketAddr,
        local_addr: Option<SocketAddr>,
    },
    Send {
        socket_id: SocketId,
        transmission_id: TransmissionId,
        payload: IoPayload,
        target: Option<SocketAddr>,
        reply_to: Recipient<UdpSendResult>,
    },
    Configure {
        socket_id: SocketId,
        option: UdpSocketOption,
    },
    Close {
        socket_id: SocketId,
    },
}
```

```rust
pub enum OpenFailureReason {
    Io(io::ErrorKind),
    InvalidHandle,
    DriverUnavailable,
}
```

```rust
pub enum ConfigureFailureReason {
    Io(io::ErrorKind),
    InvalidHandle,
    DriverUnavailable,
}
```

```rust
pub enum UdpSocketOption {
    Broadcast(bool),
    MulticastLoopV4(bool),
    MulticastLoopV6(bool),
    MulticastTtlV4(u32),
    MulticastHopsV6(u32),
    MulticastInterfaceV4(Ipv4Addr),
    MulticastInterfaceV6(u32),
    JoinMulticastV4 { group: Ipv4Addr, interface: Ipv4Addr },
    LeaveMulticastV4 { group: Ipv4Addr, interface: Ipv4Addr },
    JoinMulticastV6 { group: Ipv6Addr, interface: u32 },
    LeaveMulticastV6 { group: Ipv6Addr, interface: u32 },
}
```

```rust
pub enum UdpIndication {
    Bound {
        socket_id: SocketId,
        local_addr: SocketAddr,
    },
    BindFailed {
        socket_id: SocketId,
        local_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    Connected {
        socket_id: SocketId,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
    },
    ConnectFailed {
        socket_id: SocketId,
        local_addr: Option<SocketAddr>,
        remote_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    Received {
        socket_id: SocketId,
        source: SocketAddr,
        payload: IoPayload,
    },
    Configured {
        socket_id: SocketId,
        option: UdpSocketOption,
    },
    ConfigureFailed {
        socket_id: SocketId,
        option: UdpSocketOption,
        reason: ConfigureFailureReason,
    },
    ReadSuspended {
        socket_id: SocketId,
    },
    ReadResumed {
        socket_id: SocketId,
    },
    WriteSuspended {
        socket_id: SocketId,
    },
    WriteResumed {
        socket_id: SocketId,
    },
    Closed {
        socket_id: SocketId,
    },
}
```

```rust
pub enum UdpSendResult {
    Ack {
        socket_id: SocketId,
        transmission_id: TransmissionId,
    },
    Nack {
        socket_id: SocketId,
        transmission_id: TransmissionId,
        reason: SendFailureReason,
    },
}
```

### Semantics

- A single `IoBridge` provides one shared UDP capability to its connected components.
- Multiple local components may observe the same socket's `Received` and lifecycle indications.
- UDP bind/connect failures are broadcast as port indications because they describe the shared socket capability state.
- UDP socket-configuration changes are also broadcast as port indications because they mutate the
  shared socket capability seen by every local consumer on that bridge.
- UDP send outcomes are **not** broadcast as port indications.
- Every UDP `Send` request must carry a `reply_to: Recipient<UdpSendResult>`.
- `Ack` / `Nack` is delivered only to that recipient.
- Unconnected UDP still routes send outcomes by the local `socket_id`; the per-send `target`
  belongs to the command, not to the socket identity.
- `Configure { option }` changes shared socket state. Multiple local components sharing one UDP
  socket must therefore coordinate option changes such as broadcast enablement or multicast
  membership. v1 does not add bridge-local membership reference counting.

This split is deliberate:

- shared socket activity (`Received`, `Closed`, suspension/resume) is broadcast via the port
- shared socket configuration (`Configured`, `ConfigureFailed`) is broadcast via the port
- per-send completion is point-to-point via the supplied `Recipient`

That keeps UDP's shared capability model while avoiding useless broadcast of send completion.

## Kompact TCP API

TCP is modeled as a manager/session API rather than a raw port carrying `ConnectionId` on every
message.

### Surface

The bridge acts as a TCP manager for both outbound sessions and inbound listeners.

```rust
pub struct OpenTcpListener {
    pub local_addr: SocketAddr,
    pub incoming_to: Recipient<TcpListenerEvent>,
}
```

Opening a listener is an actor request/response operation:

```rust
bridge_ref.ask(OpenTcpListener {
    local_addr,
    incoming_to,
}) -> Result<TcpListenerRef>
```

`TcpListenerRef` is then used for listener-local control:

- it is a strong local listener handle
- `tell(TcpListenerRequest::Close)` is the normal fast path
- accepted inbound sockets are reported via `TcpListenerEvent::Incoming`
- the listener does not decide session ownership by itself; application logic must do that

```rust
pub enum TcpListenerRequest {
    Close,
}

pub enum TcpListenerEvent {
    Listening {
        local_addr: SocketAddr,
    },
    ListenFailed {
        local_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    Incoming {
        peer_addr: SocketAddr,
        pending: PendingTcpSession,
    },
    Closed,
}
```

```rust
impl PendingTcpSession {
    pub fn accept(
        self,
        events_to: Recipient<TcpSessionEvent>,
    ) -> KFuture<Result<TcpSessionRef>>;

    pub fn reject(self) -> KFuture<Result<()>>;
}
```

`PendingTcpSession` is non-cloneable and auto-rejects on `Drop`.

This is the key inbound-TCP semantic:

- the raw driver may accept the OS socket
- but normal TCP session I/O stays paused
- application logic inspects the incoming connection
- then it either:
  - accepts it into a session and chooses the session event recipient
  - rejects it
  - or drops the pending handle and lets it auto-reject

Only `accept(...)` turns the pending socket into a live `TcpSessionRef`.

```rust
pub struct OpenTcpSession {
    pub remote_addr: SocketAddr,
    pub local_addr: Option<SocketAddr>,
    pub events_to: Recipient<TcpSessionEvent>,
}
```

Opening a session is an actor request/response operation:

```rust
bridge_ref.ask(OpenTcpSession {
    remote_addr,
    local_addr,
}) -> Result<TcpSessionRef>
```

`TcpSessionRef` is then used for all traffic on that session:

- it is a strong local session handle, not just a narrowed `Recipient`
- `tell(TcpSessionRequest)` is the normal fast path
- `recipient()` can still be derived when a `Recipient<TcpSessionRequest>` is specifically needed
- session cleanup remains explicit and lifecycle-driven; dropping a `TcpSessionRef` does not by
  itself define socket teardown semantics

```rust
pub enum TcpSessionRequest {
    Send {
        transmission_id: TransmissionId,
        payload: IoPayload,
    },
    Close {
        abort: bool,
    },
}

pub enum TcpSessionEvent {
    Connected {
        peer_addr: SocketAddr,
    },
    ConnectFailed {
        remote_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    Received {
        payload: IoPayload,
    },
    SendAck {
        transmission_id: TransmissionId,
    },
    SendNack {
        transmission_id: TransmissionId,
        reason: SendFailureReason,
    },
    ReadSuspended,
    ReadResumed,
    WriteSuspended,
    WriteResumed,
    Closed {
        reason: CloseReason,
    },
}
```

### Semantics

- Session identity is represented by the Kompact session endpoint, not by repeatedly exposing
  `ConnectionId` to the client.
- `TransmissionId` only needs to be meaningful within one session.
- Inbound listeners expose pending decisions first; they do not auto-spawn session owners.
- Accepted inbound sessions reuse the same `TcpSessionRef` model as outbound sessions once the
  application accepts them.
- Accepted inbound sessions do not emit a separate `Connected` event; the listener's `Incoming`
  event is the establishment signal for that path.

## Backpressure Semantics

- Every write carries a `TransmissionId`.
- UDP `SendAck` / `SendNack` is sent only to the per-send `Recipient`.
- TCP `SendAck` / `SendNack` stays on the session event stream.
- TCP emits `WriteSuspended` when a non-blocking flush leaves bytes pending and the session stops
  accepting new sends without internal queuing.
- TCP emits `WriteResumed` once that pending transmission drains and the session starts accepting
  new sends again.
- UDP emits `WriteSuspended` when a datagram send hits kernel backpressure (`WouldBlock`); the
  triggering datagram still receives `SendNack { Backpressure }`.
- While a UDP socket is write-suspended, later sends are rejected immediately with
  `SendNack { Backpressure }` until the socket emits `WriteResumed`.
- Inbound reads are push-based.
- Inbound payload memory is lease-backed; dropping a lease returns capacity.
- If read-side pool capacity is exhausted, read interest is disabled and `ReadSuspended` is emitted.
- Reads resume automatically once capacity returns, then emit `ReadResumed`.

## Driver Event Delivery

The raw driver should not be hardwired to a crossbeam event receiver as its only output path.

Instead, event delivery should be pluggable:

- test path: channel-backed sink
- Kompact path: actor-backed sink

The driver thread already exists; the Kompact integration should not add another forwarding thread
just to block on an event channel and relay the result into Kompact.

## Handle Type Choice

- v1 uses `usize` for local handle identifiers because these are in-process runtime handles, not
  wire/persistence identifiers.
- This maps naturally to index-based internal registries (for example slab-like tables) with
  minimal conversion overhead.
- If we later need architecture-independent identifiers for cross-process/wire/state formats, we
  can introduce separate stable ID types at that boundary (for example `u64`).

## UDP v1

- Bind/unbind.
- Connected and unconnected send/receive.
- Explicit connected-mode command/event path, not just `Send { target: None }`.
- Broadcast and multicast remain a later step than the first Kompact bridge.
- Datagram size is currently bounded by the configured pool chunk size.

## TCP v1

- Outbound connect.
- Explicit connect-failed event for outbound connects.
- Listener bind/listen/close.
- Accepted inbound connections start in a paused pending-adoption state.
- Pending inbound connections can be accepted into a session or rejected.
- Read/write raw bytes.
- Graceful close and abort semantics.

## Extensibility Notes

- Keep raw command/event enums extensible so `WriteFile` can be added later.
- Keep payload abstraction flexible for future zero-copy/file-backed variants.
- The Kompact layer intentionally has different abstractions for UDP and TCP; that difference is a
  feature, not an inconsistency.
- Further session cleanup/GC after the last external session handle drops remains follow-up work.
