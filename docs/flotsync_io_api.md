# FlotSync Freeform I/O API (v1 Draft)

## Scope

v1 adds Kompact-native freeform network I/O for raw bytes over UDP and TCP.

## Constraints

- Backend is `mio` (no Tokio runtime dependency).
- Single driver thread for correctness-first behavior.
- Kompact-facing API uses typed ports/channels.
- Explicit backpressure signaling (ack/nack/suspend/resume), not bounded queues.
- Shared ingress and egress buffer pools with drop-based lease return.
- Listener bridge owns accepted TCP connections in v1.

## Non-Goals (v1)

- HTTP.
- Multi-threaded I/O sharding.
- `WriteFile`/sendfile support (API should stay extensible for this).

## High-Level Model

- `IoDriver`: one heavy shared component per `KompactSystem`.
  - Owns `mio::Poll`, socket tables, multicast state, and shared pools.
- `IoBridge`: lightweight per-client bridge component.
  - Exposes typed ports to client components.
  - Routes commands/events between client and `IoDriver`.

## API Shape (Sketch)

```rust
pub struct TcpPort;
impl Port for TcpPort {
    type Request = TcpCommand;
    type Indication = TcpEvent;
}

pub struct UdpPort;
impl Port for UdpPort {
    type Request = UdpCommand;
    type Indication = UdpEvent;
}
```

```rust
pub struct ListenerId(pub usize);
pub struct ConnectionId(pub usize);
pub struct SocketId(pub usize);
pub struct TransmissionId(pub usize);
```

```rust
pub enum IoPayload {
    Lease(IoLease),
    Bytes(bytes::Bytes), // compatibility/edge path
}
```

- `IoLease` is an immutable, cloneable shared payload handle.
- Reading is done via a separate `IoCursor`, so the same payload can be replayed many times.

## Backpressure Semantics

- Every write carries a `TransmissionId`.
- Driver emits `SendAck { transmission_id }` or `SendNack { transmission_id, reason }`.
- Driver may emit `WriteSuspended { target }` and `WriteResumed { target }`.
- Inbound reads are push-based.
- Inbound payload memory is lease-backed; dropping a lease returns capacity.
- If read-side pool capacity is exhausted, read interest is disabled and `ReadSuspended` is emitted.
- Reads resume automatically once capacity returns, then emit `ReadResumed`.

## Handle Type Choice

- v1 uses `usize` for local handle identifiers because these are in-process runtime handles, not wire/persistence identifiers.
- This maps naturally to index-based internal registries (for example slab-like tables) with minimal conversion overhead.
- If we later need architecture-independent identifiers for cross-process/wire/state formats, we can introduce separate stable ID types at that boundary (for example `u64`).

## UDP v1

- Bind/unbind.
- Connected and unconnected send/receive.
- Explicit connected-mode command/event path, not just `Send { target: None }`.
- Broadcast.
- Multicast join/leave (+ interface/ttl/loop controls).
- Datagram size is currently bounded by the configured pool chunk size.

## TCP v1

- Outbound connect.
- Explicit connect-failed event for outbound connects.
- Listener bind/accept.
- Read/write raw bytes.
- Graceful close and abort semantics.

## Extensibility Notes

- Keep command/event enums non-exhaustive internally so `WriteFile` can be added later.
- Keep payload abstraction flexible for future zero-copy/file-backed variants.
- Add bridge-to-bridge accepted-connection transfer after v1 (TODO).
