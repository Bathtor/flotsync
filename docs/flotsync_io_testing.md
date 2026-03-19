# flotsync_io Testing

## Scope

`flotsync_io` uses two complementary test layers:

- inline unit-style tests in `src/driver/*` and `src/kompact/tests.rs` for transport-specific state-machine details
- integration tests in `flotsync_io/tests/` for public-surface correctness and end-to-end flow-control behavior

The integration matrix validates:

- raw TCP and UDP command/event lifecycles through `IoDriver`
- Kompact UDP shared-capability semantics
- Kompact TCP manager/session/listener semantics
- explicit read/write suspend-resume behavior
- listener adoption, rejection, and auto-reject behavior
- UDP broadcast/multicast control-plane configuration routing

## Intentional Non-Goals

The default Rust integration suite does not try to validate:

- live broadcast packet delivery on the current host/network
- live multicast packet delivery across interfaces or routing domains
- fault-injection scenarios such as delay, reordering, corruption, or packet loss
- broad external conformance suites or packet-level tooling

Those areas are either environment-sensitive or better handled in later soak/fault-injection work.

## Why Broadcast/Multicast Traffic Is Excluded

Broadcast and multicast packet delivery depend on host interface state, routing, firewalling,
loopback behavior, and in some cases VPN or virtual-network configuration. Making those traffic
assertions part of the default `cargo test` path would reduce determinism on development hosts,
including the current macOS environment.

For `flotsync-0w3`, broadcast and multicast are therefore covered only through:

- socket configuration commands
- success/failure indication routing
- shared-bridge visibility semantics

## Running

The expected validation path for this task is:

```bash
cargo test -p flotsync_io
cargo check -p flotsync_discovery
```

No external tooling is required.
