# flotsync_io Example Applications

## Scope

The `flotsync_io_examples` crate contains runnable reference binaries built on top of the
Kompact-facing `flotsync_io` API.

`netcat` is the first example. It is intentionally string-oriented and line-oriented so it can be
used both as a quick manual validation tool and as a compact reference for how to:

- start `IoDriverComponent` and `IoBridge`
- use shared UDP ports
- open outbound TCP sessions
- open TCP listeners and accept or reject pending inbound sessions
- structure the live transport logic inside Kompact components, with only stdin feeding input from
  outside Kompact
- clone the shared `EgressPool` through `IoBridgeHandle::egress_pool()` and encode stdin/script
  lines into lease-backed payloads

## What It Covers

- `tcp connect --remote ADDR [--bind ADDR]`
- `tcp listen --bind ADDR`
- `udp connect --remote ADDR [--bind ADDR]`
- `udp bind --bind ADDR [--target ADDR]`
- `udp sendto --target ADDR [--bind ADDR]`
- scripted sends via repeated `--send STRING`
- deterministic scripted shutdown via `--exit-after-send`

## What It Does Not Cover

- binary payload handling
- multiple concurrent TCP sessions in listener mode
- automatic retries for UDP or TCP backpressure nacks
- live broadcast or multicast traffic workflows

## Examples

Start a TCP listener:

```bash
cargo run -p flotsync_io_examples --bin netcat -- tcp listen --bind 127.0.0.1:7000
```

Connect a TCP client and send one line:

```bash
cargo run -p flotsync_io_examples --bin netcat -- \
  --send "hello from client" \
  --exit-after-send \
  tcp connect --remote 127.0.0.1:7000
```

Start one UDP endpoint with a default target:

```bash
cargo run -p flotsync_io_examples --bin netcat -- \
  udp bind --bind 127.0.0.1:7001 --target 127.0.0.1:7002
```

Start the peer and send one datagram:

```bash
cargo run -p flotsync_io_examples --bin netcat -- \
  --send "hello over udp" \
  --exit-after-send \
  udp sendto --target 127.0.0.1:7001 --bind 127.0.0.1:7002
```

Use the unconnected client variant if you want sends to keep going to the same target even when
the peer disappears, instead of using connected UDP error semantics:

```bash
cargo run -p flotsync_io_examples --bin netcat -- \
  udp sendto --target 127.0.0.1:7001
```

When `udp sendto` omits `--bind`, it uses the same `UdpLocalBind::ForPeer(target)` policy as the
public API: the driver chooses a suitable ephemeral local bind address for that peer and later
reports the concrete address through the normal `UDP bound ...` log line.

If `udp bind` was started without `--target`, it remembers the source address of the most recent
received datagram and uses that as the reply target for subsequent typed lines. That makes it act
more like a classic request/reply UDP console.
