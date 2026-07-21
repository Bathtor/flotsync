---
type: Scenario
title: flotsync_io Example Applications
description: Describes example applications used to exercise and demonstrate the flotsync_io API.
status: draft
---

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

`http_server` is the second example. It is intentionally small and HTTP/1.1-only so it can
exercise the listener/session model with a familiar request/response workload without introducing
Tokio or a higher-level server framework that would hide the `flotsync_io` transport surface.

`replicated_checklist` is a manual replication-slice example. It is line-oriented and configured
entirely from one node-specific TOML file so two terminals or two machines can stage local edits and
exchange them only when the user runs `sync`.

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

For `http_server`, the current example intentionally does not cover:

- keep-alive or request pipelining
- chunked request bodies
- `Transfer-Encoding`
- HTTP versions other than `HTTP/1.1`
- routing beyond `GET /`, `HEAD /`, and `POST /echo`

That broader HTTP behavior is outside the scope of this example.

For `replicated_checklist`, the current example intentionally does not cover:

- mDNS discovery or automatic membership provisioning
- group creation or membership changes from the REPL
- automatic synchronisation after every edit
- offline catch-up or replay after extended disconnection

The group id and ordered members are static config values for this manual slice.

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

Start the HTTP example server:

```bash
cargo run -p flotsync_io_examples --bin http_server -- --bind 127.0.0.1:0
```

The server prints the chosen loopback address as `HTTP listening on 127.0.0.1:PORT`.
Use that `PORT` in the manual checks below instead of assuming a fixed port.
The example stops when stdin reaches EOF, so on a Unix terminal use `Ctrl-D` on an empty line.
Pressing Enter only writes another input line and does not request shutdown.

Exercise it with `curl`:

```bash
curl --http1.1 -i http://127.0.0.1:PORT/
curl --http1.1 -I http://127.0.0.1:PORT/
curl --http1.1 -i -X POST --data 'hello' http://127.0.0.1:PORT/echo
```

If you want a small protocol-oriented harness without wiring it into `cargo test`, install
[`h11`](https://h11.readthedocs.io/en/stable/basic-usage.html) locally and run the checked-in
helper script:

```bash
python3 -m pip install h11
python3 flotsync_io_examples/scripts/http_h11_smoke.py --port PORT
```

By default the helper runs a small suite covering `GET /`, `HEAD /`, `POST /echo`, and one
negative `404` case. Use `--case post_echo` to run one case only. The current example server
always responds with `Connection: close`, so the harness should expect EOF after each response.
Add `--verbose` if you want the raw `h11` event dump after each case summary.

## Replicated Checklist

Each peer needs only its local identity, store, and store-secret profile in the
application section. Runtime and route settings may remain in the same TOML.

```toml
# alice.toml
[flotsync.examples.replicated-checklist]
local-member = "alice"
store-path = "alice.sqlite"
store-secret-profile = "alice-dev"
```

Initialise the local private identity material before starting the runtime:

```bash
cargo run -p flotsync_io_examples --bin replicated_checklist -- keys init-local alice.toml
cargo run -p flotsync_io_examples --bin replicated_checklist -- keys init-local bob.toml
```

`keys init-local` creates or reuses the peer's local identity keys in its
configured store. It is the only key operation that runs before the replication
runtime because loading that runtime requires the private identity material.

Start each peer in a separate terminal:

```bash
cargo run -p flotsync_io_examples --bin replicated_checklist -- run alice.toml
cargo run -p flotsync_io_examples --bin replicated_checklist -- run bob.toml
```

Once inside the REPL, export, inspect, trust, and block through the already
unlocked runtime:

```text
keys export-local
keys inspect BOB_PUBLIC_BUNDLE
keys trust bob BOB_PUBLIC_BUNDLE
keys block UNTRUSTED_PUBLIC_BUNDLE
```

Trust and block print the current security assessment first and require an
explicit `y` or `yes` confirmation. Blocking accepts the bundle itself and
derives its fingerprint locally.

`store-secret-profile` selects the device-local store secret for this
application profile; the current implementation keeps that secret in OS-backed
local storage and creates it on first use. The checklist no longer accepts a
configured group id, ordered member list, or shared group-secret password.

The normal custom UDP discovery path does not need `static-peer-routes`: each
peer announces its local delivery endpoint on the peer-announcement UDP socket,
and route establishment verifies discovered endpoints with a signed
introduction before replication routes become usable.

The runtime local delivery endpoint is optional for the normal custom UDP discovery path. When
`flotsync.replication.runtime.local-endpoint-bind-addr` is absent, the runtime binds an ephemeral
wildcard UDP socket and publishes selected concrete local interface endpoints through peer
announcements. Set it only when you need a fixed local address or port for diagnostics, firewall
rules, or static route experiments; if you set it, use an address currently assigned to the host.
Static peer routes remain available as optional route hints for fallback or diagnostics; see
[`replicated_checklist_scenarios.md`](replicated_checklist_scenarios.md#scenario-6-static-route-hints).

With no active groups, the process opens a key-capable shell and reports an
actionable error for checklist data commands. A store containing exactly one
existing active group retains the current checklist editing and `sync`
behaviour. Fresh group creation and multi-group workspace commands land in the
next dynamic-group slice.

Concrete Alice/Bob manual acceptance flows for the replicated checklist are documented in
[`replicated_checklist_scenarios.md`](replicated_checklist_scenarios.md).
