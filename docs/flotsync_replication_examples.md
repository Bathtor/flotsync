---
type: Crate Overview
title: Flotsync Replication Example Applications
description: Describes runnable applications built against the Flotsync replication abstraction.
status: draft
applies_to:
  - flotsync_replication_examples
---

# Flotsync Replication Example Applications

## Scope

The `flotsync_replication_examples` crate contains runnable applications built
against the replication API. It deliberately keeps those applications separate
from the direct `flotsync_io` examples so each package documents one abstraction
level.

`replicated_checklist` is a line-oriented manual replication-slice example. Two
terminals or machines can stage local edits and exchange them when the user runs
`sync`.

## Setup

Each peer needs its local identity, store, and store-secret profile in the
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
cargo run -p flotsync_replication_examples --bin replicated_checklist -- keys init-local alice.toml
cargo run -p flotsync_replication_examples --bin replicated_checklist -- keys init-local bob.toml
```

`keys init-local` creates or reuses the peer's local identity keys in its
configured store. It is the only key operation that runs before the replication
runtime because loading that runtime requires the private identity material.

Start each peer in a separate terminal:

```bash
cargo run -p flotsync_replication_examples --bin replicated_checklist -- run alice.toml
cargo run -p flotsync_replication_examples --bin replicated_checklist -- run bob.toml
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

## Networking and Group Availability

The normal custom UDP discovery path does not need `static-peer-routes`: each
peer announces its local delivery endpoint on the peer-announcement UDP socket,
and route establishment verifies discovered endpoints with a signed
introduction before replication routes become usable.

The runtime local delivery endpoint is optional. When
`flotsync.replication.runtime.local-endpoint-bind-addr` is absent, the runtime
binds an ephemeral wildcard UDP socket and publishes selected concrete local
interface endpoints through peer announcements. Set it only when a fixed local
address or port is required for diagnostics, firewall rules, or static route
experiments; if set, it must be an address assigned to the host. Static peer
routes remain available as optional route hints; see
[`replicated_checklist_scenarios.md`](replicated_checklist_scenarios.md#scenario-6-static-route-hints).

With no active groups, the process opens a key-capable shell and reports an
actionable error for checklist data commands. A store containing exactly one
existing active group retains the current checklist editing and `sync`
behaviour. Fresh group creation and multi-group workspace commands are covered
by the normal REPL workflows.

## Exclusions

The current example does not cover mDNS discovery, automatic synchronisation
after every edit, or offline catch-up/replay after extended disconnection.

Concrete Alice/Bob manual acceptance flows are documented in
[`replicated_checklist_scenarios.md`](replicated_checklist_scenarios.md).
