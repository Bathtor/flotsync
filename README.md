# Flotsync

Flotsync is an experimental Rust workspace for local-first, peer-to-peer group
replication. It contains building blocks for replicated data models, group
membership, authenticated and encrypted replication messages, direct UDP route
establishment, multipart datagram transport, and a small application-facing
replication runtime.

The repository is under active development. APIs, protocols, storage layouts,
and crate boundaries may change while the system is still being assembled. This
README is intentionally a stable orientation document rather than a detailed
status page; use `gitrack` for current work and detailed task state.

## Project Status

Flotsync is not yet a finished library or application. The current repository is
best treated as a development workspace with design documents, implementation
slices, tests, and manual scenarios that evolve together.

In particular:

- public APIs are still allowed to change;
- protocol documents describe current design intent, not a compatibility
  promise;
- examples are development and acceptance tools, not polished end-user apps;
- open work, priorities, dependencies, and near-term plans live in `gitrack`.

## Workspace Layout

### Core Model

- `flotsync_core/`: common identifiers, version vectors, group ids, member ids,
  and ordering primitives.
- `flotsync_data_types/`: replicated data structures and schema-aware mutation
  support used by the replication runtime and wire formats.

### Wire Formats and Security

- `flotsync_messages/`: generated protobuf bindings and wire conversion helpers.
- `messages/proto/`: source `.proto` definitions organised by package and
  version.
- `flotsync_security/`: cryptographic building blocks for authenticated and
  encrypted replication frames, member identity keys, group keys, and bootstrap
  material.

### Transport, Routes, and Discovery

- `flotsync_io/`: low-level freeform network I/O and Kompact-facing socket
  integration.
- `flotsync_udpour/`: multipart datagram protocol for carrying one logical
  payload over UDP when it exceeds the single-datagram budget.
- `flotsync_routes/`: route-transport contracts, route establishment, endpoint
  selection, and route transport management.
- `flotsync_discovery/`: peer announcement and discovery services.
- `flotsync_discovery_cli/`: command-line entry point for discovery components.

### Runtime, Examples, and Support

- `flotsync_replication/`: application-facing replication API and internal
  replication runtime.
- `flotsync_io_examples/`: small examples and manual acceptance tools, including
  `replicated_checklist`.
- `flotsync_utils/`: shared utility helpers and test support.

## Design Documentation

Design documentation lives under `docs/`. The generated index is the best entry
point:

```text
docs/index.md
```

Useful starting points include:

- `docs/communication_protocol_spec.md` for the high-level replication protocol
  model.
- `docs/flotsync_security_mvp.md` for the initial security model.
- `docs/route_establishment.md` for verified direct UDP route discovery.
- `docs/datagram_multipart_transfer.md` for UDPour.
- `docs/group_broadcast_queue_model.md` for GroupBroadcast queue semantics.
- `docs/flotsync_io_examples.md` and
  `docs/replicated_checklist_scenarios.md` for example workflows.

The documentation index is generated. Update the source documents and regenerate
or check the index through `scripts/okf-docs.sc`; do not edit `docs/index.md`
directly.

## Issue Tracking

This repository uses [`gitrack`](https://github.com/Bathtor/gitrack) for
Git-native issue tracking. Issue state is stored in tracked files under
`issues/`.

Common commands:

```bash
gitrack ready --json
gitrack list --json
gitrack show <ref> --json
```

The README does not maintain a separate roadmap. Current priorities,
dependencies, follow-ups, and implementation detail belong in `gitrack`.

## Build and Test

### Toolchain

The workspace currently uses nightly Rust features, so a nightly Rust toolchain
is expected. CI is the canonical source for the exact check set. Some local
checks also require:

- `buf` for protobuf formatting and linting;
- Ammonite (`amm`) for OKF-style design documentation checks.

### Common Checks

```bash
buf format --diff --exit-code
buf lint
amm scripts/okf-docs.sc check
cargo fmt --all --check
cargo clippy --workspace --all-targets --no-deps --locked -- -D warnings
cargo test --workspace --locked
```

For focused Rust work, run the relevant package tests first and broaden to the
workspace checks before review.

### Codex Web on Ubuntu

The repository includes a bootstrap script for reproducing the Linux CI-style
environment in Codex web on Ubuntu:

```bash
./scripts/setup-codex-web-ubuntu.sh
```

The script pins the local formatting and clippy toolchain and fetches locked
Cargo dependencies while network access is available.

## Examples

The examples are intended to exercise the runtime and transport stack while the
project is still evolving.

The most complete manual example is `replicated_checklist`:

```bash
cargo run -p flotsync_io_examples --bin replicated_checklist -- --help
```

See `docs/flotsync_io_examples.md` and
`docs/replicated_checklist_scenarios.md` for configuration notes and manual
multi-peer scenarios.

## Licence

This project is licensed under the MIT License. See `LICENSE.txt` for details.
