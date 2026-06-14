# FlotSync

FlotSync is a Rust workspace for decentralized peer-to-peer group synchronization.
It currently contains reusable crates for version tracking, discovery, wire messages, and replicated data types.

## Current Status (as of 2026-03-05)

| Area | Status | Notes |
| --- | --- | --- |
| Core versioning and identifiers | Implemented | `flotsync_core` provides version vectors, group membership identifiers, and ordering logic. |
| Datamodel wire transport | Implemented (operations/snapshots) | `flotsync_messages` supports proto codecs for schema operations and snapshots via generated `buffa` bindings. |
| Schema definition transport | Planned | Schema definitions are still agreed out-of-band and need first-class transport support. |
| In-memory schema datamodel | In progress | Local table operations exist; inbound operation application remains to be added. |
| Discovery stack | In progress | Core components exist, with remaining open work on browser/public handling and non-mDNS active mode. |
| Persistence abstraction | Planned | Storage interface for schema operations and snapshots is still pending. |

## Issue Tracking

This repository uses `bd` (beads) as the source of truth for tasks and dependencies.

- Check ready work: `bd ready --json`
- Inspect an issue: `bd show <issue-id> --json`
- Create new work: `bd create "Title" --description "..." -t task|feature|bug --json`

## Workspace Layout

- `flotsync_core/`: Version vectors, group membership identifiers, and happened-before logic.
- `flotsync_messages/`: Generated protobuf message bindings.
- `messages/proto/`: Source `.proto` definitions organised by package and version.
- `flotsync_discovery/`: Discovery services (mDNS, peer announcements, and route establishment building blocks).
- `flotsync_discovery_cli/`: CLI entrypoint for discovery components.
- `flotsync_data_types/`: Replicated data structures (text and latest-value-wins register).
- `flotsync_utils/`: Shared utility helpers/macros used by other crates.

## Build and Test

### Toolchain

This workspace currently uses nightly-only features in source code (for example `#![feature(...)]` in
`flotsync_core/src/lib.rs` and `flotsync_data_types/src/lib.rs`), so a nightly Rust toolchain is currently expected.
The Linux formatting and clippy checks are pinned to `nightly-2026-04-11` so CI matches local
development for those tools, while the test workflows continue to follow floating `nightly`.

### Commands

Run the full workspace tests:

```bash
cargo test --workspace
```

Check the OKF-style design documentation index and metadata:

```bash
amm scripts/okf-docs.sc check
```

The OKF docs check requires Ammonite (`amm`). GitHub Actions installs it with
Coursier before running the check.

### Codex Web on Ubuntu

If you want to reproduce the Linux CI environment from Codex web on Ubuntu, bootstrap the host with:

```bash
./scripts/setup-codex-web-ubuntu.sh
```

That setup pins the toolchain to `nightly-2026-04-11`, matching the local fmt/clippy toolchain on
this repo, and it also runs `cargo fetch --locked` so the workspace dependencies are cached before
the Codex web environment loses internet access.

After the script finishes, install Ammonite if it is not already available, then
run the same checks as `pr-linux`:

```bash
export CARGO_NET_GIT_FETCH_WITH_CLI=true
buf format --diff --exit-code
buf lint
amm scripts/okf-docs.sc check
cargo fmt --all --check
cargo clippy --workspace --all-targets --no-deps --locked -- -D warnings
cargo test --workspace --locked
```

## Roadmap

### Near-Term

1. Apply inbound schema operations to `InMemoryData` with thorough test coverage.
2. Define a storage abstraction for persisting and retrieving schema operations/snapshots.
3. Close outstanding discovery P1 gaps (non-mDNS active mode, mDNS public/network handling, browser module).

### Medium-Term

1. Add transport serialization/deserialization for schema definitions.
2. Design database-level APIs and replication group semantics.
3. Tighten convergence/integration coverage for multi-replica, multi-schema scenarios.

## Repository Workflow

This repository uses Jujutsu (`jj`) for history/workflow.

## License

This project is licensed under the MIT License. See `LICENSE.txt` for details.
