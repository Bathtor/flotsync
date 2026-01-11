# FlotSync

FlotSync is a Rust workspace for decentralized peer-to-peer group synchronization.
It currently contains reusable crates for version tracking, discovery, wire messages, and replicated data types.

## Current Status (as of 2026-02-14)

| Area | Status | Notes |
| --- | --- | --- |
| Core versioning and identifiers | Done | `flotsync_core` is implemented and tests pass. |
| Message schemas and codegen | Done | `messages/proto` + `flotsync_messages` are in use and tested. |
| Discovery service builds | Done | `flotsync_discovery/test_features.sh` passes all listed feature combinations. |
| Text CRDT data type | Mostly done | Text diff/apply and convergence tests are present and largely passing. |
| Latest-value-wins register (`any_data`) | In progress | Two tests currently fail in `flotsync_data_types`. |
| Discovery CLI and browser surface | TODO | Non-mDNS path and browser pieces are not implemented yet. |

The detailed, line-referenced task list is tracked in [`docs/TODO_TRACKER.md`](docs/TODO_TRACKER.md).

## Workspace Layout

- `flotsync_core/`: Version vectors, group membership identifiers, and happened-before logic.
- `flotsync_messages/`: Generated protobuf message bindings.
- `messages/proto/`: Source `.proto` definitions for discovery and versions.
- `flotsync_discovery/`: Discovery services (mDNS and custom UDP announcement building blocks).
- `flotsync_discovery_cli/`: CLI entrypoint for discovery components.
- `flotsync_data_types/`: Replicated data structures (text and latest-value-wins register).
- `flotsync_utils/`: Shared utility helpers/macros used by other crates.

## Build and Test

### Toolchain

This workspace currently uses nightly-only features in source code (for example `#![feature(...)]` in
`flotsync_core/src/lib.rs` and `flotsync_data_types/src/lib.rs`), so a nightly Rust toolchain is currently expected.

### Commands

Run the full workspace tests:

```bash
cargo test --workspace
```

Current known result: the workspace compiles, but there are two failing tests in `flotsync_data_types`:

- `any_data::tests::concurrent_updates_converge_independent_of_delivery_order`
- `any_data::tests::applying_same_operation_twice_is_illegal`

Run discovery feature-matrix checks:

```bash
cd flotsync_discovery
./test_features.sh
```

## Roadmap

### Near-Term

1. Stabilize `LinearLatestValueWins` semantics and make the failing tests pass.
2. Finish discovery CLI support for non-mDNS/custom announcement mode.
3. Implement missing mDNS public/network handling and browser behavior.

### Medium-Term

1. Tighten data type convergence coverage with broader multi-replica scenarios.
2. Improve discovery integration tests beyond build/clippy checks.
3. Evaluate whether to keep, complete, or remove the alternate linked-list linear-data backend.

## Repository Workflow

This repository uses Jujutsu (`jj`) for history/workflow.

## License

This project is licensed under the MIT License. See `LICENSE.txt` for details.
