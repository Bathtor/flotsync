# FlotSync TODO Tracker

Last reviewed: 2026-02-14

## Status Legend

- `todo`: Not started.
- `in progress`: Actively being worked on.
- `blocked`: Waiting on a dependency/decision.
- `done`: Completed.

## High Priority

| ID | Status | Area | Task | Evidence |
| --- | --- | --- | --- | --- |
| DAT-001 | in progress | Data types (`any_data`) | Fix latest-value-wins register behavior so all tests pass. | Failing tests: `any_data::tests::concurrent_updates_converge_independent_of_delivery_order`, `any_data::tests::applying_same_operation_twice_is_illegal` in `flotsync_data_types/src/any_data/mod.rs:165` and `flotsync_data_types/src/any_data/mod.rs:195` |
| DISC-001 | todo | Discovery CLI | Implement non-mDNS active mode in CLI (`--active` without `--mdns`). | `flotsync_discovery_cli/src/main.rs:55`, `flotsync_discovery_cli/src/main.rs:58` |
| DISC-002 | todo | Discovery architecture | Provide Kompact interface/path for custom announcement service. | `flotsync_discovery_cli/src/main.rs:126` |
| DISC-003 | todo | Discovery mDNS | Implement public message handling for `MdnsAnnouncementComponent`. | `flotsync_discovery/src/services/mdns_announcement.rs:256` |
| DISC-004 | todo | Discovery mDNS | Implement or explicitly disable network message handling (`receive_network`). | `flotsync_discovery/src/services/mdns_announcement.rs:282` |
| DISC-005 | todo | Discovery mDNS | Implement browser module. | Empty file: `flotsync_discovery/src/services/mdns_browser.rs` |

## Medium Priority

| ID | Status | Area | Task | Evidence |
| --- | --- | --- | --- | --- |
| DAT-002 | todo | Linear data backend | Decide whether to keep and complete linked-list backend or remove it. | Disabled module note in `flotsync_data_types/src/linear_data/mod.rs:14` |
| DAT-003 | todo | Linear data backend | Implement `ids_at_pos` for linked-list backend if backend is kept. | `todo!` in `flotsync_data_types/src/linear_data/linked_list_impl.rs:151` |
| DAT-004 | todo | Coalesced linear data | Memoize right-subtree checks to avoid repeated expensive traversal. | `flotsync_data_types/src/linear_data/coalesced.rs:958` |
| DAT-005 | todo | Coalesced linear data | Validate/handle local subtree ordering in insertion path. | `flotsync_data_types/src/linear_data/coalesced.rs:1007` |
| DISC-006 | todo | Discovery UDP announcement | Add configurable socket reuse option for announcement socket setup. | `flotsync_discovery/src/services/peer_announcement.rs:62` |

## Low Priority / Cleanup

| ID | Status | Area | Task | Evidence |
| --- | --- | --- | --- | --- |
| DAT-006 | todo | Linear data cleanup | Remove or justify old commented helper in `VecLinearData` implementation. | `flotsync_data_types/src/linear_data/vec_impl.rs:83` |
| CLI-001 | todo | Discovery CLI cleanup | Remove unused `Result` import (or reintroduce async path using it). | `flotsync_discovery_cli/src/main.rs:2` |

## Notes

- The discovery feature matrix (`flotsync_discovery/test_features.sh`) currently passes.
- Workspace tests are mostly green except the two `DAT-001` failures.
