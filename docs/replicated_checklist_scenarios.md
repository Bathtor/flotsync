# Replicated Checklist Manual Scenarios

This runbook covers the manual acceptance scenarios for `flotsync-5j0.9`.

The steps use the names `alice` and `bob`. If your local config uses different member names, map
`alice` to the first terminal/machine and `bob` to the second. The checked-in examples in
`testing/` are currently named for the machines they target, but the flow is the same.

## Setup

Build or run the checklist binary from the repository root:

```bash
cargo run -p flotsync_io_examples --bin replicated_checklist -- \
  generate-keys alice ./alice-keys
cargo run -p flotsync_io_examples --bin replicated_checklist -- \
  generate-keys bob ./bob-keys
cargo run -p flotsync_io_examples --bin replicated_checklist -- run alice.toml
cargo run -p flotsync_io_examples --bin replicated_checklist -- run bob.toml
```

For a release binary:

```bash
target/release/replicated_checklist generate-keys alice ./alice-keys
target/release/replicated_checklist generate-keys bob ./bob-keys
target/release/replicated_checklist run alice.toml
target/release/replicated_checklist run bob.toml
```

The generated `public.jwks` files must be copied or otherwise made available to
the opposite peer before first run. Each config needs `store-secret-profile`,
the temporary `group-secret-password`, `local-private-jwks-path`, and
`trusted-public-jwks-paths` values. The profile selects a device-local
store-secret slot; the group password and this application-side provisioning
step are temporary for the current security MVP.

Use one terminal per peer. In each REPL, run:

```text
me
members
```

Confirm that:

- Alice prints `member: alice`, the expected config path, and Alice's local endpoint.
- Bob prints `member: bob`, the expected config path, and Bob's local endpoint.
- Both peers list the same group and the same ordered members.

The examples below use `ROW` when an item should be addressed by row UUID. Get it from `list`; the
row UUID is printed in parentheses at the end of each row. Using row UUIDs avoids accidental index
selection when earlier scenarios left extra rows visible.

## Scenario 1: Concurrent Adds

Goal: both peers can add different rows concurrently, then converge after manual syncs.

Alice:

```text
add alice concurrent add
list
```

Bob:

```text
add bob concurrent add
list
```

Exchange updates:

Alice:

```text
sync
list
```

Bob:

```text
sync
list
```

Alice:

```text
sync
list
```

Expected result:

- Bob sees both rows after Bob's `sync`.
- Alice sees both rows after Alice's second `sync`.
- `events` on each peer includes an upsert event for the remote row.

## Scenario 2: Complete vs Rename

Goal: one peer marks a row done while the other renames the same row, and both field changes survive.

Create a shared base row.

Alice:

```text
add complete rename base
sync
list
```

Bob:

```text
sync
list
```

Copy the row UUID for `complete rename base` from either `list` output as `ROW`.

Make unsynchronised concurrent edits.

Alice:

```text
complete ROW
show ROW
```

Bob:

```text
rename ROW renamed by bob while alice completed it
show ROW
```

Exchange updates:

Alice:

```text
sync
```

Bob:

```text
sync
show ROW
```

Alice:

```text
sync
show ROW
```

Expected result:

- Both peers show the renamed text.
- Both peers show `status: done`.
- The row remains visible on both peers.

## Scenario 3: Delete vs Edit

Goal: one peer deletes a row while the other edits it, and the edit must not resurrect the deleted row.

Create a shared base row.

Alice:

```text
add delete edit base
sync
list
```

Bob:

```text
sync
list
```

Copy the row UUID for `delete edit base` as `ROW`.

Make unsynchronised concurrent edits.

Alice:

```text
delete ROW
list
```

Bob:

```text
rename ROW bob edited a row that alice deleted
show ROW
```

Exchange updates:

Alice:

```text
sync
list
```

Bob:

```text
sync
list
events 5
```

Alice:

```text
sync
list
events 5
```

Expected result:

- Bob may briefly see the local rename before syncing, because it is still in Bob's local working
  set.
- After Bob syncs, Bob no longer sees the row.
- After Alice syncs again, Alice still does not see the row.
- A later edit/update for `ROW` must not make the row visible again.

## Scenario 4: Restart Keeps Durable Runtime State

Goal: a peer can stop and restart with the same store path and continue participating in the same
configured group.

Start from two running peers with the same config and stores.

Alice:

```text
add restart durable state base
sync
quit
```

Restart Alice with the same config file and store path.

Alice:

```text
me
members
add alice after restart
sync
```

Bob:

```text
sync
list
```

Expected result:

- Alice restarts without static-group mismatch or store initialisation errors.
- Alice reports the same group id, member identity, config path, and store path after restart.
- Bob receives `alice after restart` after Bob syncs.
- The current checklist REPL is an in-process working set; this scenario checks durable runtime
  state and continued replication, not automatic UI rehydration of previously visible checklist
  rows.

## Scenario 5: Manual Route Configuration

Goal: route configuration is explicit, visible in the config files, and testable without claiming
automatic discovery exists.

Before starting the peers, inspect both TOML files.

Alice config:

```toml
[flotsync.replication.runtime]
local-endpoint-bind-addr = "ALICE_BIND_ADDR"

[[flotsync.replication.runtime.static-peer-routes]]
name = "bob"
protocol = "udp"
ip = "BOB_IP"
port = BOB_PORT
```

Bob config:

```toml
[flotsync.replication.runtime]
local-endpoint-bind-addr = "BOB_BIND_ADDR"

[[flotsync.replication.runtime.static-peer-routes]]
name = "alice"
protocol = "udp"
ip = "ALICE_IP"
port = ALICE_PORT
```

Start both peers and verify local runtime state.

Alice:

```text
me
members
add route check from alice
sync
```

Bob:

```text
me
members
sync
list
add route check from bob
sync
```

Alice:

```text
sync
list
```

Expected result:

- Each peer prints its configured local endpoint from `me`.
- Each peer lists the same static ordered members from `members`.
- Alice-to-Bob and Bob-to-Alice item exchange works only because the static peer routes point at
  the correct remote IP and port.
- There is no discovery command, no discovered peer list, and no expectation that peers find each
  other without the static routes.
