---
type: Scenario
title: Replicated Checklist Manual Scenarios
description: Captures manual replicated-checklist scenarios for exercising multi-peer behaviour.
status: draft
---

# Replicated Checklist Manual Scenarios

This runbook covers manual acceptance scenarios for the replicated-checklist
example application.

The steps use the names `alice` and `bob`. If your local config uses different member names, map
`alice` to the first terminal/machine and `bob` to the second. The checked-in examples in
`testing/` are currently named for the machines they target, but the flow is the same.

The normal custom UDP discovery path does not require
`flotsync.replication.runtime.static-peer-routes` or an explicit
`flotsync.replication.runtime.local-endpoint-bind-addr`. Without an explicit local endpoint, the
runtime binds an ephemeral wildcard UDP socket and advertises selected concrete local interface
endpoints. The no-static route scenarios require a network that forwards the peer-announcement UDP
traffic between the peers.

## Setup

Start each peer from the repository root:

```bash
cargo run -p flotsync_replication_examples --bin replicated_checklist -- run alice.toml
cargo run -p flotsync_replication_examples --bin replicated_checklist -- run bob.toml
```

For a release binary:

```bash
target/release/replicated_checklist run alice.toml
target/release/replicated_checklist run bob.toml
```

On first use, each process asks whether it should create the store and initialise
local identity keys. Answer `y` or `yes` to continue. Declining exits without
creating the store or its setup state. An existing unprovisioned store uses the
same setup dialogue before runtime startup.

Inside each running REPL, print the local bundle:

```text
keys export-local
```

Copy the printed bundle to the opposite peer. Alice can assess and trust Bob as
follows; Bob performs the corresponding commands with Alice's bundle:

```text
keys inspect BOB_PUBLIC_BUNDLE
keys trust bob BOB_PUBLIC_BUNDLE
```

Trust prints the assessment and records feedback only after an explicit `y` or
`yes`. To block key material, supply its public bundle rather than transcribing
its fingerprint:

```text
keys block UNTRUSTED_PUBLIC_BUNDLE
```

Before creating a group, inspect route establishment in each REPL:

```text
peers
```

This is a one-shot snapshot. It prints the local and advertised UDP endpoints,
then each known remote route with its source, phase, expected members,
cryptographically identified members, and currently reachable members. An
identified peer with `shared groups=0` confirms that the route and peer
authentication work before the peers share a replication group. A route that
remains `probing` or becomes `stale` points to route establishment rather than
group membership; no matching route usually points to discovery or static-route
configuration.

Each config needs `store-path` and `store-secret-profile` in the
replicated-checklist section. The profile selects a device-local store-secret
slot. On first run, the application asks for the local member identity and
stores it together with its new key material.

<!-- TODO(flotsync-lsi8): Remove this unsafe headless workaround note once the
proper local store-secret backend exists. -->

On headless Linux machines without a working Secret Service keyring, use an
explicitly unsafe profile such as `unsafe:raspberrypi`. This skips OS keyring
storage and derives the local store secret from the profile string. Changing
that profile later makes existing local security material unreadable; delete the
example store and start fresh if you change it.

Create the first group on Alice. The creator is inserted automatically at
member position 0; enter one additional member identity per prompt and submit a
blank line to finish the member list:

```text
group create shared checklist
additional member id (blank to finish)> bob
additional member id (blank to finish)>
Create this group and send invitations? [y/N] y
```

Bob can inspect and accept the listener-delivered invitation:

```text
group invitations
group accept 1
```

Group creation and acceptance do not implicitly select a default. In each
terminal, inspect the registry and explicitly select the group by its unique
name or UUID:

```text
me
group list
group default shared checklist
members
```

Confirm that:

- Alice prints `member: alice` and the expected config path.
- Bob prints `member: bob` and the expected config path.
- Both peers list the same group, display name, lifecycle, and ordered members.
- `me` reports `shared checklist` as the process-local default after selection.

For a fresh zero-group store, `me` reports `default group: none`; key and group
registry commands remain available. `add` without a default creates a
process-local item. Such items are deliberately skipped by `sync` and disappear
when the process exits.

The examples below use `ROW` when an item can be addressed by a globally unique
row UUID. `list` prints a canonical qualified reference in parentheses at the
end of each row. A bare UUID remains convenient while it is unique; use the
qualified reference once the same UUID exists in several associations. Either
form avoids accidental position selection when earlier scenarios left extra
rows visible.

## Multi-group Defaults and Synchronisation

The default controls only where subsequent `add` commands place new items.
Existing rows retain their own group association, and `sync` attempts every
dirty real group in UUID order regardless of the current default. This keeps
group selection separate from replication progress.

With two writable groups named `shared checklist` and `work`, one peer can stage
changes in both before a single sync:

```text
group default shared checklist
add buy tea
group default work
add prepare status report
group clear-default
sync
```

Expected result:

- The sync report contains one outcome for each dirty real group.
- Both groups are published even though the default was cleared before `sync`.
- A failed group remains dirty and is named in the report, while later groups
  are still attempted.
- Listener batches remain pending when any group publication fails; rerunning
  `sync` retries the failed group before listener changes are applied.
- Process-local items are counted in the report but never submitted.

If a selected group becomes read-only or closed after an accepted membership
change, the next registry refresh follows its successor chain. The first open
successor becomes the new default; if no open successor is available, the
default is cleared and the REPL reports why.

## Qualified References and Item Transfer

Every item keeps its row UUID when copied or moved. Because the same UUID may
therefore appear in several groups, `list`, `show`, and `events` expose a
canonical qualified reference:

- process-local items use `local/ROW_UUID`;
- a uniquely named group without whitespace uses `GROUP_NAME/ROW_UUID`;
- ambiguous names, the reserved name `local`, and names containing whitespace
  fall back to `GROUP_UUID/ROW_UUID`.

Commands still accept a one-based list position or a bare UUID when that UUID
is unique. An ambiguous bare UUID is rejected and the error lists its canonical
qualified candidates. Qualified source references split at the final `/`, so a
group name may itself contain `/` as long as it contains no whitespace.

Editing uses item-first syntax. Replace `SOURCE` below with a list position,
bare UUID, or canonical qualified reference. Target group names are the
remaining words and may contain spaces:

```text
edit SOURCE note
edit SOURCE copy work
edit SOURCE move shared checklist
```

`copy` stages a complete target row under the same UUID and leaves the source
unchanged. `move` stages the same target row and immediately removes the source
from the visible working set. A previously replicated source becomes an
ordinary tombstone; a process-local or never-published source simply
disappears.

Both target upserts and source tombstones are published by the next ordinary
all-group `sync` in group UUID order. This example intentionally does not make
move target-first or atomic: a source tombstone can publish before a target
upsert that later fails. During the same process the complete target remains
dirty and another `sync` retries it. Replication history retains tombstoned row
data, but this slice does not automate recovery after restarting the checklist.

An existing target with identical contents is an idempotent success. A target
with different contents is rejected and never overwritten.

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
persisted groups.

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
group list
group default shared checklist
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
- Alice reports the same group ids, member identity, config path, and store path after restart.
- Alice starts without a default and explicitly selects one again because the
  default is process-local session state.
- Stored checklist snapshots repopulate the readable group rows in Alice's
  working set.
- Bob receives `alice after restart` after Bob syncs.

## Scenario 5: Custom UDP Discovery Without Static Routes

Goal: two peers discover usable direct UDP routes through peer announcements and route
establishment, without preconfigured peer endpoints.

Before starting the peers, inspect both TOML files and confirm neither file contains
`flotsync.replication.runtime.static-peer-routes`. Leave
`flotsync.replication.runtime.local-endpoint-bind-addr` unset unless this manual run needs a fixed
local address or port for diagnostics.

Start both peers and verify local runtime state.

Alice:

```text
me
members
add custom discovery check from alice
sync
```

Bob:

```text
me
members
sync
list
add custom discovery check from bob
sync
```

Alice:

```text
sync
list
```

Expected result:

- Each peer prints its member and config path from `me`.
- Each peer lists the same static ordered members from `members`.
- Alice-to-Bob and Bob-to-Alice item exchange works without any static peer route entries.
- Routes become usable only after route establishment verifies a discovered peer-announcement route
  with a signed introduction.

## Scenario 6: Static Route Hints

Goal: route hints are explicit, visible in the config files, and verified before they become usable
replication routes.

Before starting the peers, inspect both TOML files. This scenario pins the local endpoint only to
make the static-route example addresses stable; use an address currently assigned to each host.

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

- Each peer prints its member and config path from `me`.
- Each peer lists the same static ordered members from `members`.
- Alice-to-Bob and Bob-to-Alice item exchange works only after route establishment verifies the
  hinted remote IP and port with a signed introduction.
- Static peer routes are hints, not direct always-available routes. They are useful as a fallback or
  diagnostic input when the local network does not forward peer-announcement traffic reliably.

## Scenario 7: Group Replacement Reconciliation

Goal: a membership replacement changes the active group atomically while preserving deliberate
local edits which the accepted replacement did not include.

This scenario needs a replacement invitation produced by a migration-capable peer or test harness;
the checklist example itself rejects migration proposals. Before accepting the invitation, leave an
unsynchronised edit on a row from the old group. Then accept the invitation or run `sync`, depending
on which command receives the replacement event.

The checklist compares the complete old and successor views as one event. It remaps identical rows,
accepts unambiguous successor changes, preserves unpublished local insertions, and safely combines
dirty updates whose changed fields do not overlap. It pauses for input only when choosing a result
requires application judgement.

The `events` command remains a low-level diagnostic: it records the raw row transition delivered by
the framework. A replacement entry therefore shows the successor candidate before application-side
reconciliation, not an edited or locally retained result selected in the dialogue.

For an ambiguous text change, the dialogue shows both group-scoped identities, both row candidates,
the differing fields, and the available cut evidence. Choose one of:

```text
resolution [accept local/accept remote/edit local/edit remote]> edit local
edit [text/note/tags/status/priority/accept]> text reconciled checklist text
edit [text/note/tags/status/priority/accept]> accept
```

`edit local` and `edit remote` select the corresponding candidate as the editor base. The editor
accepts `text`, `note`, `tags`, `status`, and `priority` commands. If a side represents deletion,
that side can be accepted but cannot be edited; the dialogue explains this and asks again. Invalid
choices also ask again.

Expected result:

- The old-to-new transition is not partially visible, even when the framework streams several row
  pages.
- `accept remote` leaves the successor row clean.
- `accept local`, an edited result, or an accepted local deletion remains dirty in the successor
  group when it differs from the remote result.
- The command reports the number of retained reconciliation changes, names the affected successor
  groups, and instructs the user to run `sync` again.
- The wizard does not publish recursively. Run `sync` once more to publish the retained choices.
- If the process exits or input ends before all choices are made, no partial replacement is applied;
  restarting reloads the currently active group state and any unfinished choices are lost.
