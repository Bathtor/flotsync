---
type: State Machine
title: Replication Runtime Flow Contracts
description: Defines runtime flow ownership, observable transaction boundaries, listener ordering, and selective admission for the component decomposition.
status: draft
applies_to:
  - flotsync_replication
source_of_truth:
  - ../flotsync_replication/src/runtime/component/mod.rs
  - ../flotsync_replication/src/runtime/component/listeners.rs
  - ../flotsync_replication/src/runtime/component/activation_provider.rs
  - ../flotsync_replication/src/runtime/group_state.rs
  - ../flotsync_replication/src/runtime/summary_request_manager.rs
  - ../flotsync_replication/src/runtime/host/topology.rs
  - ../flotsync_replication/src/api/store.rs
  - ../flotsync_replication/src/api/groups.rs
depends_on:
  - inline_migration_policy_flow.md
  - group_replacement_row_lineage.md
  - single_recipient_durable_delivery.md
tracked_by:
  - flotsync-zop
  - flotsync-zop.01
---

# Replication Runtime Flow Contracts

## Scope and implementation boundary

This document records current behaviour that the component decomposition must
preserve, and the agreed ownership/scheduling contract for its concurrent end
state. **Phase ordering below is implemented; owned partitions and the selective
mailbox are target contracts, not existing runtime machinery.** The initial FSM
driver still uses `Handled::block_on`. No total FIFO across Kompact input sources
is assumed. API shapes, wire protocols and the separate summary/catch-up managers
remain unchanged. The [inline policy flow](inline_migration_policy_flow.md) owns
group validation/policy rules; [row lineage](group_replacement_row_lineage.md)
owns replacement evidence; this document owns their local execution boundaries.

## State and field ownership

A finite **flow** owns its phase data, reply/processed obligations and partitions
for a complete **consistency interval**. Synchronous borrowing at each transition
does not establish that ownership. The target registry distinguishes
`Available(value)`, `OwnedBy(flow_id)` and `Faulted`; the value lives in the owning
flow or its in-flight effect, without another mutable alias. A phase may hold
temporarily inconsistent private values. It must explicitly state which consumers
are excluded and the invariant required before returning those values. Completion
or verified cleanup restores availability; dropping a future does not prove it.

There is currently no persistent mutable per-group row/vector cache in this
component. `LoadedGroupMeta`, `PendingUpdateSet`, dataset state, prepared payloads
and observations are operation locals; stored group/work records are authoritative.
The partition design must move those values into typed FSM states, rather than
introduce a second mutable database mirror. `Group(g)` denotes the owned interval
for that group's operation state and mutation/listener sequencing; it cannot
prevent independent components from reading or writing database records.

| Actual component field | Access contract and aliases |
| --- | --- |
| `ctx` | Coordinator scheduling, logging, actor reference and startup configuration; never give a child unrestricted component access. |
| `local_member`, `config`, `max_inline_bootstrap_public_key_bundles` | Immutable identity/policy inputs after startup resolves the inline limit. |
| `store` | Shared service capability; describe canonical store identity, logical record scope and read/write mode per effect. The `Arc` is not an exclusive database partition. |
| `security` | Shared service; its `SecurityStore` wraps the same replication store. Preparation can read/write key bindings and permission evidence independently of the component's `store` field. |
| `group_memberships` | `SharedGroupState` publishes an immutable whole-map view. Writer ownership and published-reader capabilities must be distinct. |
| `listener` | Shared application service; `ListenerLane` serialises callback invocation/completion. An application can retain service/provider aliases outside the component. |
| `group_broadcast`, `reliable_delivery` | Coordinator-owned output ports; flows describe submissions as effects, not borrowed ports across suspension. |
| `summary_request_manager`, `catch_up_manager` | Shared actor destinations; their independent state is not moved into this component's flows. |

The following use sets include transitive helpers; all other fields in the table
are unused by that domain, except `ctx` for infrastructure. Inputs common to a
domain remain immutable; their use does not by itself create an exclusion claim.

| Domain | Fields used | Target ownership interval |
| --- | --- | --- |
| Publish | `local_member`, `store`, `group_memberships`, `listener`, `group_broadcast`, `catch_up_manager` | `Group(g)` through callback and reply; `ListenerLane` and bounded event capacity reserved before event-producing work. |
| Single/batch apply | `local_member`, `store`, `group_memberships`, `listener`, `catch_up_manager` | `Group(g)` through all callbacks/observations; batch retains it and the listener lane for the entire batch. |
| Inbound decision/policy | `local_member`, `config`, `security`, `store`, `group_memberships`, `listener`, `catch_up_manager` | Target group plus hosted predecessor and `DecisionSet(predecessor)` for competing replacements; writer/lane where publication/callback follows. |
| Decision response/activation | `local_member`, `store`, `group_memberships`, `listener`, `catch_up_manager` | Same group/direction union through activation/provider hand-off, with writer and lane. |
| Creation | `local_member`, `security`, `max_inline_bootstrap_public_key_bundles`, `store`, `group_memberships`, `listener`, `reliable_delivery` | New `Group(g)`, writer, lane and output obligations through notification, invitations and reply. |
| Membership change | Creation fields plus `catch_up_manager` | `Group(old)`, `Group(new)`, `DecisionSet(old)`, writer and lane through preparation, both commits, dispatch, callback and reply. |
| Recovery | `local_member`, `store`, `group_memberships`, `listener`, plus `max_inline_bootstrap_public_key_bundles` initialisation | Lifecycle excludes normal starts; each resumed activation uses the same group/writer/lane union. |
| API/inbound summary | `store`, `group_memberships`, `local_member`, `summary_request_manager`, `group_broadcast`, `reliable_delivery` as applicable | Short current-group read interval; release before waiting for a remote manager response. |
| Observed summary | `store`, `local_member`, `catch_up_manager`; ingress also reads `group_memberships` | Advisory snapshot inspection, explicitly stale-tolerant; no private group mutation. |
| Security APIs | `security` | Owned request/reply; declared key/trust/block record access, not a blanket store lock. Local public bundle access is synchronous. |

Take the full required set atomically at admission, never hold one partition
while waiting to acquire another. Tables use a conservative union. A writer
subsection can end after publication; narrowing admission across subsections
requires a stable checkpoint, re-admission and transactional revalidation.
`DecisionSet(old)` covers all competitors, not just one chosen target. Pending
human choice lives in stored work between finite flows, with no retained claims.

## Database and published views

Every commit must be valid for independent database consumers. Flow ownership
can hide private phase inconsistency, but cannot hide committed inconsistency
or revoke an existing `Arc`. `SecurityStore`, catch-up, reliable delivery and
application/provider store handles may alias the same backend. Store descriptors
record these aliases and record access; they are not a generic reader/writer gate.
The [store traits](../flotsync_replication/src/api/store.rs) promise read-your-own-
writes, rollback of uncommitted writes on drop, and consistent read pagination.
They do not fully specify cross-transaction lost-update/conflict prevention.
[SQLite](../flotsync_replication/src/store/sqlite/mod.rs) starts writes with
`BEGIN IMMEDIATE`, reads with `BEGIN`, and uses an eight-connection pool. Its
single writer can limit transaction overlap even when flows have disjoint claims.
Do not infer that another backend supplies identical isolation, or that merely
creating a read transaction establishes its snapshot before the first read.

Each new write phase must revalidate its actual premises *inside* its transaction:
group lifecycle, membership/schema/key definition, pending-work identity, chosen
successor and accepted cut, plus row/update/version evidence where used. In
particular, local replacement preparation uses a separate read transaction from
acceptance, and setup validation precedes persistence. Those are explicit audit
boundaries for `flotsync-zop.16`; this document does not claim the current code
already repeats every check. Transaction errors retain their operation versus
transaction classification; a failed reply/commit acknowledgement is not rollback
evidence, and committed effects must not be blindly retried.

`RuntimeGroupStateSnapshot` contains **id, name, members, resolved schema and
lifecycle**, not rows or version vectors. Keep whole-map replacement. A flow
owns `ProjectionWriter` from before the snapshot-building read through commit
and `SharedGroupState::replace`, preventing a stale full map overwriting a newer
one. `DatabaseCommittedButProjectionPending` is a valid private phase. Existing
application snapshots remain coherent but may be stale; current-state operations
must obtain the relevant capability and use transactional evidence. Delivery,
security and manager readers must be audited individually before overlap is
enabled. Group deltas could permit more writer overlap, at the cost of new merge
rules; they are an alternative, not a prerequisite.

## Phase, transaction and restoration tables

In the tables, `W` is one write transaction, `R` one retained read transaction.
Uncommitted failure rolls back writes; committed failure preserves the checkpoint.
Each row retains its domain's group/order claims from the use-set table unless
explicitly released. Prepared data may disagree with the last published view
only inside that owned interval. No write transaction spans a listener await.

### Publication and inbound application

| Flow/phase → next event or effect | Owned phase data and database scope | Visibility, exclusion and terminal restoration |
| --- | --- | --- |
| Publish validate → `publish_changes_transactionally` | Request/token, touched rows, schema; W reloads writable group and validates token/frontier. Builds row patches, update payload and next vector. | Exclude same-group mutation/listener work. Private dataset/vector changes are not externally visible before commit; failure leaves the previous committed state. |
| Publish W commit → prepared publish | W writes row state, applied update and group vector atomically. | Independent readers can consume the committed result. No projection publication is needed for vector-only progress. |
| Publish committed → submit → listener completion → reply | Prepared bytes, changes/token, reply obligation; broadcast submission then catch-up availability precede callback. | Success returns receipt after callback. Callback failure reports the existing API error while preserving commit/submission; restore group consistency, never describe this as rollback or automatic retry. |
| Single apply validate → `persist_and_apply_update` | Sender/forwarder authority, group/schema, incoming update; W reloads group/lifecycle and stored payload, builds `PendingUpdateSet` and ready-chain plan. | Duplicates/post-cut updates use their existing explicit outcomes. Causally blocked payloads can commit without application changes; they need not await a future incoming update while holding a flow. |
| Single apply W commit → observe → callbacks | W appends payload, applies every newly-ready successor's row patches, marks applied records and advances vector atomically. Retains ordered event batches and needed/available ranges trimmed to the lifecycle's final cut. | Chain failure rolls back the whole transaction. On success, catch-up observations precede callbacks; ordinary empty batches are filtered. Classified inbound errors retain drop/fault policy. |
| Batch item commit → item callback → next input | Owned input cursor and accumulated catch-up ranges; **one W per input**, each containing its ready chain. | The first item's callback must finish before the second transaction begins. Exclude same-group operations throughout the batch; preceding commits/notifications survive a later failure. |
| Batch finish/fail → flush observations → terminal | Flush both accumulated availability and needs, including on transaction or listener failure. | Restore group/order claims only after these obligations are handled. Callback invocation remains contiguous in the listener lane. |

### Pending decisions and activation

| Flow/phase → next event or effect | Owned phase data and database scope | Visibility, exclusion and terminal restoration |
| --- | --- | --- |
| Inbound classify → verify setup → policy | Invitation/proposal, authenticated envelope, private setup and processed promise; R reads lifecycle/material/permissions, security validates/decrypts setup. | Exclude competing predecessor/target direction work. Prepared material may be incomplete privately; current active views remain valid. Do not expose setup secrets to the decision listener. |
| Reject policy → W cleanup → complete processed | Remove matching pending decisions/activations and inactive material. | Active state remains intact; success completes processed only after the handler succeeds. Failure retains its classified delivery handling. |
| Ask listener → W material+decision commit → callback | Verified inactive material and unresolved work committed together; listener receives responder after commit. | Failure accepting the event leaves pending work for restart. On successful bounded hand-off, complete processed and end the flow; a later human response starts new work. |
| Auto-accept or response → W acceptance commit → activation | W stores accepted activation. Hosted migration marks source read-only at accepted cut and removes undecided competitors/material; response paths rebuild/publish full map. | Checkpoint is resumable without asking again. Reserve predecessor, target and decision-set together. Unsupported Metadata response removes work/material and returns the existing unsupported error; a stale matching response is idempotent after fresh lookup. |
| Accepted replay → W metadata refresh → complete processed | Validate accepted direction/material; refresh metadata without reopening choice or reusing the consumed initial snapshot. Inactive accepted targets retain activation work. | Active refresh owns writer through rebuild/commit/publication. Conflicting direction is rejected; restore only after publication where required. |
| Activation → `activate_pending_group_record` W | Reload material and validate members/schema; activate target, embed Empty/Inline rows, close hosted predecessor at chosen cut, remove activation work, build full map and token. | Those changes are one atomic commit. Failure before commit preserves acceptance/read-only checkpoint for restart. No successor is partially exposed. |
| Activation committed → publish → open provider R | Own snapshot/token/lineage and target mutation interval; publish full map, then acquire `StoreActivationRowProvider`. | Failure opening R is unrecoverable: pending activation has been removed, so restart/application resynchronisation is required. Do not release a faulted partition as healthy. |
| Provider → activation callback → caller continuation | Provider owns R through exhaustion/release/drop; creation has ordinary rows, replacement uses hosted or unavailable predecessor evidence. | Empty activation still emits a callback. Reserve lane before opening a retained provider if its reader can impede a queued writer. Retain group interval through callback initially; validate actual read cut/token before any later relaxation. |

### Local orchestration and recovery

| Flow/phase → next event or effect | Owned phase data and database scope | Visibility, exclusion and terminal restoration |
| --- | --- | --- |
| Creation prepare → setup/key preparation | Canonical members, schema, generated group identity, sealed setup and invitation bytes; security key/permission reads may touch the same backend. | New-group interval excludes concurrent installation of that identity. Preparation failure returns existing typed API error without a created group. |
| Creation → `store_new_replication_group` W → publish | Insert/validate complete group record; read full active record set and build map in W; commit, then publish with writer ownership. | Commit is independently readable and map is replaced coherently; failure before commit leaves no new group. |
| Creation published → empty callback → invitations → reply | Initial group token, invitation bytes, output/reply obligations; no retained provider transaction for this local empty callback. | **Listener precedes invitations**, unlike publish/replacement. Callback error leaves created group but suppresses invitation submission on this path; return existing API error and restore coherent local state. |
| Membership prepare → stage acceptance | R loads writable predecessor/cut/schema and builds inline row snapshot, then releases; prepare keys/setup and dispatch payload. W ensures target material and commits accepted direction/read-only predecessor; publish, notify finalisation. | Own predecessor/target/decision-set union; prepared snapshot can be private while old view is published. Acceptance must revalidate R-derived premises for concurrent execution. After acceptance failure, distinguish uncommitted failure from an acknowledged/uncertain checkpoint. |
| Membership accepted → common activation → dispatch → callback → reply | Reuse activation/provider above, then operation-specific reliable dispatch. | **Dispatch follows activation and precedes listener**. Pre-activation commit failure leaves accepted work; post-commit provider-open failure faults. Callback error preserves committed replacement/dispatch and returns existing API error. |
| Startup resolve limit → replay decisions | `on_start` resolves inline limit; R loads pending decisions then releases before callbacks, grouping competing proposals by predecessor. | Host first reconciles application state and installs real listener before starting runtime logic. Replay callback failure retains existing startup fault; stored decisions remain. |
| Recovery decisions replayed → resume activations → Running | R loads accepted activations then releases; process each through common activation and listener machinery. | Do not re-ask accepted work. Current `block_on` waits for the whole sequence; target lifecycle admits no new domain starts until recovery succeeds. Faults use existing Kompact startup semantics. |

## Entry-point inventory

Names refer to [component/mod.rs](../flotsync_replication/src/runtime/component/mod.rs).
All ten production blocking paths and four spawned paths are represented here.

| Input / handler | Current driver | Transaction, output and completion obligation |
| --- | --- | --- |
| API PublishChanges / `handle_publish_changes` | `block_on` | Publication table; API reply after listener. |
| Group Update / `handle_update` | `block_on` | Single-apply table; no reliable processed promise on group delivery. |
| Group UpdateBatch / `handle_update_batch` | `block_on` | Batch table; notification/observation obligations on partial failure. |
| Reliable invitation/proposal / `handle_inbound_pending_group_decision` | `block_on` | Decision/activation tables; complete reliable processed promise only after successful installation/notification. |
| API/responder / `handle_pending_group_decision_response` | `block_on` | Response and optional activation; API reply after resulting callback, or idempotent/error disposition. |
| API CreateGroup / `handle_create_group` | `block_on` | Creation table; reply after invitations are submitted. |
| API ChangeGroupMembership / `handle_change_group_membership` | `block_on` | Membership table; reply after activation/dispatch/callback. |
| API RequestSummary / `handle_request_summary` | `block_on` | R loads group/vector, releases, checks readability; local reply or forwards original ask to SummaryRequestManager. Remote timeout/reply belongs to that manager. |
| Reliable/group SummaryRequest / `handle_inbound_summary_request` | `block_on` | R builds summary, releases; reliable submission then processed completion, or best-effort group submission. Error before success does not complete processed. |
| Component lifecycle / `on_start` | `block_on` | Recovery table; startup failure faults. `on_stop`/`on_kill` currently return OK without flow cleanup logic. |
| API AssessPublicKeyBundle / `handle_assess_public_key_bundle` | `spawn_local` | Security service reads evidence or stores candidate bindings according to request mode; reply with existing store-error mapping. |
| API RecordPublicKeyBundleFeedback / `handle_record_public_key_bundle_feedback` | `spawn_local` | W updates key/trust/block feedback; API reply after service returns. |
| API KnownMemberKeys / `handle_known_member_keys` | `spawn_local` | Security report R; API reply after service returns. |
| Reliable/group Summary / `handle_observed_summary` | `spawn_local` | Advisory R inspects vector/pending ids, then sends catch-up observations; unknown hosted group is ignored. Reliable processed completion belongs to the separately connected SummaryRequestManager after correlation handling, not this advisory flow. |

Synchronous paths: local public bundle replies from loaded security; ingress
decodes/validates reliable scope and group message kinds against a membership
snapshot; group NeedRange is handled by CatchUpManager, so this component returns
OK. Unexpected message kinds use classified errors, not successful processing.
`group_state`, diagnostics/peer-route queries and shutdown are handle/host paths,
not component domain messages. Test-only blocking paths are install-group, apply-
update, apply-batch and apply-pending-group; read-group-token is spawned and ping
is synchronous. They must not be mistaken for production flow requirements.
Reliable indications fan out to runtime and summary manager with clones of one
claimable processed promise. Preserve its single completion owner per message
family; the last uncompleted clone disappearing withholds the recipient ack and
forgets the accepted inbound copy so a later sender retry can re-surface it.
That existing rule does not by itself establish a bounded upstream defer contract.

## Combined scheduler contract (target)

The coordinator lifecycle is Inactive → Recovering → Running → Stopping → Stopped,
with existing Kompact fault exits. Running owns domain FSMs, partition registry,
one arrival-ordered selective mailbox and bounded effect/output reservations.
Turns are serial; concurrency means overlapping asynchronous work, not parallel
component mutation. Only the **actual mailbox head** may start a new flow, and
only with its complete required set available. Do not scan for younger starts.
Existing finite flows expose pure correlation/phase matchers and consume their
first matching event from anywhere. A second publish is new work, not a
continuation simply because it names an already owned group. One event has one
owner. Each flow decides whether/how to accumulate admissible out-of-order input;
there are no mandatory local FIFOs or global-to-local queue transfers.

Use bounded traversal and arrival-linked slots (or equivalent), with O(1) removal
given the matched slot. Resume a scan for an unchanged matcher; invalidate its
cursor when matching conditions change. Give active flows fair bounded turns and
revisit the head after consumption/release. Reserve completion capacity **before**
launching effects; ordinary ingress cannot occupy it. Bound entries, bytes,
in-flight effects, flow accumulations and event/output capacity. Lifecycle control
is handled directly; expected timer identity uses Kompact's scheduled UUID.

| Worked arrival order | Allowed action and progress argument |
| --- | --- |
| Active A owns group g; mailbox `[Publish(g), Complete(A, listener)]` | A takes its matching completion from behind the head, restores g and ends. Only then can Publish(g) start. |
| Active A owns g; mailbox `[Publish(g), Publish(h), Complete(A, listener)]`, h disjoint | Publish(h) does not bypass the head. A progresses, then head admission advances in arrival order. This deliberately trades utilisation for simpler start fairness. |
| Head is malformed/unsupported ingress or known orphan completion | Consume with explicit classified error/drop/diagnostic disposition; fulfil API error obligations where present. Never complete a reliable processed promise merely to clear the head. A stale decision requiring database evidence is a short head-started validation flow, not speculative disposal. |

All owners must be finite, including timeout/failure/cancellation cleanup. No
active flow may need a *new* later flow to start in order to complete: transaction
and listener completions return directly to their owner; a batch owns its cursor;
recovery composes activation; causal waiting commits pending payload and ends;
human choice ends the invitation flow and later starts a response flow; remote
summary correlation remains in its manager after delegation. A listener must
hand off promptly and must not await a conflicting runtime request/responder:
that can create this forbidden wait cycle, even with concurrency enabled.
No write transaction is retained across that hand-off. A retained read provider
can still impede writes on some backends; lane reservation must avoid reader/
writer wait cycles. One listener lane preserves invocation order, per-group
causal order and contiguous batch callbacks; unrelated preparation/transactions
may overlap if their admitted claims and backend permit it.

Finite flows plus fair turns make a finite accepted prefix advance only if effects
eventually complete or reach a safe terminal outcome. Cancellation/lost replies
are not rollback evidence. Late known cancelled completions need an explicit
terminal policy; an impossible completion for a live phase is an invariant fault.
`flotsync-zop.20` specifies concrete API overload errors, advisory coalescing and
reliable defer/retry promises before mailbox caps are enabled; `.23` specifies
shutdown/drain and uncertain-effect cleanup. Bounds must not silently acknowledge
or discard reliable work. Their policies, plus backend isolation/provider cuts
in `.16`, are outstanding implementation contracts rather than implemented claims.

## Behavioural evidence and limits

Evidence lives in [changes.rs](../flotsync_replication/src/runtime/tests/changes.rs),
[delivery.rs](../flotsync_replication/src/runtime/tests/delivery.rs),
[groups.rs](../flotsync_replication/src/runtime/tests/groups.rs) and
[host.rs](../flotsync_replication/src/runtime/tests/host.rs). Test names below are
stable lookup keys; they describe current ordering, not future concurrency proof.

| Guarantee | Characterisation evidence |
| --- | --- |
| Committed publication visible during pending callback; API reply awaits completion | `publish_changes_emits_local_data_changed_event_before_reply`: explicitly gated callback, independent store reads and pending reply poll. |
| Batch retains item ordering across callback completion | `update_batch_waits_for_listener_before_committing_next_input`: first commit visible, second absent during gate, ordered final callbacks. |
| Partial batch failure/retry preserves committed notifications and positions | `update_batch_failure_after_first_update_keeps_first_notifications`; `update_batch_store_failure_preserves_committed_notification_and_retry_position`. |
| Newly-ready causal chain is one atomic transaction | `causally_ready_apply_chain_rolls_back_when_store_write_fails`; `buffered_updates_survive_runtime_restart_and_drain_from_store`. |
| Accepted work resumes without another human decision; failed activation leaves published source read-only | `auto_accept_commit_failure_restarts_from_activation_instead_of_listener_decision`; `failed_auto_accepted_migration_activation_keeps_published_source_read_only`. |
| Provider-open failure after commit faults; activation streams bounded batches/tokens | `post_commit_activation_read_failure_faults_runtime_and_requires_restart_resync`; `runtime_resumes_pending_group_activation_with_bounded_batches_and_group_read_token`. |
| Pending listener rejection retains work; stale responder outcomes are explicit | `runtime_replay_listener_failure_keeps_pending_group_decision`; `stopped_runtime_stale_invitation_accept_reports_unavailable_after_reject`; `stopped_runtime_stale_invitation_reject_reports_unavailable_after_accept`. |
| Projection aliases retain coherent historical views; local creation/replacement emit positions | `group_state_retains_one_coherent_view_across_publications`; `create_group_persists_membership_across_runtime_restart`; `change_group_membership_emits_inline_snapshot_upserts_for_new_group`. |
| Host reconciliation precedes runtime activation; queued input delivered once | `non_empty_store_replays_application_snapshot_before_runtime_activation`; `inbound_work_queued_during_synchronisation_is_delivered_once_after_completion`; `update_represented_by_synchronisation_cut_is_not_redelivered_live`. |

The test gate captures rows/position before signalling entry, then awaits explicit
release outside the listener mutex. It releases on test unwinding and uses the
existing timeout/socket-reservation fixtures, without timing sleeps or a network
simulator. It proves a real committed-store/listener boundary; it does not pause
every individual store call or establish a generic backend isolation theorem.
Dispatch-versus-listener ordering is additionally traced directly through current
handlers above; these tests do not separately observe outbound port submissions.
Selective admission, compatible overlap, callback wait cycles, capacity and
concurrent shutdown require executable coverage in `.18`–`.23`; they are not
proven by these blocking-driver characterisation tests.
