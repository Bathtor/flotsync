---
type: API Contract
title: Typed Owned Flow DSL
description: Proposes a caller-driven typed flow declaration with explicit ownership and ordinary Rust transitions, independent of the replication runtime.
status: deferred
applies_to:
  - flotsync_replication
source_of_truth:
  - ../flotsync_replication/src/runtime/component/publication_flow.rs
  - ../flotsync_replication/src/runtime/component/flow_driver.rs
depends_on:
  - replication_runtime_flow_contracts.md
tracked_by:
  - flotsync-oyr
---

# Typed Owned Flow DSL

## P1 — Proposal and scope

Deferred on 2026-10-11 at the user's request. Retain both alternatives and build
a few more concrete flows before revisiting the abstraction. Neither proposal
is selected, and this work no longer blocks further flow conversions. The
prototype plans below are retained for later evaluation, not current work.

Option A proposes a small `macro_rules!` declaration for owned, caller-driven state
machines. Each step declares its action input, retained waiting data, result and
named Rust handler together. Generate the mechanical pairing and dispatch;
leave domain decisions and action execution in ordinary Rust.

[Option B: derive-based declarations](derive_owned_flow_dsl.md) explores the same
contract using ordinary Rust types and helper attributes. Neither macro strategy
has been selected. Compare both before choosing a prototype.

This is a proposal awaiting alignment, not an adopted API or implementation.
The existing publication flow remains the behavioural reference. The initial
prototype should be isolated from production dispatch, with publication adoption
reviewed separately after the prototype demonstrates a worthwhile reduction.

Treat replication as the first consumer, not as the abstraction's vocabulary.
The reusable layer must not depend on Kompact, replication groups, stores,
listeners, promises, resource registries or scheduling. It accepts a caller's
identity type, context, terminal output and domain error types. The initial
implementation can remain private to the prototype; choose a shared crate only
after a second consumer establishes the useful boundary. Avoid exporting a
general framework prematurely.

## P2 — Single declaration

Proposed syntax below is illustrative and has not been compiled. Explicit step
module names avoid identifier concatenation and keep generated paths predictable.
The variant and module identifiers are the only naming duplication; action,
completion and waiting variants all use the same step name.

```rust,ignore
owned_flow! {
    mod publication {
        id: FlowId;
        context: PublicationContext;
        output: PublicationResult;
        error: PublicationInvariantError;

        Writing as writing {
            input { scoped: ScopedPublication, schema: Arc<GroupSchema> }
            waiting {}
            result: Result<CommittedPublication, PublishChangesError>;
            then: after_write;
        }
        Broadcasting as broadcasting {
            input { committed: CommittedPublication }
            waiting {}
            result: CommittedPublication;
            then: after_broadcast;
        }
        Advertising as advertising {
            input { committed: CommittedPublication }
            waiting {}
            result: CommittedPublication;
            then: after_advertise;
        }
        NotifyingListener as notifying_listener {
            input { changes: ListenerDataChanges }
            waiting { receipt: PublishReceipt }
            result: Result<(), InboundDeliveryError>;
            then: after_listener;
        }
        Replying as replying {
            input {
                promise: KPromise<Result<PublishReceipt, ApiError>>,
                reply: Result<PublishReceipt, ApiError>,
            }
            waiting { outcome: PublicationOutcome }
            result: ReplyOutcome;
            then: after_reply;
        }
    }
}
```

Documentation attributes on steps and fields must be forwarded to generated
items. The macro should not require `Clone`, `Debug`, `Send` or `'static` for
payloads. An asynchronous executor imposes its own necessary bounds. A first
prototype need only support concrete owned payload types; support for arbitrary
generic declarations is a later syntax decision, not an excuse to introduce
type erasure.

## P3 — Generated API and Rust shape

For each step generate `Input`, `Waiting`, a result type alias, `Request`, and
`enter(waiting, input) -> Next`. `Next` wraps a private decision enum so callers
cannot construct an action paired with another step's state. For the listener:

```rust,ignore
pub struct Input {
    pub changes: ListenerDataChanges,
}
pub struct Waiting {
    pub receipt: PublishReceipt,
}
pub type Output = Result<(), InboundDeliveryError>;

pub struct Request {
    pub input: Input,
    pub complete: CompletionToken,
}

pub fn enter(waiting: Waiting, input: Input) -> Next {
    Next(Decision::NotifyingListener { waiting, input })
}

impl CompletionToken {
    pub fn complete(self, result: Output) -> Completion {
        Completion {
            identity: self.identity,
            value: CompletionValue::NotifyingListener(result),
        }
    }
}
```

`CompletionToken` is specific to this flow and step, has private fields, and is
neither `Copy` nor `Clone`. Matching an action exposes its input and consumes its
token once after execution. For example, awaiting the existing callback and then
calling `request.complete.complete(result)` constructs the correct completion.
The token does not run code or signal cancellation when dropped.

The complete publication enum expansion has these shapes (visibility and
documentation abbreviated):

```rust,ignore
enum Action {
    Writing(writing::Request),
    Broadcasting(broadcasting::Request),
    Advertising(advertising::Request),
    NotifyingListener(notifying_listener::Request),
    Replying(replying::Request),
}
enum Waiting {
    Writing(writing::Waiting),
    Broadcasting(broadcasting::Waiting),
    Advertising(advertising::Waiting),
    NotifyingListener(notifying_listener::Waiting),
    Replying(replying::Waiting),
}
enum CompletionValue {
    Writing(writing::Output),
    Broadcasting(broadcasting::Output),
    Advertising(advertising::Output),
    NotifyingListener(notifying_listener::Output),
    Replying(replying::Output),
}
enum Decision {
    Writing { waiting: writing::Waiting, input: writing::Input },
    Broadcasting { waiting: broadcasting::Waiting, input: broadcasting::Input },
    Advertising { waiting: advertising::Waiting, input: advertising::Input },
    NotifyingListener {
        waiting: notifying_listener::Waiting,
        input: notifying_listener::Input,
    },
    Replying { waiting: replying::Waiting, input: replying::Input },
    Finished(PublicationResult),
}
```

`Decision` is a temporary owned transition proposal, not another stored phase.
The machine installs its waiting payload before returning the corresponding
action. A generated pairwise match checks `Waiting` against `CompletionValue`
before moving either payload; generated `name()` methods use `stringify!` for
diagnostics. There is no `PublicationPhase`, numeric step-kind table or string
comparison for validation.

The outer machine distinguishes running, finished and faulted lifecycle states.
Running owns context and waiting data; finished accepts no more results. An
internal moving-state sentinel may be needed during dispatch, but must never
be exposed as an ordinary waiting or successful terminal state.

## P4 — Ordinary Rust transitions and ownership

Each handler has a concrete signature:

```rust,ignore
fn after_listener(
    context: &mut PublicationContext,
    waiting: publication::notifying_listener::Waiting,
    result: publication::notifying_listener::Output,
) -> Result<publication::Next, publication::notifying_listener::Failure>;
```

Handlers consume their waiting data and result. The flow retains context across
the call. `Failure` carries the declared invariant error **and the step's retained
waiting data**; it is not a bare error that silently drops owned state. Generated
dispatch stores both in the faulted machine. Where fallible work would consume
waiting data, the handler must first validate or explicitly reconstruct the
retained payload on failure. This constraint is deliberate and must be assessed
for ergonomics in the prototype. No generic rollback is promised for arbitrary
context mutation or panics.

`PublicationContext` owns the non-cloneable `FlowOwnership` and the optional reply
promise. The promise is present until entry into Replying. A missing promise is a
contextual Snafu invariant error, not a normal absent-value outcome. Claims stay
in context throughout the reply attempt. Successful termination returns
`Finished { context, output }` explicitly; the replication adapter destructures
these into the existing `FinishedPublication` and releases the slot. The reusable
layer never knows how to release claims.

Equivalent publication transitions are:

| Handler | Result branch | Next state and ownership |
| --- | --- | --- |
| `after_write` | Success | Broadcasting; move committed data into its input. |
| `after_write` | Failure | Replying with `BeforeCommitFailed` and the existing classified API error. |
| `after_broadcast` | Submission complete | Advertising; move the same committed data into its input. |
| `after_advertise` | Availability submitted | Split committed data; retain receipt in NotifyingListener and move changes into its input. |
| `after_listener` | Success | Replying; move receipt into the reply and retain Published outcome. |
| `after_listener` | Failure | Replying; retain CommittedListenerFailed outcome and preserve the boxed API error source chain. |
| `after_reply` | Delivered or receiver dropped | Finish with publication outcome and reply outcome; return context with all claims. |

`begin_reply` remains a named Rust helper shared by both error branches and the
success branch. It validates promise availability before moving the promise or
receipt, then constructs the typed Replying decision. The prototype should show
its concrete failure-recovery signature; hiding ownership problems inside a
macro would defeat the purpose of this design.

No DSL retry, cancellation propagation, transaction management or implicit
resource release is introduced. A dropped receiver does not cancel committed
work, and neither a callback failure nor any later failure retries the write.

## P5 — What typing proves, and what runtime checks prove

The prototype must reject at compile time: a listener result supplied to a write
token; listener waiting data passed to Replying; an input of the wrong step type;
reuse of a consumed completion token; cloning claims or completion tokens; and
passing a completion from a different generated flow type.

This proposal does **not** statically encode the full legal transition graph.
Any handler can deliberately enter any declared step with correctly typed data.
Tests still prove that publication does not reply before the callback or write
again after commit. Generating per-source successor enums would strengthen that
guarantee but repeat graph edges and complicate shared helpers; defer that feature
unless prototype review establishes a concrete need. “Wrong typed transition”
must not be confused with “type-correct but wrong business decision”.

Results from another instance of the same flow type still require runtime checks.
Completion identity contains the caller's flow ID plus a per-action invocation
number. Validate owner, invocation and step before removing waiting data or
calling a handler. Errors contain expected/received identity and step names,
with Snafu location information. Rejection leaves live state and claims intact;
the replication adapter retains the existing policy of faulting the owning slot.
Return the rejected completion with its error so future resource-bearing results
are not silently discarded; the adapter must retain that payload when faulting.

The component-local wrapping FlowId allocator remains the authority for flow
identity. The DSL allocates no global IDs. A checked invocation counter prevents
an old completion being accepted on a later visit to the same step. Exhaustion
faults before exposing another action, retaining its prepared decision and
context. This is action correlation, not scheduling or timer bookkeeping.
Flow-ID reuse retains the existing condition that no old result survives a full
allocator cycle. Tokens prevent ordinary duplicate construction; defensive tests
inside the generated module still exercise malformed and repeated envelopes.

## P6 — Branching, composition and another consumer

Branching is ordinary Rust: `after_write` matches success/failure and enters
Broadcasting or Replying. A hypothetical non-runtime import flow can similarly
branch after validation into Save or ReportInvalid, and loop through ReadChunk
and SaveChunk. It needs no claims, promise, listener, Kompact system or database
type supplied by the DSL. Use this as the second small prototype fixture.

Composition initially means an explicit child-driving adapter. For example, a
parent batch flow declares `DeliverItem`, whose input owns one child's starting
input, result is that child's terminal output, and waiting data owns the remaining
items and accumulated outcomes. The executor drives the child using its ordinary
Action/Completion API, then completes the parent's DeliverItem token. The parent
matches child success/failure and either enters DeliverItem for the next item or
finishes with the partial batch outcome.

The parent's token remains associated with that one child invocation across all
child actions. Parent and child waiting data are distinct. Parent-held claims
stay with the parent; any claims deliberately transferred into the child must
return through the child's terminal or fault result. The adapter must forward
child faults with retained ownership rather than turn them into a success or
ordinary domain rejection. Never reacquire a conflicting claim held by a parent.

This establishes a composition boundary without flattening child phases into a
global enum. Parallel children, joins, cancellation, scheduler admission and
shutdown policy remain outside this prototype and in their existing follow-up
tasks. The reusable API must not assume that each action executes immediately or
that the caller blocks while it runs.

## P7 — Macro choice and review criteria

The rationale for Option A is that its grammar is repetitive and needs neither type
inspection nor generated identifier spelling. Normal function paths preserve
navigation into transition logic and normal Rust diagnostics for handlers.
Generated types remain harder to navigate, macro diagnostics can be indirect,
and expanded source is not as readable as hand-authored code. Capture an actual
expansion and compile-failure diagnostics during prototype review.

A derive is an equal alternative: ordinary Rust implementation structure and
better formatting of consumer declarations are useful independently of any
limitation in declarative matching. Option B discusses parsing, span handling,
a separate macro crate and maintenance cost. An async-function DSL is not proposed:
it would hide waiting state and make explicit inspection and ownership recovery
harder. A handwritten generic engine alone would still require repetitive enum
pairing at each flow declaration.

Measure handwritten substantive LOC for declaration, handlers, adapter and tests
separately from reusable macro/support code and generated expansion. The current
publication has five action variants, five completion variants, six state/phase
variants and their handwritten correspondence matches. The proposal has five
step declarations and generated correspondence, but typed wrappers, recovery and
tokens add machinery. Actual net savings are unproven; reject the prototype if
the complete consumer becomes harder to read or the benefit is only hidden code.

## P8 — Proposed work and evidence

| Work package | Estimated substantive LOC |
| --- | ---: |
| Declaration macro, generated pairing and dispatch | 180–240 |
| Typed tokens, identity validation and ownership-preserving faults | 100–150 |
| Isolated publication example with explicit transitions | 130–180 |
| Branching/composition fixture and compile-failure/runtime checks | 160–220 |
| Total prototype | 570–790 |
| Subsequent production adapter/adoption and test adjustments | 150–250 |

Combined work could reach **1,040 LOC**. Keep prototype evaluation and production
adoption as separately planned and reviewed tasks rather than implementing both
at once. Broad conversion stays with the existing flow conversion issues.

Prototype checks must demonstrate the compile-time failures in P5 and runtime
rejection without ownership loss for wrong IDs, wrong steps, repeated results,
same-step stale invocations and terminal completions. Include faults with
non-cloneable waiting/result payloads and invocation exhaustion. Exercise both
branches of the child composition fixture and show explicit ownership recovery.

Publication's existing tests cover success ordering, pre-commit failure,
post-commit listener failure and error sources, dropped receivers, terminal
duplicates, impossible results and slot cleanup. The prototype should reproduce
those assertions against its isolated flow; existing tests alone do not validate
generated code. On production adoption, retain those cases and the runtime test
`publish_changes_emits_local_data_changed_event_before_reply`, which checks the
real store/callback/reply boundary. Use the existing socket broker for any socket
fixtures. Rust changes require formatting and pedantic Clippy before review.

Acceptance is an explicit decision to adopt or keep handwritten flows after
examining expansion, diagnostics, ownership recovery, tests and measured source
reduction. No prototype or production Rust has been implemented for this draft.
