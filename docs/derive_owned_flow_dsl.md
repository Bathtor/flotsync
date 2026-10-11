---
type: API Contract
title: Derive-Based Owned Flow DSL
description: Explores a derive-based alternative using a real waiting-state enum, ordinary Rust payload types and structured macro implementation.
status: deferred
applies_to:
  - flotsync_replication
source_of_truth:
  - ../flotsync_replication/src/runtime/component/publication_flow.rs
  - ../flotsync_replication/src/runtime/component/flow_driver.rs
depends_on:
  - typed_owned_flow_dsl.md
tracked_by:
  - flotsync-oyr
---

# Derive-Based Owned Flow DSL

## D1 — Alternative and common contract

Deferred on 2026-10-11 at the user's request, alongside Option A. Build a few
more concrete flows before choosing an abstraction. This proposal is retained
for comparison and does not block further flow conversions; its prototype plan
is future evaluation work.

Option B uses `#[derive(OwnedFlow)]` on the actual waiting-state enum. Its variants
associate normal Rust input and result types with ordinary transition functions.
Payload fields live in Rust structs, not in attribute token languages.

The ownership, typed completion tokens, explicit fault handling, caller-provided
identity, invocation correlation and composition contracts are those in
[Option A, P3–P6](typed_owned_flow_dsl.md). This alternative changes how those
relationships are declared and how the generator is maintained. Neither option
is approved for implementation. Syntax and expansion below are design sketches,
not compiled macro output.

Unlike an attribute macro, a derive adds items without replacing its input item.
Consequently the annotated enum should be useful at runtime; it should not be a
dummy schema containing action inputs that disappear after expansion. This is
why the declaration is the waiting-state enum. See the
[Rust Reference on derives](https://doc.rust-lang.org/stable/reference/procedural-macros.html#derive-macros).

## D2 — Publication declaration

The example uses concrete types for the first prototype. Names are provisional.
Imports are omitted; the referenced domain types are the existing publication
types. Each helper attribute has one short role. Result aliases keep long generic
types in normal Rust syntax where rustfmt can format them.

```rust,ignore
/// Publication progress while its caller executes one action.
#[derive(OwnedFlow)]
#[flow(module = publication)]
#[flow(id = FlowId)]
#[flow(context = PublicationContext)]
#[flow(output = PublicationResult)]
#[flow(error = PublicationInvariantError)]
enum PublicationWaiting {
    /// Await the write transaction; the action owns its request and schema.
    #[flow(input = WriteInput)]
    #[flow(result = WriteResult)]
    #[flow(handler = after_write)]
    Writing,

    /// Await broadcast submission of the committed bytes.
    #[flow(input = CommittedPublication)]
    #[flow(result = CommittedPublication)]
    #[flow(handler = after_broadcast)]
    Broadcasting,

    /// Await availability submission to catch-up.
    #[flow(input = CommittedPublication)]
    #[flow(result = CommittedPublication)]
    #[flow(handler = after_advertise)]
    Advertising,

    /// Await the callback while retaining the receipt for the API reply.
    #[flow(input = ListenerDataChanges)]
    #[flow(result = ListenerResult)]
    #[flow(handler = after_listener)]
    NotifyingListener(ListenerWaiting),

    /// Await the reply attempt while retaining its publication outcome.
    #[flow(input = ReplyInput)]
    #[flow(result = ReplyOutcome)]
    #[flow(handler = after_reply)]
    Replying(ReplyWaiting),
}

/// Result of the write action, before any broadcast or callback.
type WriteResult = Result<CommittedPublication, PublishChangesError>;

/// Callback success or rejection after the update was committed.
type ListenerResult = Result<(), InboundDeliveryError>;

/// Caller reply preserving the existing API error source chain.
type PublicationReply = Result<PublishReceipt, ApiError>;

/// Request and schema owned by the write action.
struct WriteInput {
    scoped: ScopedPublication,
    schema: Arc<GroupSchema>,
}

/// Receipt retained while the listener action owns the changes.
struct ListenerWaiting {
    receipt: PublishReceipt,
}

/// Promise and value owned by the reply action.
struct ReplyInput {
    promise: KPromise<PublicationReply>,
    reply: PublicationReply,
}

/// Publication outcome retained until the reply attempt completes.
struct ReplyWaiting {
    outcome: PublicationOutcome,
}
```

`PublicationContext` and `PublicationResult` retain the meanings in P4: context
holds claims and the reply promise until it moves into the action; the output
holds publication and reply outcomes. The driver explicitly returns context on
termination, and the replication adapter constructs `FinishedPublication`.

The declaration accepts unit variants or single-field tuple variants. Unit
variants mean waiting payload `()`. Tuple variants use the field's actual type;
the derive does not inspect that type's definition. Named variant fields could
be supported by generating payload structs, but this first syntax deliberately
uses explicit Rust structs for better navigation and less generator machinery.

There is a tradeoff: input and waiting definitions are nearby rather than all
inline in one declaration. Each relationship is still declared once on its
variant. No companion phase enum is handwritten or generated. The outer machine
holds terminal/faulted lifecycle state, so Finished is not a waiting variant.

## D3 — Generated surface and execution

The derive emits a sibling `publication` module. It contains Machine, Action,
Completion, Next, typed per-step completion tokens and the private dispatch code.
The names of step modules are derived from variants, for example
`NotifyingListener` becomes `notifying_listener`; support an explicit
`#[flow(step = name)]` override and diagnose duplicate resulting names.

For a non-empty step the generated aliases and entry function look like:

```rust,ignore
pub(super) type Input = super::super::ListenerDataChanges;
pub(super) type Waiting = super::super::ListenerWaiting;
pub(super) type Output = super::super::ListenerResult;

pub(super) fn enter(waiting: Waiting, input: Input) -> super::Next {
    super::Next(super::Decision::NotifyingListener { waiting, input })
}
```

These aliases refer to the author's types; they do not copy fields into another
payload struct. For a unit step, `Waiting = ()`. Actual path resolution must
support local aliases, qualified paths and `super` paths in the derive's scope;
the expansion sketch above shows the simple local-name case only. Resolve
consumer types and handler references through a generated bridge in the original
scope, then refer to that bridge from nested modules. Do not blindly prepend
`super::` to user-supplied paths. This needs explicit compile-pass fixtures.

Installing a decision constructs the original `PublicationWaiting` variant and
records it before exposing the action. Dispatch validates identity and matches
that enum against the generated completion enum. The listener match unwraps
`ListenerWaiting` and calls the named handler. Thus the derive reuses the real
enum instead of generating another waiting enum beside it. The private Decision
temporarily pairs waiting and input, as in Option A.

The normal handler and executor shapes are:

```rust,ignore
fn after_broadcast(
    _context: &mut PublicationContext,
    _waiting: (),
    committed: CommittedPublication,
) -> Result<publication::Next, publication::broadcasting::Failure> {
    Ok(publication::advertising::enter((), committed))
}

fn after_listener(
    context: &mut PublicationContext,
    waiting: ListenerWaiting,
    result: ListenerResult,
) -> Result<publication::Next, publication::notifying_listener::Failure>;

// Inside the caller's ordinary action match:
publication::Action::NotifyingListener(request) => {
    let result = notify_listener(request.input).await;
    request.complete.complete(result)
}
```

The listener handler retains the explicit success/error branches from P4. The
completion token accepts `ListenerResult` only. Failure retains waiting data
and a contextual Snafu error; rejected completions retain their payload. The
derive choice does not solve or remove the ownership-recovery ergonomics still
to be assessed in P4.

Constructing a `PublicationWaiting` value alone cannot issue an action or mutate
a running machine. Machine fields and raw constructors remain private; public
entry points accept typed Next decisions. Generated visibility must not expose
private payload types: the first prototype uses private declarations and sibling
access within the enclosing module. Public API visibility and generic derives
need explicit design before broader export; do not apply blanket `pub`.

## D4 — Maintainable generator structure

Use a dedicated procedural-macro crate. The entry point should only parse its
`DeriveInput`, invoke the expansion function and turn errors into diagnostics.
Use `syn` and `quote` rather than token-string concatenation. Keep the internal
representation small and specific to this declaration, for example a FlowSpec
with context/identity/output/error types and a vector of StepSpec values carrying
variant, module name, waiting type, input type, result type, handler and spans.

Separate responsibilities into ordinary named Rust functions/modules:

| Responsibility | Work |
| --- | --- |
| Parse | Read the enum and helper attributes; retain source spans and docs. |
| Validate | Check required/duplicate/unknown keys, variant shapes and generated names. |
| Model | Represent each step once, with an explicit Unit/Payload waiting outcome. |
| Generate types | Emit actions, completions, decision and per-step aliases/tokens. |
| Generate transitions | Emit entry functions, pairwise validation and dispatch. |
| Generate diagnostics | Preserve spans and combine independent declaration errors. |

This is more source than a compact repetition macro, but its control flow,
helpers and tests are ordinary Rust. Avoid a single large `quote!` function:
generate each coherent item with a named helper, assembling their results at
the top level. Keep reusable identity and fault support outside parsing code.
Neither macro crate nor its support should depend on replication or Kompact.

Reject unsupported field-level configuration and variant `cfg`/`cfg_attr` in the
first prototype with an explicit diagnostic rather than silently generating
inconsistent matches. Whole-item configuration is sufficient initially. Forward
documentation deliberately; do not copy unrelated derives or arbitrary attributes
onto every generated item. Tests must establish this restricted support surface.

The macro validates syntax and relationships it can see. It cannot resolve type
aliases or inspect function signatures; ordinary compiler type checking verifies
handlers and generated calls. Attach emitted references to the corresponding
attribute spans, and test actual diagnostics rather than promising perfect IDE
navigation. Original payload structs and handlers remain directly navigable;
generated wrappers still require macro expansion support.

## D5 — Formatting and alternative derive shapes

The formatting benefit comes mainly from keeping fields, types, enums and
functions in normal Rust syntax. Helper attributes reference names and paths;
they do not embed field lists, match bodies, Rust code strings or another DSL.
Custom attribute formatting has limits, so keep attributes short and inspect the
example with the repository's actual rustfmt configuration during prototyping.
No formatter extension should be required. See the
[rustfmt documentation](https://github.com/rust-lang/rustfmt).

Two other derive shapes are less attractive for this first comparison:

| Shape | Reason not to lead with it |
| --- | --- |
| Derive on action enum; waiting fields encoded in attributes | Optimises the executor view but moves important ownership data into custom syntax. |
| Derive per step plus aggregate flow declaration | Allows normal fields everywhere but spreads five-step wiring across multiple derives and an aggregation layer. |

A single attribute macro over a module could inspect and rewrite several items,
but it would broaden the transformation surface. The enum derive is sufficient
without rewriting the user's code.

## D6 — Side-by-side assessment

| Concern | Option A: macro_rules | Option B: derive |
| --- | --- | --- |
| Consumer declaration | All step data inline in one custom block | Real waiting enum; payload types beside it |
| rustfmt | Custom block formatting needs examination | Most content is ordinary Rust; short helper attributes |
| Generator maintenance | Pattern repetitions; complexity increases with grammar and diagnostics | Named parse/validate/generate functions and normal Rust tests |
| Dependencies | No procedural-macro crate needed | Dedicated proc-macro crate and parsing dependencies |
| Diagnostics | Matcher errors plus errors in expansion | Targeted syntax errors with spans; Rust still checks types |
| Navigation | Handlers are normal; payload definitions may be generated | Handlers and payloads are normal; wrappers still generated |
| Ownership and behaviour | P3–P6 contract | Same contract |
| Additional consumer effort | Inline data declaration | Explicit payload structs or reuse existing types |

My preference for further exploration is Option B given the emphasis on
maintaining the generator and formatting the consumer. This does not select it on
the user's behalf. Its principal cost is the number of named payload types and
attributes; the full publication example makes that cost visible before coding.
Judge both on consumer readability and generator maintainability, not merely
which implementation has fewer lines.

Sequential composition remains the P6 model: a parent DeliverItem variant has
an ordinary child input type, child terminal result type and BatchWaiting payload.
Its handler either enters DeliverItem again or finishes. A non-runtime importer
uses the same shape with ReadChunk/SaveChunk states and no replication types.
There is no reason for the derive to know what a listener or child executor is.

## D7 — Sizing and evaluation plan

These are estimates for the derive alternative only, not an instruction to build
both options. No macro code has been implemented or type-checked for this draft.

| Work package | Estimated substantive LOC |
| --- | ---: |
| Derive parsing, validation and source model | 130–190 |
| Generated API and dispatch | 170–230 |
| Typed identity/tokens and ownership-preserving faults | 100–150 |
| Isolated publication declaration and handlers | 140–190 |
| Diagnostics, compile-pass/fail and branching/composition tests | 180–250 |
| Total derive prototype | 720–1,010 |
| Subsequent production adoption and test adjustments | 150–250 |

The prototype itself could exceed 1,000 LOC, and combined adoption could reach
1,260 LOC. If this alternative is chosen, split the work into separately reviewed
subtasks: declaration parsing/expansion and diagnostics (300–420 LOC), then
execution/ownership fixtures and behaviour (420–590 LOC). Production adoption
follows a separate explicit decision. Keep the first subtask internal until the
second establishes ownership correctness; it is not an independently approved
production API.

Retain all P8 behavioural and compile-failure checks. Add declaration-error
fixtures for missing/duplicate attributes, unsupported shapes/configuration,
module-name collisions and non-enum derives. Add compile-pass fixtures for local
and qualified type aliases, parent-module handler paths, non-Clone payloads and
two flows in one module. Inspect expanded code and handler-mismatch diagnostics.
Review rustfmt output for the example and a deliberately long type name. Compare
the complete consumer and generated support against the handwritten flow before
deciding to adopt either macro approach.
