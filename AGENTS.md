# Agent Instructions

- Use "durable" only for explicit persistence/crash-survival guarantees. Do not use it as a general synonym for local, stored, applied, persisted, observed, or current state.
- Do not plan "migrations" of any kind at this point. We are still building up the project. There are no existing deployments or database state or any such thing we need to handle.
- Do not use the term "arm <timers>". Use "set" or "schedule" or something normal.

## Local Agent Notes

- Use `.agent_local_docs/tmp/` for temporary agent working notes that should survive ordinary machine uptime. Do not put this kind of state in `/tmp` or `/private/tmp` unless the user explicitly asks for OS-level temporary storage.
- Review-feedback working notes are temporary coordination state, not project issue tracking. Keep them under `.agent_local_docs/tmp/`, not in `bd`.
- When the user provides substantial review feedback, capture every comment in a working note before implementation. Include stable IDs, file context, the feedback, current status, and the intended direction.
- After plan alignment, update the working note with the agreed direction before editing production files.
- During and after implementation, use the working note as the checklist. Before final response, double-check every captured feedback item and report any item that remains unresolved or intentionally deferred.

## Rust Rules

- Do not run multiple `cargo` instances in parallel! They anyway lock.
- Inside the sandbox, `cargo test` may run either one explicit test or a broader test selection only when passed `-- --test-threads=1`. Any grouped or repeated multi-threaded `cargo test` run must be executed outside the sandbox.
- Format Rust code according to `rustfmt.toml`.
- Keep Rust changes clippy-clean where practical. Use `-W clippy::pedantic` before a change is considered ready for review.
- Prefer readable control flow over chained iterator side effects.
- Prefer `for` and `while`/`while let` over `loop` where possible; seeing the termination condition up front is usually clearer, even when the condition contains a fallible expression such as `while let Some(item) = next_item()?`.
- When splitting a single-file Rust module into a folder module, move the original module contents to `mod.rs` in the new folder.
- Avoid nesting `?` into expressions. It's easier to read if they only occur at the end of a line. Refactor the expression into a field where needed.
- Document non-public Rust helpers, fields, variants, and local types whenever their role, invariants, lifecycle, or preconditions are non-trivial or non-obvious. Prefer documenting what the item is supposed to do before adding code that explains how it does it.
- Add loop labels when control flow spans non-trivial nested loops or retries.
- Prefer the following top-level grouping within Rust files unless there is a strong local reason not to:
    1. public items (`pub`)
    2. restricted-visibility items (`pub(<qualifier>)`)
    3. macros
    4. private items
    5. exposed test helpers
    6. tests
- Within each group, use this order:
    1. constants
    2. traits
    3. functions
    4. structs/enums, each followed immediately by all associated `impl` blocks
- Imports should remain at the very top of the file/module/function.

### Snafu

- Use Snafu-derived error types (`#[derive(Snafu)]`) for Rust error enums.
- Prefer `context(...)` / `with_context(...)` over manual `map_err(...)` when the target error still wraps the original source. Use `with_context(...)` when building the context captures clones, allocations, or other non-trivial work.
- If the only reason to introduce a new error variant is to differentiate the use-site of an existing variant, prefer adding `location: Location` to the existing variant instead.
- Use `#[snafu(module(...))]` plus module-qualified selector names when otherwise identical selector names would collide. Do not introduce custom selector aliases like `FooBarBazSnafu` just to disambiguate use sites.
- Keep Snafu variant names generic inside one error enum. Do not bake call-site names like `PublishStoreAccess` into the variant when the enum type or selector module already provides that context.
- Reserve manual `map_err(...)` for real error translation cases that `context(...)` cannot express cleanly.
- Do not manually construct Snafu boxed-source variants with `Box::new(source)`; use `result.boxed().context(SelectorSnafu)` or `context(...)`/`with_context(...)` instead.

### Kompact

- In Kompact components, the `async_self` argument in `spawn_local` and `Handled::block_on` has access to all of the component's `&mut self` state. Do not clone fields just to pass them into the closure!
- Do not hang on the result of `biconnect_components` unless the use-case strictly requires manual disconnection of ports. This is only the case for components that are dynamically created and destroyed during the normal operation of a Kompact system. For all other connections, just drop the handle returned `biconnect_components` immediately (e.g. by assigning to a `let _connection = biconnect_components(...)`.

### Flotsync

- Any test that binds TCP or UDP sockets must declare its full socket requirement up front via `flotsync_io::test_support::reserve_sockets(...)` or a harness built on top of that broker. Do not bind ad hoc sockets or rely on unmanaged ephemeral ports in parallel tests.

<!-- BEGIN BEADS INTEGRATION -->

## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Important Rules

- Do not access `bd` in parallel!
- Always access it outside the sandbox. (It starts a server and it gets confused otherwise.)

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

_Note:_ Do _not_ close issues until I have reviewed the changes made and agreed that the issue is indeed completed. Ask if uncertain.

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Immediate (Drop everything and do this now.)
- `1` - ASAP (Finish what we are working on and then pick this up next. Don't start anything lower priority until this is done, even if it would complete a MVP slice.)
- `2` - High (Important Items to work on.)
- `3` - Normal (default; Just stuff that needs to be done.)
- `4` - Low/Backlog (nice-to-have, polish, cleanup, future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
    - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Auto-Sync

bd automatically syncs via Dolt:

- Each write auto-commits to Dolt history
- Use `bd dolt push`/`bd dolt pull` for remote sync
- No manual export/import needed!

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

<!-- END BEADS INTEGRATION -->
