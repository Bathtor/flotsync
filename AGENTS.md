# Agent Instructions

## Rust Rules

- Format Rust code according to `rustfmt.toml`.
- Keep Rust changes clippy-clean where practical.
- Prefer readable control flow over chained iterator side effects.
- Use Snafu-derived error types (`#[derive(Snafu)]`) for Rust error enums.
- When splitting a single-file Rust module into a folder module, move the original module contents to `mod.rs` in the new folder.

## Version Control Rules

- Use `jj` commands for repository operations instead of `git`, unless explicitly requested otherwise.

## Issue Tracking

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
```
