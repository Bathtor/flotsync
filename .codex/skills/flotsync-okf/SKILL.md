---
name: flotsync-okf
description: Maintain Flotsync's OKF-style design documentation and repo-local OKF conventions. Use when Codex needs to create, update, validate, index, or cross-link Flotsync design docs under docs/, edit docs/index.md, maintain scripts/okf-docs.sc, or preserve the project-specific OKF frontmatter and concept taxonomy.
---

# Flotsync OKF

## Core Workflow

Use this skill for Flotsync design-document organisation work, not for ordinary
Rust implementation tasks.

1. Read `references/doc-conventions.md` before editing OKF concept docs or
   `scripts/okf-docs.sc`.
2. Refresh the current file contents before editing, because project-local
   instructions assume manual changes may have happened since the last read.
3. Preserve prescriptive protocol, state-machine, wire-format, and API contract
   content unless the user explicitly asks for a design change.
4. Keep issue tracking in `bd`; do not add Markdown task lists or OKF log files
   for project work tracking.
5. After editing concept docs, run `amm scripts/okf-docs.sc generate-index`
   and then `amm scripts/okf-docs.sc check`.

## Scope

OKF is an organisation layer for Flotsync documentation. It gives design
concepts stable metadata, an index, and checkable links. It is not the design
authority: Rust types, protobuf definitions, tests, and prescriptive design
documents remain authoritative for behaviour.

## Resources

- `references/doc-conventions.md`: frontmatter fields, concept types, status
  values, body conventions, link rules, and checker expectations.
