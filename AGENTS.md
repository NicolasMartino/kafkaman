# AGENTS.md - Project Schema

This is kafkaman: We imagined the following, in a service oriented architecture each service would communicate with another using kafka instead of rest because of its reliabilty and monitoring story. In order to clarify when a message is consumed/sent it would first be saved in a  postgres database then a scheduler would attempt to run the code in the app to consume the message or send it and handle retries. So we plan to make a library to handle kafka that would plug in our modern rust projects.

## Agent Role

You own `wiki/`. You write, update, cross-link, and maintain all wiki content.
Humans curate `raw/` and make judgment calls. You handle the bookkeeping.

## How To Orient

1. Read `project_guidelines.md` for the documentation model and rules.
2. Read `wiki/index.md` for the catalog of all project knowledge.
3. Read specific wiki pages identified from the index.
4. Read `raw/` sources only when wiki content is insufficient.

Never browse the filesystem to find information. `wiki/index.md` is your
entry point.

## Operations

### Ingest

When new material appears in `raw/`:

1. Read the raw source fully.
2. Identify facts, entities, relationships, decisions.
3. Write or update wiki pages using the correct document type.
4. Check for contradictions with existing wiki content.
5. Update `wiki/index.md`.
6. Append to `wiki/log.md`.

### Query

When answering questions:

1. Read `wiki/index.md` to find relevant pages.
2. Read those pages.
3. Synthesize an answer with citations.
4. If the answer is durable new knowledge, file it as a wiki page.

### Lint

Periodically or on request:

1. Scan for contradictions between pages.
2. Find stale statuses or outdated claims.
3. Identify orphan pages not linked from index.
4. Check for missing cross-references.
5. Fix issues directly.
6. Log all changes in `wiki/log.md`.

## Conventions

- Document types: spec, decision, proposal, roadmap, plan, checklist,
  reference.
- Pack-specific document types are listed below when active packs add them.
- Use the type by role, not convenience. See `project_guidelines.md`.
- Every wiki page has a metadata block: Document Class, Status, Date,
  Category, Scope, Sources, and Related when useful.
- Filenames: `[slug].type.md` or `[index]-[slug].type.md`.
- Archived documents go to `wiki/archive/`.
- `wiki/log.md` uses format: `## [YYYY-MM-DD] operation | subject`.

## Pack Document Types

| Document type | Filename suffix | Folder |
| --- | --- | --- |
| Compatibility Note | `compat.md` | `wiki/compatibility` |

## Library Pack

- Track public API compatibility in `wiki/compatibility/`.
- Keep runnable examples under `examples/`.
- Record changelog, migration, and semver-impact notes when public behavior changes.

## Code Pack

- Keep application or tool source under `src/`.
- Keep automated tests under `tests/`.
- Keep project utilities and automation under `scripts/`.
- Keep infrastructure definitions under `infra/`.
