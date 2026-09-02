# AGENTS.md - Project Schema

This is kafkaman: We imagined the following, in a service oriented architecture each service would communicate with another using kafka instead of rest because of its reliability and monitoring story. In order to clarify when a message is consumed/sent it would first be saved in a postgres database then a scheduler would attempt to run the code in the app to consume the message or send it and handle retries. So we plan to make a library to handle kafka that would plug in our modern rust projects.

## Agent Role

You own `wiki/`. You write, update, cross-link, and maintain all wiki content.
Humans curate `raw/` and make judgment calls. You handle the bookkeeping.

## How To Orient

When the host exposes the LLM Wiki MCP server, it is your primary surface for
wiki operations — prefer it over shell-family file reads and search:

- Read `wiki/` and `raw/` pages with the `llm_wiki_read` MCP tool (not `cat`,
  `sed`, or a shell read on those paths).
- Find pages with the `llm_wiki_search` MCP tool, and `llm_wiki_search_all` for
  cross-project search, instead of grepping the filesystem.

(On a developer test instance these tools carry a `_test` suffix, e.g.
`llm_wiki_search_test`.) If the MCP server is not configured, fall back to the
index-and-read steps below.

1. Read `project_guidelines.md` for the documentation model and rules.
2. Read `wiki/index.md` for the catalog of all project knowledge.
3. Read specific wiki pages identified from the index (via `llm_wiki_read` when
   the MCP server is available).
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

1. Search with the `llm_wiki_search` MCP tool first — it ranks across the whole
   project. Read `wiki/index.md` for orientation when the MCP server is
   unavailable.
2. Read the matching pages with `llm_wiki_read` (or directly when no MCP server
   is configured).
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
| Implementation Review | `reference.md` | `wiki/reviews` |

An Implementation Review is a `reference.md` because that is what it is: evidence
from an investigation, at a point in time. It keeps the `Sourced` status and the
line-number citations it was written with, and it is **not** updated when the
code moves underneath it — a review that is edited to stay current stops being a
record of what was found. Durable conclusions from a review are promoted into a
spec or a decision instead, which is where staleness *is* a defect.

**Cite the wiki page, not the working file.** A review is usually drafted as a
scratch file at the repository root (`review.md`, `worktree-review.md`) that is
never committed. Three pages cite `review.md` in their `Sources:` and it has
never existed in this repository, which makes those citations unfollowable —
provenance that points at nothing is worse than none, because it reads as though
it could be checked. Land the review as a `wiki/reviews/*.reference.md` page
first, then cite that.

## Library Pack

- Track public API compatibility in `wiki/compatibility/`.
- Keep runnable examples under `examples/`.
- Record changelog, migration, and semver-impact notes when public behavior changes.

## Code Pack

- Keep application or tool source under `src/`.
- Keep automated tests under `tests/`.
- Keep project utilities and automation under `scripts/`.
- Keep infrastructure definitions under `infra/`.

