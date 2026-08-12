# Project Guidelines - kafkaman

- Document Class: Spec
- Status: Active
- Date: 2026-08-12
- Category: Documentation and execution model
- Scope: Project documentation, knowledge management, and execution rules.

## Purpose

This file defines the documentation and execution model for kafkaman.

We imagined the following, in a service oriented architecture each service would communicate with another using kafka instead of rest because of its reliabilty and monitoring story. In order to clarify when a message is consumed/sent it would first be saved in a  postgres database then a scheduler would attempt to run the code in the app to consume the message or send it and handle retries. So we plan to make a library to handle kafka that would plug in our modern rust projects.

The model combines the LLM Wiki pattern (Karpathy, 2026) with typed document
roles. The agent owns the wiki layer. The human curates raw sources and makes
judgment calls.

## Architecture

Three layers:

```
raw/              Immutable source material. Human-curated.
wiki/             Compiled knowledge. Agent-owned.
AGENTS.md        Schema. Conventions, workflows, agent instructions.
```

The agent compiles raw sources into structured wiki pages at ingest time.
Queries run against pre-organized wiki content, not raw sources. Knowledge
compounds with each ingest rather than being re-derived on each query.

### Layer 1: Raw Sources

`raw/` contains immutable source material: PRDs, meeting transcripts, research
papers, vendor docs, external API references, screenshots, data samples,
user feedback, design mockups.

Rules:

1. humans decide what enters `raw/`
2. once ingested, raw files are not modified
3. raw files are the provenance chain for wiki content
4. organize by source type or date, not by topic (the wiki handles topical organization)

### Layer 2: The Wiki

`wiki/` contains agent-generated markdown files. The agent writes, updates,
cross-links, and maintains all wiki content. This is the project's compiled
knowledge base.

Rules:

1. the agent owns every file in `wiki/`
2. the agent updates affected pages on every ingest, not just the new page
3. every wiki page links back to its raw sources
4. wiki content uses typed documents (see Document Roles below)
5. the wiki must always make it clear whether a statement is validated reality,
   future direction, execution support, or a durable decision

### Layer 3: The Schema

`AGENTS.md` at the repository root tells the agent how to operate. It specifies:

1. wiki structure and folder conventions
2. ingest, query, and lint workflows
3. document types and their roles
4. metadata format
5. status vocabulary
6. project-specific entity types and naming

The schema is the contract between the human and the agent. Update it when
conventions change.

## Navigation Files

### index.md

`wiki/index.md` is the master catalog. The agent reads this file first to
orient itself before any operation.

Contents:

1. one-line project summary
2. current stage or milestone
3. catalog of all wiki pages organized by document type
4. each entry has: file path, title, status, one-line summary
5. updated on every ingest and every lint

The index must fit in a single context window. If it grows beyond ~50,000
tokens, split into a root index with per-type sub-indexes.

### log.md

`wiki/log.md` is an append-only chronological record of all wiki mutations.

Format:

```
## [YYYY-MM-DD] operation | subject

Brief description of what changed and why.
Pages affected: [list of paths]
```

Operations: `ingest`, `update`, `lint`, `promote`, `archive`, `create`.

## Core Operations

### Ingest

Triggered when new material enters `raw/`.

The agent:

1. reads the raw source
2. identifies key facts, entities, relationships, and decisions
3. writes or updates summary pages in `wiki/`
4. updates entity and concept pages across the wiki where affected
5. checks for contradictions with existing wiki content
6. updates `wiki/index.md`
7. appends to `wiki/log.md`

Ingest compiles knowledge once. The goal is that future queries never need to
read the raw source again.

### Query

The agent answers questions against the wiki.

The agent:

1. reads `wiki/index.md` to identify relevant pages
2. reads those pages
3. synthesizes an answer with citations to wiki pages (and transitively to raw sources)
4. if the answer produces durable new knowledge, files it as a new wiki page

### Lint

Periodic health check of the wiki.

The agent scans for:

1. contradictions between pages
2. stale claims (status or facts that no longer match reality)
3. orphan pages with no inbound links from index or other pages
4. missing cross-references between related pages
5. specs that reference unvalidated claims
6. proposals or plans with outdated status
7. broken file path references

Lint fixes issues directly rather than just reporting them. Changes are logged
in `wiki/log.md`.

## Document Roles

Use the document type by role, not by convenience.

### Core Document Types (all projects)

| Document type | Core question | Purpose |
| --- | --- | --- |
| `*.spec.md` | What is validated truth? | Accepted baseline: interfaces, schemas, behavior, architecture, measured properties |
| `*.decision.md` | What lasting choice did we make, and why? | Durable architectural, product, model, data, or process decision |
| `*.proposal.md` | Should we do this? | Future direction not yet accepted or validated |
| `*.roadmap.md` | In what order will we deliver? | Ordered orchestration of deliverables, dependencies, and proof gates |
| `*.plan.md` | How do we execute one deliverable? | Tactical execution with steps, ownership, and verification |
| `*.checklist.md` | What repeatable procedure must be followed? | Operational, release, deployment, or incident procedure |
| `*.reference.md` | What external evidence exists? | Source notes for papers, APIs, vendor docs, benchmarks |

Short version:

- Spec - validated truth
- Decision - durable choice
- Proposal - direction
- Roadmap - ordering
- Plan - execution

- Checklist - repeatable procedure
- Reference - raw evidence

## Wiki Folder Structure

```text
wiki/
  index.md                    Master catalog, agent reads first
  log.md                      Append-only mutation log
  specs/                      Validated truth
  decisions/                  Durable choices
  proposals/                  Future direction
  roadmaps/                   Deliverable orchestration
  plans/                      Tactical execution
  checklists/                 Repeatable procedures
  references/                 External evidence synthesis

  archive/                    Completed, superseded, or rejected documents
```

## Full Repository Structure

```text
./
  AGENTS.md                   Schema: agent conventions and workflows
  CLAUDE.md                   Compatibility shim for Claude-oriented tooling
  project_guidelines.md       This file: documentation and execution model
  README.md                   Project orientation (optional; not created by init)
  raw/                        Immutable source material (human-curated)
  wiki/                       Compiled knowledge (agent-owned)
    index.md
    log.md
    specs/
    decisions/
    proposals/
    roadmaps/
    plans/
    checklists/
    references/

    archive/

```

## Naming And Metadata

Filename patterns:

```
[slug].spec.md
[slug].decision.md
[index]-[slug].proposal.md
[slug].roadmap.md
[slug].plan.md
[index]-[slug].experiment.md
[index]-[slug].eval.md
[slug].checklist.md
[slug].reference.md
```

Metadata block for all wiki documents:

```md
- Document Class: [Spec / Decision / Proposal / Roadmap / Plan / Experiment / Eval / Checklist / Reference]
- Status: [STATUS]
- Date: YYYY-MM-DD
- Category: [SHORT LABEL]
- Scope: [ONE SENTENCE]
- Sources: [list of raw/ paths or external URLs that informed this page]
```

Optional fields:

1. `Owner`
2. `Supersedes` / `Superseded By`
3. `Related` (links to other wiki pages)
4. `Promotion Target` (what spec or decision to update after validation)

## Status Vocabulary

| Document class | Statuses |
| --- | --- |
| Specs | `Active`, `Superseded` |
| Decisions | `Accepted`, `Superseded` |
| Proposals | `Proposed`, `Accepted`, `Deferred`, `Rejected`, `Superseded` |
| Roadmaps | `Draft`, `Active`, `Paused`, `Completed`, `Superseded` |
| Plans | `Draft`, `Active`, `Blocked`, `Completed`, `Superseded` |
| Experiments | `Planned`, `Running`, `Recorded` |
| Evals | `Planned`, `Baseline`, `Candidate`, `Accepted`, `Rejected`, `Superseded` |
| Checklists | `Active`, `Superseded` |
| References | `Draft`, `Sourced`, `Archived` |

Superseded, rejected, or no-longer-useful historical documents move to
`wiki/archive/` and are removed from the active index. Completed plans,
roadmaps, checklists, and accepted proposals may remain in the active index
while they are current evidence, reusable procedures, or important context.

## Pack Document Types

| Document type | Filename suffix | Folder |
| --- | --- | --- |
| Compatibility Note | `compat.md` | `wiki/compatibility` |

## Library Pack Additions

Library and SDK projects add compatibility notes for public API behavior,
semver-sensitive changes, examples, and migration guidance. Public-surface
changes should identify downstream impact.

## Code Pack Additions

Code projects add root folders for application or tool source, automated tests,
project utilities, and infrastructure definitions:

```text
src/                        Application or tool source
tests/                      Automated tests
scripts/                    Utilities and automation
infra/                      Infrastructure definitions
```

## Read-Only Access Contract For `wiki/` And `raw/`

These rules keep `wiki/` and `raw/` provenance exact, and keep them safe if a
context-compression tool such as Headroom is ever run in front of the model.

**Route wiki/raw through the MCP tools.** When the host exposes the LLM Wiki MCP
server, read and search `wiki/` and `raw/` through the framework-owned tools —
`llm_wiki_read`, `llm_wiki_search`, and `llm_wiki_search_all` — rather than
shell-family file reads/search (`cat`, `sed`, `grep`). On Claude Code the native
`Read` tool is an acceptable fallback for a single wiki/raw file; on Codex (which
has no native read tool) the MCP tools are the primary surface. These outputs are
produced by llm-wiki itself, so read this way they are exact at the MCP server
boundary; if an optional host/proxy later replaces content with CCR markers,
compression envelopes, or omitted payload fields, treat that as a failed
delivery check rather than exact content.

**`headroom_read` ban.** If you run Headroom, do not enable `HEADROOM_MCP_READ=on`
and do not call the `headroom_read` MCP tool against any `wiki/` or `raw/` path.
That tool routes file content through CCR with retrieval markers, which would
defeat exact provenance. Headroom is an optional, unmanaged external runtime; the
framework does not bundle or configure Headroom itself, but it does ship one
launch convenience —
`llm-wiki headroom [--headroom-bin <PATH>] [--unsafe-mcp-read] [--] <headroom args...>` —
which execs the real `headroom` binary with `HEADROOM_MCP_READ=off` and
`*llm_wiki*` plus the explicit `llm_wiki_*` MCP tool names added to
`HEADROOM_EXCLUDE_TOOLS`. Exact wiki/raw
provenance still comes from routing reads/search through the `llm_wiki_*` MCP
tools; `HEADROOM_EXCLUDE_TOOLS` is best-effort only and is not a provenance
boundary. Known `headroom-ai 0.24.0` Codex/OpenAI-Responses runs did not consult
that exclude list for normal tool-output compression; inspected
`headroom-ai 0.32.0` source appears to honor Responses excludes, so live
Headroom runs must record the exact version/path and treat CCR markers,
compression envelopes, or omitted payload fields as failed delivery checks.
Current Headroom support is Option A: compact search is supported for discovery,
while exact large `wiki/` and `raw/` reads should be performed outside Headroom.
The framework does not add read pagination for Headroom in the current product
posture.

## Core Rule

Document tested truth, not intended truth.

The wiki must always make it clear whether a statement is:

1. validated reality (spec, decision)
2. future direction under consideration (proposal)
3. an execution plan (roadmap, plan)
4. evidence from investigation (experiment, eval)
5. a durable choice (decision)

## Promotion Rule

Knowledge flows from raw sources through the wiki toward validated truth:

1. New information enters `raw/`. The agent ingests it.
2. Ingest produces or updates proposals, references, or entity pages.
3. Accepted proposals get a roadmap when they need deliverable coordination.
4. Deliverables get plans when they need execution sequencing.
5. Implementation, prototypes, experiments, or evaluations produce evidence.
6. Validated durable outcomes promote to specs and decisions.
7. Completed or rejected documents move to `wiki/archive/`.

If an idea fails, validated truth does not change. Record the failure in the
experiment, eval, or plan outcome.

## Deliverables

A deliverable is the smallest externally observable result that moves the
project forward.

Deliverables live as sections inside a roadmap. A complex deliverable may have
its own plan.

Each deliverable answers:

1. **Promise** - what becomes true for a user, developer, operator, or evaluator?
2. **Included** - what is inside this deliverable?
3. **Excluded** - what is deliberately out of scope?
4. **Depends On** - what must already be true?
5. **Execution Plan** - which `*.plan.md` owns the detailed work?
6. **Proof** - what command, test, demo, or measurement proves completion?
7. **Promotion Target** - what spec or decision should be updated after validation?
8. **Unlocks** - what later deliverables become possible?

The deliverable rule:

> If the result cannot be observed or verified, the work unit is too vague.

## Roadmap Template

```md
### D1 - [Deliverable Name]

Status: [Draft / Active / Blocked / Completed]
Promise: [Externally observable result]
Depends On: [None / D0 / external prerequisite]
Execution Plan: [path or "Not created yet"]

Included:
- ...

Excluded:
- ...

Proof:
- ...

Promotion Target:
- ...

Unlocks:
- ...
```

## Plan Template

A plan answers:

1. What deliverable or workstream does this execute?
2. What files, modules, systems, or workflows are in scope?
3. What is explicitly out of scope?
4. What steps will be taken?
5. What verification gates must pass?
6. What evidence will be recorded?
7. What wiki pages should be updated when done?
8. What closes the plan?

## Spec Template

A spec answers:

1. What is currently accepted and validated?
2. What architecture, API, data flow, schema, or behavior exists now?
3. What is deliberately not yet validated?
4. What commands, tests, measurements, or artifacts prove the claim?
5. What limitations remain?

## Decision Template

A decision answers:

1. What choice was made?
2. Why was it made?
3. What alternatives were considered?
4. What consequences or tradeoffs are accepted?
5. What evidence or plan supports the decision?
6. What would cause the decision to be revisited?

## Definition Of Done

Work is done only when:

1. the implementation exists
2. relevant automated tests or checks pass
3. required evidence is recorded (tests, demos, logs, measurements, evals)
4. affected wiki pages are updated (specs, decisions, index, log)
5. durable decisions are recorded
6. future-only ideas remain in proposals or roadmaps
7. procedures are updated when repeatable operations changed
8. completed plans and roadmap deliverables have their status updated
9. `wiki/index.md` reflects the current state
10. `wiki/log.md` records what changed

## Deliverables-First Execution

Frame work as deliverables, not subsystems.

Prefer:

```
Gallery proves the full mobile chat/research/cache workflow.
```

Avoid:

```
Build shared UI, cache, API, auth, model orchestration, and retrieval.
```

Good deliverables are:

1. externally observable
2. small enough to verify in one sitting
3. tied to proof: a command, test, screenshot, demo, or measurement
4. explicit about exclusions
5. connected to a roadmap and, when needed, a plan

Before starting work, ask:

> What is the smallest observable thing this task delivers, and what future
> work are we explicitly refusing to do inside it?

## Common Failure Modes

1. writing future intent into specs
2. using roadmaps as proof of progress
3. letting plans become the only source of truth
4. creating architecture work without an observable deliverable
5. leaving durable decisions only in conversation
6. running experiments without recording setup and outcome
7. changing APIs or schemas without updating specs after validation
8. claiming model performance without eval conditions
9. expanding scope inside a deliverable without updating the roadmap
10. creating placeholder documents that imply progress that does not exist
11. skipping index.md updates after wiki changes
12. ingesting without checking for contradictions with existing wiki content
13. allowing orphan pages to accumulate without lint

## Agent Reading Order

When starting work on a project, the agent reads:

1. `AGENTS.md` - schema and conventions
2. `project_guidelines.md` - this file
3. `wiki/index.md` - orient to all project knowledge
4. relevant wiki pages identified from the index
5. raw sources only when wiki content is insufficient

The agent should never need to browse the filesystem to find information.
`wiki/index.md` is the single entry point.

## Bottom Line

The human curates sources and makes judgment calls. The agent compiles,
maintains, cross-references, and keeps the wiki consistent.

1. raw sources enter through human curation
2. ingest compiles sources into typed wiki pages
3. queries run against the compiled wiki
4. lint keeps the wiki healthy
5. specs and decisions record validated truth
6. roadmaps coordinate deliverables
7. plans execute bounded work
8. tests, demos, and operational checks produce evidence
9. the index is always current
10. knowledge compounds with every ingest
