# tinker-review — Design

How the skill is built and how to change it. What a review *does* and the
guidance it applies live in `SKILL.md`, the playbooks, and `references/`. This
document is only the machinery and its extension points.

## How it works

The pipeline is two phases, split by what is mechanical and what needs judgment.

**Phase 1 — deterministic** (`scripts/review.js`) builds the graph and writes
`evidence.json`: fetch PR → worktree → start Gremlin Server → tree-sitter
extract → populate → discover discussions → run the structural checks. It exits
cleanly (a `PHASE1_COMPLETE` sentinel) so a caller can detect completion; the
Gremlin Server container stays up.

**Phase 2 — agent-driven** does everything that needs judgment: enrich the graph
via the enrichment CLI, an optional functional test, then the report. It ends by
tearing down the container and worktree.

Data flows one direction through three artifacts:

```
source → [Phase 1] → evidence.json → [agent adds narrative] → report.json → [renderer] → HTML
```

The knowledge graph is a live **Gremlin Server (TinkerGraph)** — the skill
dogfoods TinkerPop. Phase 1 talks to it directly; Phase 2 opens a fresh
connection per enrichment CLI call, because the process that populated the graph
has already exited.

### Module map

| Path | Role |
|------|------|
| `scripts/review.js` | Phase 1 orchestrator — `setup` / `phase1` / `teardown` |
| `scripts/extraction/tree-sitter.js` | source → structural extraction |
| `scripts/graph/*.js` | populate the graph; `confidence.js` / `externals.js` / `references.js` hold the data-model vocabularies and shared edge helpers |
| `scripts/patterns/*.js` | one structural check per file; each defines its own result `@typedef` |
| `scripts/enrichment/{api,cli}.js` | Phase 2 read/write commands over the live graph |
| `scripts/renderer/{render.js,template.html}` | `report.json` → HTML |
| `playbooks/*.md` | domain review guidance — prompt scaffolds, not code |
| `references/{schema,interfaces}.md` | the graph schema and the evidence composite |

## Constraints that bound a change

- **One source of truth per fact.** Each check's result shape is a `@typedef` in
  the pattern file that produces it; `references/interfaces.md` holds only the
  composite `Evidence` / `ReportPackage` and points at those typedefs. The graph
  schema lives once in `references/schema.md`. Never re-declare a shape in a
  second place.
- **Type docs carry meaning.** A `@typedef` field gets a prose line saying what
  it means and how to weigh it, not just its type — that is what the Interpret
  sections and the reviewer consume.
- **Plain JS + JSDoc, no build step.** The skill runs under `node` directly.
  Don't add TypeScript, a bundler, or a compile step.
- **Edges obey the data model.** Every edge carries a `confidence`
  (`EXTRACTED | INFERRED | AMBIGUOUS`); an edge whose endpoint may fall outside
  the changed set is created find-or-create against a marker vertex so it can't
  silently vanish. A new edge must follow both.
- **Mechanical vs judgment decides where code goes.** Anything reproducible is a
  Phase-1 module; anything needing judgment is a Phase-2 command the agent drives
  from a playbook. "Reproducible" means the signal is strong enough to act on
  without judgment — not merely that a script *could* run. Finding references to
  removed code is mechanical (grep a known symbol), so it is a Phase-1 pass
  (`removal-refs.js`); `addReference` stays as the manual escape hatch for the
  non-code cases the pass can't see. Mapping a method to a Gremlin step is *not*
  mechanical — step names collide with ordinary method names, so it needs the
  judgment the GLV playbook describes, and stays a Phase-2 command.

## How to change it

- **Add a structural check** — new `scripts/patterns/<name>.js` exporting the
  function and its result `@typedef`; call it in `review.js` Phase 1 and add it
  to `evidence.checks`; add the type to `Evidence` in `interfaces.md`; reference
  it from the relevant playbook's Interpret.
- **Add an enrichment command** — a function in `scripts/enrichment/api.js` (or a
  pattern module), wired into `cli.js` (COMMANDS + help + switch), documented in
  `SKILL.md`. Edge-creating commands take a `confidence`, default `INFERRED`.
- **Add a playbook** — four sections, each with a job: **Context** states when
  the playbook applies (an applicability gate, read while choosing playbooks),
  **Enrich** uses real enrichment commands, **Interpret** cites `evidence.json`
  fields, **Escape** sets stop/escalate gates. Add an orient rule in `SKILL.md`.
- **Add an edge or vertex type** — document it in `references/schema.md`; tag new
  edges with `confidence`; use find-or-create for cross-boundary endpoints.
