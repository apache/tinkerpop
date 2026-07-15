---
name: tinker-review
description: >
  Graph-based PR review for Apache TinkerPop. Builds a knowledge graph from
  PR source code, enriches it with semantic relationships guided by domain
  playbooks, runs structural analysis, and produces an HTML evidence package.
  Use when asked to review a TinkerPop PR by number.
license: Apache-2.0
compatibility: Requires Docker, Node.js 20+, git. Network access for fetching PR refs.
metadata:
  version: "0.1.0"
  project: Apache TinkerPop
---

# Graph Review Skill

## Prerequisites

- Docker running (for Gremlin Server)
- `upstream` remote pointing to `git@github.com:apache/tinkerpop.git` (fetch only)
- Node.js 20+ with dependencies installed in `.skills/tinker-review/`


## References (load on demand)

- Read [references/schema.md](references/schema.md) when you need to understand what vertices, edges, or properties exist in the knowledge graph (typically during enrichment or when writing raw Gremlin)
- Read [references/interfaces.md](references/interfaces.md) when you need the exact function signatures or data type definitions for a module
- Read [references/enrichment-cli.md](references/enrichment-cli.md) when you need to know what an enrichment CLI command *does* and when to reach for it (the command names below are terse; this is where their meaning lives)
- Read [references/functional-testing.md](references/functional-testing.md) when you run the optional functional test (step 4) — what `functional/cli.js` does, how the built server is configured, and how to drive it from the subagent

## Execution Sequence

When invoked with `/review <pr-number>`:

The run has two phases (see [DESIGN.md](DESIGN.md)). **Phase 1** (step 1) is
deterministic and builds the graph. **Phase 2** (steps 3–5) is agent-driven —
enrichment, an optional functional test, then the report. Step 2 chooses the
playbooks that guide Phase 2; step 6 tears down.

### 1. Setup + Phase 1 (deterministic)

Run the review script. This handles everything mechanical:

```bash
npm install --prefix .skills/tinker-review  # only needed once
node .skills/tinker-review/scripts/review.js <pr-number> <repo-path>
```

This performs: fetch PR → create worktree → start Gremlin Server → extract
structure via Tree-sitter → populate knowledge graph → discover discussions
(JIRA, dev list, proposals, PR comments) → run pattern checks (completeness,
coverage gaps, centrality, blast radius, cluster analysis) → write evidence JSON.

**Output:** `/tmp/pr-review-<pr>/evidence.json`
**Server:** remains running (the agent needs it for enrichment)
**Worktree:** available at `/tmp/pr-review-<pr>/src/`

If re-running, the script cleans up stale worktrees/branches automatically.

### 2. Choose the playbooks

Playbooks carry the domain judgment for Phase 2. `general.md` always applies;
select the domain playbooks in two passes:

1. **Orient** — from the changed file paths, gather the candidates:
   - `gremlin-dart/`, `gremlin-go/`, `gremlin-python/`, `gremlin-dotnet/`, `gremlin-js/` → `playbooks/glv.md`
   - `gremlin-core/` with new step patterns → `playbooks/new-step.md`
   - `gremlin-driver/`, `gremlin-server/`, `gremlin-util/` → `playbooks/driver-server.md`
   - Small change set with linked issue → `playbooks/bug-fix.md`
   - `gremlin-language/` or `*.g4` → `playbooks/grammar.md`
   - Deletion-heavy change set (removes a feature/module/dependency; `listDeleted` returns entries) → `playbooks/removal.md`
2. **Confirm** — read each candidate's **Context** and keep only the ones that
   truly fit this PR. A path can match a playbook that doesn't apply: a
   `gremlin-core/` change that adds no step, or a `gremlin-driver/` fix that's
   really a bug-fix.

Then apply each kept playbook's sections at the point each is used:

The three working sections (**Enrich**, **Inspect**, **Interpret**) are bullet
checklists — one item per action.

| Section | Used when | What you do |
|---------|-----------|-------------|
| **Context** | choosing playbooks (above) | Confirm the path-matched playbook fits this PR; set aside the ones that don't. |
| **Enrich** | improving the graph (step 3) | Run each command bullet to add or re-grade semantic edges. Graph mutation only. |
| **Inspect** | reading the changed source (step 3) | Check each bullet against the source; record what you find as a *candidate finding* for Interpret. |
| **Verify** | functional testing (step 4) | Apply the gate, then shape the blind subagent's test battery. `general.md` holds the shared gate/framework; domains specialize it. |
| **Interpret** | writing the report (step 5) | Weigh the named `evidence.json` fields together with the Inspect and Verify candidates into `findings` / `openQuestions`. |
| **Escape** | any time | Honor its stop/escalate gates; halt or flag when one holds. |

The working sections split by data flow: **Enrich** writes to the graph,
**Inspect** reads the changed source into candidate findings, **Verify** exercises
the built feature as a user would, and **Interpret** reads the computed
`evidence.json` checks and weighs the Inspect and Verify candidates into the
report. `findings` is an ordered list — Interpret ranks it most-severe-first,
grading each entry blocking / high / low.

Phase 1 already computes every structural check — completeness, coverageGaps,
centrality, blastRadius, clusters, confidence, externals, orphans — into
`evidence.json`. Playbooks' Interpret sections reference those results **by field
name** (e.g. `checks.blastRadius`); they do not re-run checks. The shape and
meaning of every field is documented in
[references/interfaces.md](references/interfaces.md) (`Evidence`), which points
to the per-check `@typedef`s in `scripts/patterns/*.js`.

### 3. Phase 2 — Enrichment

The Gremlin Server is still running. Use `scripts/enrichment/cli.js` to
read from and write to the knowledge graph. Do NOT write your own connection
code or import gremlin-js directly — the CLI handles connections for you. For
what each command does and when to use it, see
[references/enrichment-cli.md](references/enrichment-cli.md).

Every command takes `--workDir /tmp/pr-review-<pr>`, connects using that dir's
`session.json`, prints its JSON result to stdout, and disconnects:
```bash
node scripts/enrichment/cli.js <command> --workDir /tmp/pr-review-<pr> [--options...]
```

The command catalog — what each does and when to reach for it — lives in
[references/enrichment-cli.md](references/enrichment-cli.md); run
`node scripts/enrichment/cli.js --help` for exact flags. In brief: **read
commands** (`listFunctions`, `listDeleted`, `listExternalRefs`, …) orient you and
feed the playbooks; **write commands** (`mapStep`, `addReference`,
`setEdgeConfidence`, …) add the semantic edges. Edges you create default to
`INFERRED` — pass `--confidence AMBIGUOUS` for a flagged guess or `EXTRACTED`
when the source states it directly.

The confidence loop is the backbone of enrichment: `auditConfidence` →
`listInferred` (your verification worklist) → read the source → `setEdgeConfidence`
to promote or downgrade, then re-run `auditConfidence` and reflect anything still
`AMBIGUOUS` in `openQuestions`. It is defined once in `general.md`'s **Enrich**
section and inherited by every playbook.

**Read source files:** worktree at `/tmp/pr-review-<pr>/src/`

Follow the playbook's Enrich section (graph writes) and its Inspect section (read
the changed source, recording concerns as candidate findings for Interpret to
weigh at report time). Check Escape conditions.

### 4. Phase 2 — Functional Testing (optional, subagent)

**IMPORTANT: Functional testing is NOT running the project's unit tests.**
DO NOT use `mvn test`. DO NOT run TreeTest, DO NOT run any existing test class.
The project's own tests are the author's responsibility — running them tells
the reviewer nothing new.

Functional testing means: test the feature AS A USER WOULD. Connect to a
Gremlin Server built from the PR and submit traversals. Try to use the feature
based only on what the documentation says. Try to break it with adversarial
inputs.

**Gate — decide whether to run it.** Consult the applicable playbooks' **Verify**
sections. `general.md`'s Verify holds the shared gate: skip functional testing
when the change has no user-facing runtime surface (tests-only, docs-only, a pure
internal refactor, build/CI plumbing) and either omit `functionalTest` or record
the skip reason. Otherwise continue.

**Build the PR and start the server (mechanical).** The `functional/cli.js`
command builds the full reactor, locates the assembly, and launches a Gremlin
Server from the built artifacts on a fresh port (distinct from the knowledge
graph server). It reads `pr`/`repoPath` from `session.json` and writes the
handle to `functional.json` so teardown can find it:

```bash
node .skills/tinker-review/scripts/functional/cli.js start --workDir /tmp/pr-review-<pr>
```

This prints `{ url, port, pid, assemblyDir, logFile, ... }`. The build is slow
(full reactor); on failure, the error names the server log to inspect. See
[references/functional-testing.md](references/functional-testing.md) for what the
command does and how to drive it.

**Test from the outside.** Spawn a subagent that receives ONLY:
- PR title/description (what the change claims to do)
- Relevant documentation sections
- Relevant Gherkin test features (for expected behavior reference)
- The Gremlin Server URL (the `url` from the command above)

The subagent does NOT get: source code, the knowledge graph, code review findings,
or access to the analysis worktree. Brief it as a **minimally experienced
TinkerPop user** who has only the docs — it tests blind, and its stumbles are
signal about how usable the feature is.

**The subagent tests by submitting Gremlin traversals** via the Gremlin Console
or a GLV client. It devises its own test plan from the docs, executes traversals,
and attempts adversarial edge cases. The **shape** of the battery comes from the
applicable playbooks' Verify sections — which languages/layers to exercise and
what adversarial cases matter for this class of change.

**Layer decision** (the Verify sections say which applies to this PR):
- **Layer 1 (embedded):** Gremlin Console with TinkerGraph. For core logic changes.
- **Layer 2 (per-GLV wire):** Connect from each GLV to the server. For
  serialization/type changes. Skip if purely computational.

**Label every scenario in the code (required).** Instruct the subagent to keep
the test code as the single source of truth for *what* was tested, and to label
each scenario with a comment carrying a stable id and a one-line intent, e.g.:

```groovy
// Scenario A1: group().by(T.id) — validates the id key survives when its reduction is productive
g.V().group().by(id).by(out().count())
```

The subagent returns:
- **Complete, unabbreviated test code**, every scenario labeled as above — this is
  `appendixFunctional.testCode` (raw text; the renderer wraps it — do NOT add
  `<pre>`/`<code>`). No `...`, no "(abbrev)"; if the battery is large, that is
  what the appendix is for.
- **Themes, not a scenario dump** — group the scenarios into a handful of themes,
  each citing the labels it spans (e.g. "Barrier family (A4–A12): …").
- Results, adversarial findings, and observations keyed to those labels.

Fold these into the `functionalTest` / `appendixFunctional` report fields (step 5)
and weigh them in Interpret. The count and granularity of `functionalTest.results`
rows must match the themes, not inflate to imply more tests than the appendix
actually lists.

### 5. Phase 2 — Produce Report

Write the narrative JSON, then render.

**Step A:** Read `/tmp/pr-review-<pr>/evidence.json`. Add narrative fields to
produce a complete evidence-with-narrative JSON file. Write it to
`/tmp/pr-review-<pr>/report.json`. The narrative fields you must provide:

- `summary` — HTML paragraph describing the PR
- `clusters.assessment` — HTML prose about what the connected-component clusters mean
- `communityAssessment` — HTML, **light by default**: usually one or two sentences on
  whether the change is coherent/localized and what its dominant theme is. Expand *only*
  when the community **structure itself** shows something non-obvious that no other section
  captures — e.g. disconnected communities hinting at bundled unrelated changes, a community
  bridging subsystems that shouldn't be coupled, a changed file stranded in a community about
  an unrelated concern, or diffuse modularity on a supposedly focused change. Absent such a
  structural surprise, keep it short and instead use the communities as *supporting evidence*
  cited in other sections (Guided Walk, Findings, Change Coherence). Do NOT restate the graph
  literally (vertices/edges/labels/counts), do NOT re-narrate what Removal References,
  Coverage, or Blast Radius already report, and do NOT pad. The renderer draws the diagram
  and the appendix lists membership; you supply only the judgment.
- `guidedWalk` — array of `{ title, badge, badgeText, body }` objects
- `findings` — array of `{ title, snippet, body }` objects, ordered most-severe-first (Interpret grades each blocking / high / low)
- `openQuestions` — array of `{ title, body, meta }` objects
- `functionalTest` — `{ plan, results: [{name, pass, output}], observations }` (if testing was done).
  `plan` and `observations` are HTML and surface **themes and insights** — what
  families of behavior were exercised and what was learned — not a per-scenario
  list. `results` rows are **theme-level**: each `name` names a theme and the
  scenario labels it spans (e.g. `"Barrier family (A4–A12)"`), so the PASS/FAIL
  grid stays scannable and every row is backed by labeled code in the appendix.
  Do not enumerate every scenario here and do not let the row count imply more
  tests than the appendix lists.
- `appendixFunctional` — `{ environment, testCode, fullOutput }` (if testing was done).
  `environment` is HTML. `testCode` and `fullOutput` are **raw text** — the
  renderer wraps them in `<pre><code>`, so do NOT add `<pre>`/`<code>`/`<p>`
  yourself. `testCode` is the **complete, unabbreviated** labeled battery from the
  subagent (see step 4); it is the source of truth the Functional Test section
  summarizes by label.

All `body` fields (and `functionalTest.plan`/`observations`, `appendixFunctional.environment`)
are HTML — use `<code>`, `<strong>`, `<ul>`, `<p>` as needed. The raw-text fields
noted above are the exception. The renderer handles all layout, CSS, and structure.

**Step B:** Render the report:

```bash
node .skills/tinker-review/scripts/renderer/render.js /tmp/pr-review-<pr>/report.json /tmp/pr-review-<pr>/report.html
```

The renderer produces a complete HTML report with consistent structure.
Every section is always present — if you didn't provide a field, it shows
"Section not provided" in red. DO NOT write HTML yourself.

### 6. Teardown

```bash
node -e "import { teardown } from './scripts/review.js'; await teardown('/tmp/pr-review-<pr>');"
```

Or simply stop the Docker container and clean up manually:
```bash
docker stop $(cat /tmp/pr-review-<pr>/session.json | python3 -c "import json,sys; print(json.load(sys.stdin)['containerId'])")
rm -rf /tmp/pr-review-<pr>
git worktree prune
git branch -D pr-review/<pr>
```

This stops the knowledge graph server, removes worktrees, deletes the branch.
If a functional test ran (step 4), teardown also stops that server (via its
`pid` in `functional.json`) and removes the `build/` worktree. Call this ONLY
after all phases are complete.

## Important Notes

- NEVER push to the `upstream` remote. It is fetch-only.
- ALL output goes to `/tmp/pr-review-<pr>/` — never write inside the git repo.
- The Gremlin Server stays alive until teardown. Don't kill it early.
- If re-running, the script auto-cleans stale state.
- The agent populates JSON narrative fields; the renderer produces HTML. No agent writes raw HTML.
