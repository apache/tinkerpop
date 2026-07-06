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
| **Interpret** | writing the report (step 5) | Weigh the named `evidence.json` fields together with the Inspect candidates into `findings` / `openQuestions`. |
| **Escape** | any time | Honor its stop/escalate gates; halt or flag when one holds. |

The three working sections split by data flow: **Enrich** writes to the graph,
**Inspect** reads the changed source into candidate findings, **Interpret** reads
the computed `evidence.json` checks and weighs the Inspect candidates into the
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
running Gremlin Server and submit traversals. Try to use the feature based
only on what the documentation says. Try to break it with adversarial inputs.

**Build the full project:**
```bash
git worktree add /tmp/pr-review-<pr>/build pr-review/<pr>
mvn -f /tmp/pr-review-<pr>/build/pom.xml clean install -DskipTests
```

**Start Gremlin Server from built artifacts** on a random port (different from
the knowledge graph server). Use the built assembly at:
`/tmp/pr-review-<pr>/build/gremlin-server/target/apache-tinkerpop-gremlin-server-*-standalone/`

**Test from the outside.** Spawn a subagent that receives ONLY:
- PR title/description (what the change claims to do)
- Relevant documentation sections
- Relevant Gherkin test features (for expected behavior reference)
- The Gremlin Server URL

The subagent does NOT get: source code, the knowledge graph, code review findings,
or access to the analysis worktree. It tests blind — like a user who read the docs.

**The subagent tests by submitting Gremlin traversals** via the Gremlin Console
or a GLV client. It devises its own test plan from the docs, executes traversals,
and attempts adversarial edge cases.

**Layer decision:**
- **Layer 1 (embedded):** Gremlin Console with TinkerGraph. For core logic changes.
- **Layer 2 (per-GLV wire):** Connect from each GLV to the server. For
  serialization/type changes. Skip if purely computational.

The subagent returns: test plan, results, adversarial findings, exact test code.

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
- `functionalTest` — `{ plan, results: [{name, pass, output}], observations }` (if testing was done)
- `appendixFunctional` — `{ environment, testCode, fullOutput }` (if testing was done)

All `body` fields are HTML. Use `<code>`, `<strong>`, `<ul>`, `<p>` as needed.
The renderer handles all layout, CSS, and structure.

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
Call this ONLY after all phases are complete.

## Important Notes

- NEVER push to the `upstream` remote. It is fetch-only.
- ALL output goes to `/tmp/pr-review-<pr>/` — never write inside the git repo.
- The Gremlin Server stays alive until teardown. Don't kill it early.
- If re-running, the script auto-cleans stale state.
- The agent populates JSON narrative fields; the renderer produces HTML. No agent writes raw HTML.
