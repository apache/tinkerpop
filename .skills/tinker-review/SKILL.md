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

## Execution Sequence

When invoked with `/review <pr-number>`:

The run has two phases (see [DESIGN.md](DESIGN.md)). **Phase 1** (step 1) is
deterministic and builds the graph. **Phase 2** (steps 3–5) is agent-driven —
enrichment, an optional functional test, then the report. Step 2 loads the
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

### 2. Classify and Load Playbooks

**Always load** `playbooks/general.md` — it applies to every PR.

Then determine which domain-specific playbooks apply from changed file paths:
- `gremlin-dart/`, `gremlin-go/`, `gremlin-python/`, `gremlin-dotnet/`, `gremlin-js/` → `playbooks/glv.md`
- `gremlin-core/` with new step patterns → `playbooks/new-step.md`
- `gremlin-driver/`, `gremlin-server/`, `gremlin-util/` → `playbooks/driver-server.md`
- Small change set with linked issue → `playbooks/bug-fix.md`
- `gremlin-language/` or `*.g4` → `playbooks/grammar.md`
- Deletion-heavy change set (removes a feature/module/dependency; `listDeleted` returns entries) → `playbooks/removal.md`

Load ALL matching playbooks. Execute enrichment for each in sequence.

**How a playbook is applied.** Each playbook has four sections, and each maps to
a phase of this run — this is the contract for what to do with the content:

| Section | When | What you do with it |
|---------|------|---------------------|
| **Context** | framing | Orient to the change type and its risks; not actioned directly. |
| **Enrich** | Phase 2 | Execute the listed steps using the enrichment CLI commands. |
| **Interpret** | Phase 2 (report) | When writing the report, weigh the named `evidence.json` fields into `findings` / `openQuestions`. |
| **Escape** | any phase | Check the stop/escalate conditions; halt or flag when one holds. |

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
code or import gremlin-js directly — the CLI handles connections for you.

**Usage:**
```bash
node scripts/enrichment/cli.js <command> --workDir /tmp/pr-review-<pr> [--options...]
```

**Read commands:**
```bash
node scripts/enrichment/cli.js listFunctions --workDir /tmp/pr-review-<pr> --changed true --visibility public
node scripts/enrichment/cli.js listTypes --workDir /tmp/pr-review-<pr> --kind class
node scripts/enrichment/cli.js getCallsFrom --workDir /tmp/pr-review-<pr> --function <name> --file <path>
node scripts/enrichment/cli.js getCanonicalSteps --workDir /tmp/pr-review-<pr>
node scripts/enrichment/cli.js auditConfidence --workDir /tmp/pr-review-<pr>
node scripts/enrichment/cli.js listInferred --workDir /tmp/pr-review-<pr> --relation implements_step
node scripts/enrichment/cli.js listDeleted --workDir /tmp/pr-review-<pr>          # removal PRs: files the PR deleted + their symbols
node scripts/enrichment/cli.js listExternalRefs --workDir /tmp/pr-review-<pr>     # unresolved callees; flags any matching a deleted symbol
```

`auditConfidence` returns the edge confidence distribution and the list of
AMBIGUOUS edges. Re-run it after enrichment to refresh the audit with the edges
you added, then reflect any remaining AMBIGUOUS links in `openQuestions`.

`listInferred` is your **verification worklist** — the name-resolved / agent-mapped
edges worth a source check (optionally narrowed with `--relation`). After reading
the source, use `setEdgeConfidence` (below) to promote a confirmed edge to
`EXTRACTED` or downgrade a wrong resolution to `AMBIGUOUS`.

**Write commands** (edges you create default to `INFERRED`; pass
`--confidence AMBIGUOUS` for a guess you want flagged, or `EXTRACTED` when the
source states it directly):
```bash
node scripts/enrichment/cli.js mapStep --workDir /tmp/pr-review-<pr> --function <name> --file <path> --step <canonicalName> [--confidence INFERRED|AMBIGUOUS|EXTRACTED]
node scripts/enrichment/cli.js setEdgeConfidence --workDir /tmp/pr-review-<pr> --relation <label> --fromName <name> [--fromFile <path>] [--toName <name>] --confidence <EXTRACTED|INFERRED|AMBIGUOUS>
node scripts/enrichment/cli.js addReference --workDir /tmp/pr-review-<pr> --fromPath <survivingFile> --toPath <deletedFile> --symbol <name> [--location <where>] [--confidence ...]
node scripts/enrichment/cli.js linkDiscussion --workDir /tmp/pr-review-<pr> --url <url> --source jira --title <title> [--confidence ...]
node scripts/enrichment/cli.js linkDoc --workDir /tmp/pr-review-<pr> --entity Step --name <name> --doc <path> [--confidence ...]
node scripts/enrichment/cli.js addGrammarRule --workDir /tmp/pr-review-<pr> --name <name>
node scripts/enrichment/cli.js annotate --workDir /tmp/pr-review-<pr> --label Function --name <name> --key <key> --value <value>
```

Each command connects, executes, prints JSON result to stdout, and disconnects.
The connection info is read from `/tmp/pr-review-<pr>/session.json` automatically.

**Read source files:** worktree at `/tmp/pr-review-<pr>/src/`

Follow the playbook's Enrich section. Check Escape conditions.

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
- `clusters.assessment` — HTML prose about what the clusters mean
- `guidedWalk` — array of `{ title, badge, badgeText, body }` objects
- `findings` — array of `{ title, snippet, body }` objects
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
