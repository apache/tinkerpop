<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
-->

# Functional Testing — Reference

The mechanics behind SKILL.md step 4. *Whether* to run a functional test and
*what* to test are judgment calls that live in the playbooks' **Verify** sections
(`general.md` holds the shared gate and battery framework; domain playbooks
specialize). This file covers only the reproducible half: building the PR and
standing up a server to test against.

## Why a second server

Phase 1's knowledge-graph server (`infrastructure/docker.js`) runs the **stock
published image** — it holds the review graph, not the PR's code. Functional
testing must exercise the PR's **own compiled artifacts**, so it builds from
source and launches natively. The two servers run on different ports and never
share state.

## `functional/cli.js`

| Command | What it does |
|---------|--------------|
| `start --workDir <dir> [--port <n>]` | Reads `pr`/`repoPath` from `session.json`; adds a `build/` worktree on `pr-review/<pr>`; runs `mvn clean install -DskipTests` over the full reactor; locates the `*-standalone` assembly; writes a TinkerGraph config, an init script (binding `g` and `a`), and a server yaml; launches `bin/gremlin-server.sh` on a free port; polls until ready. Prints the handle as JSON and persists it to `functional.json`. |
| `stop --workDir <dir>` | Reads `functional.json`, kills the server JVM, and removes the `build/` worktree. Also invoked automatically by `review.js` teardown. |

The handle: `{ port, url, pid, buildWorktree, assemblyDir, logFile }`. `url` is
the HTTP endpoint (`http://localhost:<port>/gremlin`) to hand the subagent.

The build is the slow step (a full reactor build, minutes). If readiness times
out, the error names `functional-server.log` in the work dir — inspect it for the
JVM's own startup errors.

## What the built server exposes

Mirrors the Phase-1 server so a reviewer's queries look the same against either:

- `g` — a standard traversal source over an empty TinkerGraph
- `a` — the same graph `withComputer()`, for OLAP steps (`connectedComponent()`, …)

Serializers are the project defaults (GraphSON V4 + GraphBinary V4) over the
`HttpChannelizer`, so any current GLV client connects normally.

## Driving it from the subagent

The subagent connects to `url` and submits traversals — via a GLV client or the
Gremlin Console — exactly as a user reading the docs would. It gets the server
URL, the PR title/description, the relevant docs, and the relevant Gherkin
features; it does **not** get source, the graph, or the review findings. See
SKILL.md step 4 for the full briefing contract and the Verify sections for the
per-change battery.

## Layer 1 — embedded, through the built console

When the Verify sections call for an embedded exercise (semantics changed, API
stable), there is no server to start: the subagent drives the PR's own console
against an in-process TinkerGraph. `start` has already built it — the console
assembly sits next to the server's in the build worktree:

```bash
<buildWorktree>/gremlin-console/target/apache-tinkerpop-gremlin-console-*-standalone/bin/gremlin.sh -e scenarios.groovy
```

`-e` runs the script in execution mode and returns. Hand the subagent the path
to `gremlin.sh` (with the `*` resolved) in place of a server URL, and these
gotchas, which otherwise cost it a few failed runs:

- **No top-level `def`.** The console evaluates a script statement by statement,
  so a `def`-declared variable is local to its statement and gone by the next
  line. Assign without `def` (`g = TinkerGraph.open().traversal()`), as the reference
  docs' execution-mode examples do. `def` inside a closure or method is fine.
- **End with `System.exit(0)`** if the console is still running after the last
  statement — a non-daemon thread (a remote connection, an executor) keeps the
  JVM alive.
- **Print what you assert.** Output is the only record: `println` each result,
  or use `assert`, which stops the script on the first failure — so label every
  scenario and print before asserting.

The same scenario labels and complete-code rules below apply to a console script.

## What the subagent must return (report inputs)

The test code is the source of truth for *what* was tested, so the subagent
returns it **complete and unabbreviated**, with every scenario labeled in a
comment carrying a stable id and one-line intent:

```groovy
// Scenario A1: group().by(T.id) — id key survives when its reduction is productive
g.V().group().by(id).by(out().count())
```

Those labels are the anchor between the report's two functional sections:

- `appendixFunctional.testCode` — the full labeled battery (raw text; the
  renderer wraps it — the subagent must not pre-wrap in `<pre>`/`<code>`).
- `functionalTest` — **themes, not a scenario dump**. `results` rows are
  theme-level, each citing the labels it spans (e.g. "Barrier family (A4–A12)");
  `plan`/`observations` surface what was exercised and what was learned. The row
  count must not imply more tests than the appendix lists.

The renderer defensively strips a stray wrapper from the raw-text fields, but the
contract is raw text — relying on the guard is a slip, not a plan.

## Isolation notes

- The `build/` worktree is separate from the enrichment worktree (`src/`) so
  Maven's `target/` output never pollutes the tree the agent reads.
- Teardown removes the `build/` worktree and stops the JVM. `start` records the
  worktree in `functional.json` before it builds, and removes it itself when the
  build or server start fails, so a failed or interrupted start never leaves it
  behind for teardown to miss.
- The build worktree is checked out from the PR, so the server config written
  into it must suit the PR's branch. It sets only what a review needs; metrics
  are left out because their setting names differ between branches (master
  renamed `interval` to `intervalMillis`).
