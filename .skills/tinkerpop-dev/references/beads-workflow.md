<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Beads Workflow for TinkerPop

TinkerPop uses [beads](https://github.com/steveyegge/beads) (`bd`) as the agent planning,
execution, and long-term memory system, backed by a shared DoltHub database
(`tinkerpop/tinkerbeads`). This reference covers TinkerPop-specific conventions that differ
from or extend the beads defaults.

## Setup

New contributors run the bootstrap script from the repo root:

```bash
bin/beads-bootstrap.sh
```

Prerequisites: `bd` and `dolt` installed, valid DoltHub credentials (`dolt login`).

## Core Rules

- **Default**: Use beads for ALL planning and tracking (`bd create`, `bd ready`, `bd close`)
- **Scope**: Beads captures both concrete tasks *and* open design questions — create a bead
  whenever a question, decision, or trade-off needs to be preserved, not just when there is
  code to write
- **Prohibited**: Do NOT use TodoWrite, TaskCreate, or markdown files for task tracking
- **Workflow**: Create a beads issue BEFORE writing code, mark `in_progress` when starting
- **Commit messages**: Every commit must include bead ID(s) as `(tinkerpop-NNN)` on its own
  line, placed above the `Assisted-by:` trailer. Validate this before committing:
  ```
  Add vertex label support to GraphSON serializer

  (tinkerpop-123)
  Assisted-by: Claude:claude-sonnet-4-6 [Claude Code]
  ```
- **Bias toward persistence**: When in doubt, capture it in a bead. Lost reasoning is harder
  to recover than an unused issue.
- **Session start**: Check `bd ready` for available work before beginning any session

## Essential Commands

### Finding Work
- `bd ready` - Show issues ready to work (no blockers)
- `bd list --status=open` - All open issues
- `bd list --status=in_progress` - Your active work
- `bd show <id>` - Detailed issue view with dependencies

### Creating & Updating
- `bd create --title="Summary of this issue" --description="Why this issue exists and what needs to be done" --type=task|bug|feature --priority=2` - New issue
  - Priority: 0-4 or P0-P4 (0=critical, 2=medium, 4=backlog). NOT "high"/"medium"/"low"
- `bd update <id> --claim` - Claim work
- `bd update <id> --assignee=username` - Assign to someone
- `bd update <id> --title/--description/--notes/--design` - Update fields inline
- `bd close <id>` - Mark complete
- `bd close <id1> <id2> ...` - Close multiple issues at once (more efficient)
- `bd close <id> --reason="explanation"` - Close with reason
- **Tip**: When creating multiple issues/tasks/epics, use parallel subagents for efficiency
- **WARNING**: Do NOT use `bd edit` - it opens $EDITOR (vim/nano) which blocks agents

### Dependencies & Blocking
- `bd dep add <issue> <depends-on>` - Add dependency (issue depends on depends-on)
- `bd blocked` - Show all blocked issues
- `bd show <id>` - See what's blocking/blocked by this issue

### Sync & Collaboration
- **WARNING**: Do NOT use `bd dolt push` - that is a manual task left to human maintainers
- `bd dolt pull` - Pull beads from Dolt remote
- `bd search <query>` - Search issues by keyword

### Project Health
- `bd stats` - Project statistics (open/closed/blocked counts)
- `bd doctor` - Check for issues (sync problems, missing hooks)
- `bd doctor --check=conventions` - Check for convention drift (lint, stale, orphans)

### Quality Tools
- `bd create --validate` - Check description has required sections
- `bd create --acceptance="criteria"` - Set acceptance criteria (checked by --validate)
- `bd create --design="decisions"` - Record design decisions
- `bd create --notes="context"` - Add supplementary notes
- `bd config set validation.on-create warn` - Auto-validate on every create
- `bd lint` - Check existing issues for missing sections

### Lifecycle & Hygiene
- `bd defer <id> --until="date"` - Defer work to a future date
- `bd supersede <id> --with=<new-id>` - Mark issue as superseded
- `bd close <id> --suggest-next` - Show newly unblocked issues after closing
- `bd stale` - Find issues with no recent activity
- `bd orphans` - Find issues with broken dependencies
- `bd preflight` - Pre-PR checks (lint, stale, orphans)
- `bd human <id>` - Flag for human decision (list/respond/dismiss)

### Structured Workflows
- `bd formula list` - See available workflow templates
- `bd mol pour <name>` - Start structured workflow from formula

## Labels

Labels provide multi-dimensional categorization orthogonal to type and priority. An issue
can carry multiple labels simultaneously, enabling cross-cutting views of the issue graph.

```bash
# At creation
bd create --title="..." --labels="gremlin-core,3.8"

# After creation
bd label add <id> <label>
bd label remove <id> <label>
bd label list <id>          # labels on a specific issue
bd label list-all           # all labels in use across the database
```

Suggested label dimensions for TinkerPop:
- **Module**: `gremlin-core`, `gremlin-server`, `gremlin-python`, `gremlin-javascript`, `gremlin-dotnet`, `gremlin-go`, `tinkergraph`
- **Cross-cutting concern**: `serialization`, `traversal`, `driver`, `docs`, `breaking-change`, `deprecation`

These are conventions, not enforced values. Use `bd label list-all` to see what labels are
already in use before introducing new ones.

## Issue Lifecycle

```
open → in_progress → closed
```

Close a bead when its associated commit has been made and the operator has approved the
commit message. The bead represents completed local work; the DoltHub push (which makes
it visible to others) is a separate, deferred maintainer action.

## Issue Content Standards

Every issue must carry enough context to be understood without a side conversation.
Use these fields when creating:

```bash
bd create \
  --title="Short imperative summary" \
  --description="What the problem is and why it matters" \
  --design="Decisions made, alternatives considered, approaches rejected" \
  --acceptance="Testable definition of done" \
  --labels="module,release-target" \
  --type=bug|feature|task \
  --priority=0-4
```

- **`--description`**: The *why*, not just the *what*. A reader should understand the
  motivation without prior context. For open design questions, state what is unresolved
  and why it matters.
- **`--design`**: Record decisions made, alternatives considered, and approaches rejected.
  This is where the reasoning lives — future sessions and agents read this to understand
  the path that led to the current state, not just what the current state is.
- **`--acceptance`**: What does done look like? Should be verifiable (test passes, behavior
  observed, doc updated). For open questions, the condition under which the bead can close.

Use `bd create --validate` to check completeness before finalizing.

## JIRA Linking

When a TinkerPop JIRA ticket exists for a bead, record it via `--external-ref`. This can
be set at creation time or added later when the operator supplies the ticket identifier:

```bash
# At creation
bd create --title="..." --external-ref="TINKERPOP-3456" ...

# Added later
bd update <id> --external-ref="TINKERPOP-3456"
```

## Daily Workflow

**Finding and claiming work:**
```bash
bd ready                    # show unblocked issues
bd show <id>                # review details
bd update <id> --claim      # claim it
```

**Completing work:**
```bash
# 1. Ensure commit message includes the bead ID above the Assisted-by trailer
# 2. Get operator approval on the commit message
# 3. Close the bead
bd close <id>
```

**Checking project state:**
```bash
bd stats                    # open/closed/blocked counts
bd blocked                  # issues with unresolved blockers
bd list --status=in_progress  # all active work
```

**Creating dependent work:**
```bash
# Run bd create commands in parallel (use subagents for many items)
bd create --title="Implement feature X" --description="Why this issue exists and what needs to be done" --type=feature
bd create --title="Write tests for X" --description="Why this issue exists and what needs to be done" --type=task
bd dep add beads-yyy beads-xxx  # Tests depend on Feature (Feature blocks tests)
```

## DoltHub Push Policy

**Contributors do not push to DoltHub.** `bd dolt push` is a maintainer action performed
after PRs merge. Beads are closed locally as work completes; the push to DoltHub defers
visibility of that closed state until the work has been reviewed and merged.

Maintainers run after merging a batch of PRs:
```bash
bd dolt pull   # integrate any remote changes first
bd dolt push   # publish to tinkerpop/tinkerbeads
```

## What Not To Do

- Do not run `bd dolt push` as a contributor — this is a maintainer action post-merge
- Do not use `bd edit` — it opens an interactive editor that blocks agents
- Do not serialize issue creation or task execution when parallelization is possible
