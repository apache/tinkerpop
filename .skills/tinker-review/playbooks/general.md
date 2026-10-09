# Playbook: General (applies to all PRs)

## Context
These are concerns TinkerPop reviewers consistently raise regardless of
the type of change. This playbook always applies in addition to any
domain-specific playbook.

## Enrich
The confidence pass runs on every review; it lives here and every playbook
inherits it. Run in order:
- `auditConfidence` — read the edge-confidence distribution and the `AMBIGUOUS` list.
- `listInferred --changedOnly` — pull the verification worklist: the INFERRED
  edges touching changed code (`--relation implements_step` first, then the
  `calls` edges your findings lean on). Read each against the worktree source.
  Edges between untouched code are not worth verifying.
- `setEdgeConfidence` — re-grade what you verified: promote a confirmed edge to
  `EXTRACTED`, downgrade a wrong name-resolution to `AMBIGUOUS`.
- `auditConfidence` again — anything still `AMBIGUOUS` goes to `openQuestions`,
  never asserted as fact.

**Project memory** — the PR's beads, when a record named the PR or its
discussions (`discussions.beads.roots` is non-empty). Run these before the
confidence pass so the `governs` edges they write go through it:
- `listBeads` — read the roots, decisions (each chosen one with the alternatives
  it beat) and tasks. The `primary` root is the PR's own work; `owner` and
  `shared` roots are connected work that explains it.
- `linkBead` — link each decision whose design is about changed code to that
  Function, Type or File, chosen and rejected alike.

## Inspect
**Style:**
- Wildcard imports in Java (`import foo.*`)
- Formatting/indentation changes mixed with functional changes
- Unused variables or imports
- Non-final variables that should be final

**Deprecated API:**
- `withRemote` (deprecated in 4.0, use `with_()`)
- Groovy script strings where gremlin-lang should be used
- Any `@Deprecated` API used in new code

**Tests:**
- Tests that drop/clear all data instead of isolating with specialized labels
- Assertions that don't clearly explain what they verify
- Error/exception paths that aren't tested
- Test helpers without guard clauses (missing else/throw for invalid input)

**Project memory** (only when beads were found; read decisions by `listBeads`):
- Chosen decisions are claims to verify, not reasoning to accept. A design
  states facts — a mechanism, an invariant, a measurement, what the choice rules
  out. Check each against the code, and against the build or the functional test
  where the claim is about behavior. Does the diff do something the decision
  rules out? Judge against the newest decision in a `supersededBy` chain.
- Rejected alternatives — each answers "why not X?" only if the reason X lost
  is true of this code, so check that reason the same way. If the diff does X
  anyway, does the reason still hold?
- Where the beads stop, look harder. Changed code no decision governs (no
  `governs` edge after `linkBead`), and any part a bead says was built without
  beads (a decision accepting that earlier commits have none), had its
  reasoning recorded nowhere. Give that code the closest read, not a note.
- Trade-offs argued only in the PR thread — read `discussions.prComments` and the
  JIRA comments for design debates that ended in a choice (concurrency,
  cardinality, compatibility, ...). Each one with no decision bead under the
  PR's root is reasoning that lives only in a comment thread.
- `in_progress` tasks under the PR's root against what the PR says it delivers —
  claimed work that isn't here, or is here half-done.
- Decisions that record a departure from the JIRA, proposal or dev@ thread —
  does the PR description say so too?
- When several roots were loaded, which one does this PR deliver, and what do
  the others show about the connected work it depends on or affects?

**Resource safety:**
- Connections, channels, or streams opened without a clear cleanup path
- Log levels: error for unexpected failures, info for expected lifecycle events
- Concurrency-implicated data structures (`CopyOnWriteArraySet`, synchronized
  collections) introduced without profiling justification

## Verify
Context: this is the shared gate and battery-design framework for the optional
functional test (SKILL.md step 4). Domain playbooks add their own Verify bullets;
they do not repeat this framework.

**Gate — does functional testing run at all?** Run it only when the change has a
user-facing runtime surface. Skip (state why in `functionalTest`, or omit the
field) when the change is tests-only, docs-only, a pure internal refactor with no
observable behavior change, or build/CI plumbing.

**Design the battery to match the change** — exercise what changed, then try the
mistakes a real user would make:
- New or changed **step / API surface** → submit native queries against the built
  server in every affected GLV. A step that spans grammar + core + all GLVs is
  tested per language.
- **Semantics changed, API stable** → a small embedded exercise (Layer 1: Gremlin
  Console / TinkerGraph, or a short Java snippet) that drives the feature is
  enough; per-GLV wire tests add nothing.
- **Serialization / type / protocol** → round-trip the affected types over the
  wire from at least one GLV (Layer 2).
- Always include adversarial cases: wrong argument types, empty/null inputs,
  boundary values, and the feature used against the grain of the docs.

The blind subagent designs and runs this battery from the docs alone — see
SKILL.md step 4 for how it is briefed and isolated. It never gets the beads: the
author's recorded reasoning is as much inside knowledge as the source code.

## Interpret
- `checks.coverageGaps` / `checks.orphans` — a lower bound from the static call
  graph, not a coverage measurement: code tested through GraphFactory, the graph
  API, a strategy or the Gherkin suite counts as untested. Never report the count
  as a finding. Name a gap only after reading the test tree and finding no test
  that reaches the changed behavior, directly or indirectly; then weigh it as a
  test-quality concern alongside the Inspect smells.
- `functionalTest` observations (if testing ran) — a failing or surprising result
  is a finding graded by severity; a documented-but-unusable feature is blocking.
  Adversarial gaps the subagent found (unclear errors, silent wrong answers) are
  high. If testing was skipped, the gate reason is not itself a finding.
- Safety concerns (resource leaks, concurrency risks, missing error handling)
  and test-quality issues — high; make these the focus.
- Style nits and unused variables — low; note them, don't let them dominate.
- Formatting mixed with functional changes — high; it makes the PR harder to
  review and should ideally be separate commits.
- `discussions.beads` — the root is expected to be open: it closes at merge, so
  an open root is never a finding, and neither is finding no beads or a failed
  sync (the report says which).
- A chosen decision's claim that doesn't hold in the code or the build — high:
  the change rests on it. A rejected alternative whose stated reason is false
  of this code — an open question: the road may have been closed for the wrong
  reason.
- A trade-off settled in the PR thread with no decision bead — an open question
  per trade-off, naming it and citing the comments, so it can be recorded before
  merge. Raise it only when the PR has beads; without any, the thread is the
  only record and that is not a finding.
- Code where the beads stop is not a finding by itself; weigh what the closer
  read turns up like any other Inspect candidate.
- Diff does what a rejected alternative ruled out and the reason it lost still
  holds — high. If that reason no longer holds, an open question: was this a
  deliberate reconsideration? Diff contradicts a chosen decision with no newer
  decision superseding it — high.
- An `in_progress` task under the PR's root that the PR doesn't finish — high
  when the PR claims to be complete. An open, unclaimed task — an open question
  only: an epic root spans several PRs, so it may belong to a later one.
- An open bead labelled `human` under a loaded root — an open question no one
  has answered; carry it into `openQuestions`.
- The root's release label doesn't fit `meta.baseBranch` (a `3.7.x` root on a
  PR to `master`) — an open question about where the work should land.
- Deprecated API in new code — high. Deprecated API already present in modified
  code — low, unless the PR is specifically a migration away from it.

## Escape
None — this playbook always completes. No conditions warrant stopping.
