# Playbook: General (applies to all PRs)

## Context
These are concerns TinkerPop reviewers consistently raise regardless of
the type of change. This playbook always applies in addition to any
domain-specific playbook.

## Enrich
The confidence pass runs on every review; it lives here and every playbook
inherits it. Run in order:
- `auditConfidence` — read the edge-confidence distribution and the `AMBIGUOUS` list.
- `listInferred` — pull the verification worklist (`--relation implements_step`
  first, then any `calls` edges your findings lean on); read each against the
  worktree source.
- `setEdgeConfidence` — re-grade what you verified: promote a confirmed edge to
  `EXTRACTED`, downgrade a wrong name-resolution to `AMBIGUOUS`.
- `auditConfidence` again — anything still `AMBIGUOUS` goes to `openQuestions`,
  never asserted as fact.

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

**Resource safety:**
- Connections, channels, or streams opened without a clear cleanup path
- Log levels: error for unexpected failures, info for expected lifecycle events
- Concurrency-implicated data structures (`CopyOnWriteArraySet`, synchronized
  collections) introduced without profiling justification

## Interpret
- `checks.coverageGaps` / `checks.orphans` — missing tests on changed code; a
  test-quality concern, weighed alongside the Inspect smells.
- Safety concerns (resource leaks, concurrency risks, missing error handling)
  and test-quality issues — high; make these the focus.
- Style nits and unused variables — low; note them, don't let them dominate.
- Formatting mixed with functional changes — high; it makes the PR harder to
  review and should ideally be separate commits.
- Deprecated API in new code — high. Deprecated API already present in modified
  code — low, unless the PR is specifically a migration away from it.

## Escape
None — this playbook always completes. No conditions warrant stopping.
