# Playbook: Bug Fix

## Context
A fix for a reported issue. Should be minimal, targeted, include a
regression test, and not introduce new API surface. The fix should
address the root cause, not just the symptom.

## Enrich
- `linkDiscussion` — record the referenced JIRA (TINKERPOP-XXXX) or dev-list
  thread (`--source jira|devlist`). Creates the `addresses` edge Interpret checks.

## Inspect
- Fix location — which functions changed to address the bug; does the regression
  test reproduce the reported symptom?
- Scope creep — changes to functions unrelated to the reported bug.
- Error messages — meaningful to users, not just developers? (a common TinkerPop
  reviewer concern)
- Log levels — error stays error for unexpected failures; not downgraded to info
  without justification.
- Resource cleanup on error paths — if the bug involves connection/channel
  handling, no leak when the fix triggers.

## Interpret
- `checks.blastRadius` — high on a bug fix is a warning: verify the fix doesn't
  subtly change behavior for existing callers.
- `checks.centrality` — if a hot function changed, say explicitly that every
  caller needs a behavioral-change check.
- `checks.coverageGaps` / `checks.orphans` — a fix with no new or modified test
  is blocking; it can't be shown to prevent regression.
- `checks.completeness` on `addresses` — no linked issue means correctness can't
  be assessed.
- Out-of-scope changes — flag as "necessary for fix?", not "wrong."

## Escape
- if no linked issue — "Cannot assess whether fix is correct without knowing the bug"
- if public API signature changed — "Bug fix changes public API — needs broader discussion before merge"
