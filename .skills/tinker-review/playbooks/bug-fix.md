# Playbook: Bug Fix

## Context
A fix for a reported issue. Should be minimal, targeted, include a
regression test, and not introduce new API surface. The fix should
address the root cause, not just the symptom.

## Enrich
Identify the fix location — which functions were modified to address
the bug. Trace from the PR title/description to understand the reported
symptom. Check if the regression test actually reproduces that symptom.

Look for:
- Changes outside the issue's scope — modifications to functions not
  related to the reported bug may indicate scope creep
- Error messages: are they meaningful to users, not just developers?
  (A common reviewer concern at TinkerPop)
- Log level changes: error should remain error for unexpected failures,
  don't downgrade to info without justification
- Resource cleanup on error paths: if the bug involves connection/channel
  handling, verify resources aren't leaked when the fix triggers

If the PR references a JIRA ticket (TINKERPOP-XXXX), link it as a discussion.

## Interpret
Read the structural signals from evidence.json (schema in
[references/interfaces.md](../references/interfaces.md)).

High blast radius (checks.blastRadius) on a bug fix is a warning signal — the
fix touches something many callers depend on. It isn't wrong, but verify it
doesn't subtly change behavior for existing callers. If high-centrality
functions (checks.centrality) are modified, say explicitly that every caller
needs a behavioral-change check — a fix in a hot function breaks things far
from the fix site.

A bug fix with no new or modified test is the biggest red flag: an untested fix
(checks.coverageGaps, checks.orphans) can't be shown to prevent regression.
Call it out prominently.

Confirm the fix is tied to its issue — the PR should have an `addresses` edge to
the JIRA/discussion (checks.completeness on `addresses`). No linked issue means
correctness can't be assessed.

Changes outside the issue scope aren't automatically bad — sometimes a fix needs
adjacent code. But they should be explainable. Flag them "necessary for fix?"
not "wrong."

## Escape
- if no linked issue — "Cannot assess whether fix is correct without knowing the bug"
- if public API signature changed — "Bug fix changes public API — needs broader discussion before merge"
