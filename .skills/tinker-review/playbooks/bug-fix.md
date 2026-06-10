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

## Checks
- completeness(pr, ["addresses"])
- coverage_gaps(pr.tests(), pr.modified())
- blast_radius(pr.modified(), 3)
- high_centrality(pr.modified())

## Interpret
High blast radius on a bug fix is a warning signal — the fix touches
something many callers depend on. This doesn't mean it's wrong, but it
means the reviewer should verify the fix doesn't subtly change behavior
for existing callers.

Changes outside the issue scope aren't automatically bad — sometimes
fixing a bug requires touching adjacent code. But they should be
explainable. Flag them with "necessary for fix?" not "wrong."

If high-centrality functions are modified, emphasize that the reviewer
should check all callers for behavioral changes. A fix in a hot function
can silently break things far from the fix site.

## Escape
- if no linked issue — "Cannot assess whether fix is correct without knowing the bug"
- if public API signature changed — "Bug fix changes public API — needs broader discussion before merge"
