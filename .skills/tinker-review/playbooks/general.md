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

# Playbook: General (applies to all PRs)

## Context
These are concerns TinkerPop reviewers consistently raise regardless of
the type of change. This playbook always applies in addition to any
domain-specific playbook.

## Enrich
Look for these patterns in the changed code and annotate them:

**Style violations:**
- Wildcard imports in Java (import foo.*)
- Formatting/indentation changes mixed with functional changes
- Unused variables or imports
- Non-final variables that should be final

**Deprecated API usage:**
- Use of withRemote (deprecated in 4.0, use with_())
- Groovy script strings where gremlin-lang should be used
- Any API marked @Deprecated being used in new code

**Test concerns:**
- Tests that drop/clear all data instead of isolating with specialized labels
- Assertions that don't clearly explain what they verify
- Error/exception paths that aren't tested
- Test helpers without guard clauses (missing else/throw for invalid input)

**Resource safety:**
- Connections, channels, or streams opened without clear cleanup paths
- Log levels: error for unexpected failures, info for expected lifecycle events
- Data structures with concurrency implications (note if CopyOnWriteArraySet,
  synchronized collections, etc. are introduced without profiling justification)

## Checks
- coverage_gaps(pr.tests(), pr.modified())
- orphans("Function", "tests", { changedOnly: true })

## Interpret
Style nits and unused variables are low severity — note them but don't
make them the focus of the report. Prioritize safety concerns (resource
leaks, concurrency risks, missing error handling) and test quality issues.

Formatting changes mixed with functional changes are worth flagging
prominently — they make the PR harder to review and should ideally be
separate commits.

Deprecated API usage in NEW code is always a concern. Deprecated usage
in MODIFIED code (that was already there) is lower priority unless the
PR is specifically about migrating away from the deprecated API.

## Escape
None — this playbook always completes. No conditions warrant stopping.
