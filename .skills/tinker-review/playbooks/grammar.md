# Playbook: Grammar Change

## Context
A change to Gremlin.g4 or related language infrastructure. Grammar changes
are inherently high-risk — they affect all parsers, all GLVs, and all
downstream tooling. Backwards compatibility is critical.

## Enrich
- `addGrammarRule` — record each grammar rule the PR adds.
- `linkRule` — wire each rule to the step it defines (`has_rule`), so
  completeness can check every rule reaches a step.
- `linkDiscussion --source proposal` — record the proposal or dev-list thread
  (grammar changes need prior community consensus).

## Inspect
- New vs modified syntax — a modified rule that changes the parse of existing
  syntax is the high-risk case: could valid Gremlin become invalid?
- ANTLR targets — Java, Python, and Go parsers all updated?
- Step wiring — is there a step implementation for each new rule?
- New keywords — TinkerPop has special handling for keywords as map keys (#3091);
  a new keyword can break queries that use it as an identifier.

## Interpret
- `checks.blastRadius` / `checks.centrality` — grammar touches everything; don't
  flag the reach, focus on backwards compatibility.
- `checks.completeness` on `has_rule` — shows whether the recorded rules are
  wired to a step.
- New-syntax rule (existing queries still parse) — low. Modified rule (changes
  the parse of existing syntax) — high; needs explicit backwards-compat analysis.

## Escape
- if no proposal or dev-list discussion found — "Grammar changes require community consensus — flagging for discussion"
- if existing grammar rules are modified (not just added) — "Potential backwards-incompatible change — needs explicit compatibility analysis"
