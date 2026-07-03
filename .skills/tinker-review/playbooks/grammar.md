# Playbook: Grammar Change

## Context
A change to Gremlin.g4 or related language infrastructure. Grammar changes
are inherently high-risk — they affect all parsers, all GLVs, and all
downstream tooling. Backwards compatibility is critical.

## Enrich
Identify which grammar rules were added or modified. Check:
- Is this adding new syntax or modifying existing syntax?
- If modifying: could existing valid Gremlin become invalid?
- Are all ANTLR targets updated? (Java, Python, Go parsers)
- Is there a corresponding step implementation for new grammar rules?

Link the proposal/discussion — grammar changes should always have prior
community discussion.

## Interpret
Read the structural signals from evidence.json (schema in
[references/interfaces.md](../references/interfaces.md)). Grammar changes have
outsized blast radius by nature (checks.blastRadius, checks.centrality) — the
grammar touches everything, so don't flag the reach itself; focus on backwards
compatibility. Completeness (checks.completeness on has_rule) shows whether new
rules are wired to a step.

A new rule that adds syntax (existing queries still work) is low risk.
A modified rule that changes parsing of existing syntax is high risk and
needs explicit backwards-compatibility analysis.

Look for keywords being added — TinkerPop has specific handling for
allowing keywords as map keys (#3091). New keywords can break existing
queries that use them as identifiers.

## Escape
- if no proposal or dev-list discussion found — "Grammar changes require community consensus — flagging for discussion"
- if existing grammar rules are modified (not just added) — "Potential backwards-incompatible change — needs explicit compatibility analysis"
