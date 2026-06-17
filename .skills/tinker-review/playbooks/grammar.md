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

## Checks
- completeness(grammarRule, ["in:has_rule"])
- blast_radius(pr.modified(), 2)
- high_centrality(pr.modified())

## Interpret
Grammar changes have outsized blast radius by nature — the grammar
touches everything. Focus on whether the change is backwards compatible.

A new rule that adds syntax (existing queries still work) is low risk.
A modified rule that changes parsing of existing syntax is high risk and
needs explicit backwards-compatibility analysis.

Look for keywords being added — TinkerPop has specific handling for
allowing keywords as map keys (#3091). New keywords can break existing
queries that use them as identifiers.

## Escape
- if no proposal or dev-list discussion found — "Grammar changes require community consensus — flagging for discussion"
- if existing grammar rules are modified (not just added) — "Potential backwards-incompatible change — needs explicit compatibility analysis"
