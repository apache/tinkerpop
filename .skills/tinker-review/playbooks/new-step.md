# Playbook: New Step

## Context
A new traversal step added to Gremlin. Must propagate across grammar,
core implementation, all active GLVs, documentation, and test suite.
The proposal (dev list or JIRA) is the source of truth for intended
semantics.

## Enrich
Link the step's core implementation to its canonical name. The step
class (e.g., TreeStep) contains internal methods — only the method on
GraphTraversal/GraphTraversalSource that users call should map to the step.

Check for:
- A linked proposal (TINKERPOP-XXXX in title/description, or dev list thread)
- Whether the step appears in Gremlin.g4 (grammar rule)
- Whether GLV implementations exist for the step
- Whether documentation references the step

For API design concerns (mined from TinkerPop reviewer patterns):
- Default implementations on interfaces need justification — if something
  implements an interface, should it have a proper implementation?
- Naming should be consistent with existing patterns (look at sibling steps)
- Class design: wrapping + extending the same parent is suspicious
- Type restrictions should not be too narrow (provider implementations vary)

## Interpret
Read the structural signals from evidence.json (schema in
[references/interfaces.md](../references/interfaces.md)).

A new step's completeness (checks.completeness over implements_step / has_rule /
covers / documents / proposed_in) tells you what's missing. Missing from some
GLVs is acceptable if a follow-up issue tracks it — check the PR or linked JIRA
for phased rollout. Missing docs (no `documents` edge) is not acceptable — an
undocumented step is undiscoverable. Missing tests (checks.coverageGaps, no
`covers` edge) is a blocking gap — a new step must be exercised.

A new step usually has low blast radius (checks.blastRadius) since nothing calls
it yet; a high value means it hooks into shared infrastructure — verify those
integration points.

When comparing signatures across GLVs, parameter count should match but
parameter types will differ by language. Focus on semantic equivalence,
not syntactic identity.

High centrality (checks.centrality) in step infrastructure (TraversalStrategy,
Step interface implementations) is expected — these are shared abstractions.
Flag it for attention but don't treat it as a problem. Ignore out-degree that's
just library calls (checks.externals, origin=library).

## Escape
- if missing: proposal — "Cannot assess intent — need human to confirm expected semantics"
- if touches: graph_computer — "GraphComputer interactions require deep architectural knowledge — flagging for senior review"
- if step has no core_impl in PR — "Step concept referenced but no core implementation found — is this a GLV-only addition?"
