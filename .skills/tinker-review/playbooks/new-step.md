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

## Checks
- completeness(step, ["in:implements_step", "out:has_rule", "in:covers", "in:documents", "out:proposed_in"])
- coverage_gaps(pr.tests(), pr.modified())
- high_centrality(pr.modified())
- blast_radius(pr.modified(), 3)

## Interpret
A step missing from some GLVs is acceptable if tracked in a follow-up
issue — check if the PR or linked JIRA mentions phased rollout. Missing
documentation is not acceptable — a step without docs is undiscoverable.

When comparing signatures across GLVs, parameter count should match but
parameter types will differ by language. Focus on semantic equivalence,
not syntactic identity.

High centrality in step infrastructure (TraversalStrategy, Step interface
implementations) is expected — these are shared abstractions. Flag it
for attention but don't treat it as a problem.

## Escape
- if missing: proposal — "Cannot assess intent — need human to confirm expected semantics"
- if touches: graph_computer — "GraphComputer interactions require deep architectural knowledge — flagging for senior review"
- if step has no core_impl in PR — "Step concept referenced but no core implementation found — is this a GLV-only addition?"
