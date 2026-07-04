# Playbook: New Step

## Context
A new traversal step added to Gremlin. Must propagate across grammar,
core implementation, all active GLVs, documentation, and test suite.
The proposal (dev list or JIRA) is the source of truth for intended
semantics.

## Enrich
- `getCanonicalSteps` — validate the step name before mapping.
- `mapStep` — link the core implementation to its canonical name. Map the
  `GraphTraversal`/`GraphTraversalSource` method users call, not the internal
  methods on the Step class (e.g., `TreeStep`).
- `linkDoc` — record the documentation that references the step.
- `linkDiscussion` — record the proposal or JIRA that defines the step's semantics.

## Inspect
- Default interface implementations — justified? If something implements an
  interface, should it have a proper implementation?
- Naming — consistent with sibling steps?
- Class design — wrapping and extending the same parent is suspicious.
- Type restrictions — not too narrow (provider implementations vary).
- Cross-GLV signatures — parameter count should match; types differ by language.
  Judge semantic equivalence, not syntactic identity.

## Interpret
- `checks.completeness` over `implements_step` / `has_rule` / `covers` /
  `documents` / `proposed_in` — what's missing. Missing from some GLVs is
  acceptable if a follow-up issue tracks it (check the PR or linked JIRA for
  phased rollout). Missing docs (no `documents`) — blocking; an undocumented
  step is undiscoverable. Missing tests (`checks.coverageGaps`, no `covers`) —
  blocking; a new step must be exercised.
- `checks.blastRadius` — usually low (nothing calls a new step yet); high means
  it hooks into shared infrastructure — verify those integration points.
- `checks.centrality` — high in step infrastructure (TraversalStrategy, Step
  implementations) is expected; note it, don't treat it as a problem. Ignore
  out-degree that's just library calls (`checks.externals`, origin=library).

## Escape
- if missing: proposal — "Cannot assess intent — need human to confirm expected semantics"
- if touches: graph_computer — "GraphComputer interactions require deep architectural knowledge — flagging for senior review"
- if step has no core_impl in PR — "Step concept referenced but no core implementation found — is this a GLV-only addition?"
