# Playbook: GLV Implementation

## Context
A new or updated Gremlin Language Variant. Implements the full traversal
API in a host language. Must pass the Gremlin test suite. Should be
idiomatic for the target language, not a mechanical translation. The
reference GLV for structural comparison is typically gremlin-go (most
recent accepted GLV).

## Enrich
Map public methods in the GLV to their canonical Gremlin steps. Only map
methods that are actual traversal step implementations — the methods a user
calls to build a traversal. Do NOT map inherited language boilerplate
(toString, hashCode, equals, clone, close, etc.) or internal helper methods
(getLocalChildren, setTraversal, getRequirements, etc.) to steps.

A step implementation is typically:
- A method on a traversal class that returns the traversal (fluent API)
- Named to match the canonical step (cased per language convention)
- Part of the public traversal DSL, not internal plumbing

In Java specifically, the step *class* (e.g., TreeStep) contains internal
methods — only the method on GraphTraversal/GraphTraversalSource that users
call (e.g., `tree()`) should map to the step. In a GLV, the equivalent is
the method on the traversal DSL class.

Record your confidence in each mapping via `mapStep --confidence`: use the
default `INFERRED` for a solid match, and `AMBIGUOUS` when you can't reliably
tell whether a method is a real step implementation (this is the graph form of
the `step_mapping_confidence < 0.7` escape below — AMBIGUOUS mappings surface in
the report's review list instead of being asserted as fact). Once you've mapped
methods, run `listInferred --relation implements_step` and, for any mapping you
then confirm against the reference GLV or grammar, promote it with
`setEdgeConfidence --relation implements_step --fromName <method> --toName <step>
--confidence EXTRACTED`.

If the PR references a JIRA ticket (TINKERPOP-XXXX), link it as a discussion.

For the driver layer, identify connection acquisition and release points.
Trace resource lifecycle through error paths — the common GLV bug is
leaking connections when a traversal fails mid-execution.

## Checks
- completeness(glv, canonical_step_list())
- coverage_gaps(pr.tests(), pr.modified())
- high_centrality(pr.modified())

## Interpret
When reporting completeness gaps, distinguish between missing steps and
steps that exist but use a language-specific name (e.g., Python uses
`addV` but Go uses `AddV` — same step, different convention).

When reporting divergence from the reference GLV, the question isn't
"is it different?" — it's "is the difference justified by the host
language?" A Go GLV using goroutines where Python uses asyncio is fine.
A Go GLV using a different serialization format is a concern.

Coverage gaps in a GLV are expected for driver internals (connection
management, serialization) — but traversal step methods should have
corresponding test coverage.

## Escape
- if not test_suite_passes(glv): stop("Cannot proceed — GLV must pass test suite first")
- if not exists(reference_glv(language_family(glv))): escalate("Need human familiar with language")
- if step_mapping_confidence < 0.7: escalate("Cannot reliably map methods to steps — need human verification of API surface")
