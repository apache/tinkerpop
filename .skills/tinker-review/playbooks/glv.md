# Playbook: GLV Implementation

## Context
A new or updated Gremlin Language Variant. Implements the full traversal
API in a host language. Must pass the Gremlin test suite. Should be
idiomatic for the target language, not a mechanical translation. The
reference GLV for structural comparison is typically gremlin-go (most
recent accepted GLV).

## Enrich
- `getCanonicalSteps` — pull the authoritative step vocabulary from `Gremlin.g4`;
  validate every step name against it before mapping.
- `mapStep` — map each real traversal-step method to its canonical step. A step
  method is on a traversal class, returns the traversal (fluent), is named to
  match the canonical step (cased per language), and is public DSL — not internal
  plumbing. Never map boilerplate (`toString`, `equals`, `close`) or helpers
  (`getLocalChildren`, `setTraversal`). Pass `--confidence AMBIGUOUS` when you
  can't tell whether a method is a real step (it surfaces in the review list
  instead of being asserted).
- `listInferred --relation implements_step` then `setEdgeConfidence` — promote a
  mapping to `EXTRACTED` once confirmed against the reference GLV or grammar.
- `linkDiscussion` — record a referenced JIRA or dev-list thread.

## Inspect
- Connection acquisition and release points — trace resource lifecycle through
  error paths; the common GLV bug is leaking a connection when a traversal fails
  mid-execution.

## Verify
- Test from the GLV under review by connecting its native client to the built
  server — this is the language whose wire behavior the PR changes.
- Round-trip the value types the GLV serializes (numbers, lists, maps, vertices,
  the language's date/UUID types) and confirm they survive the trip unchanged.
- Adversarial: submit a traversal that errors server-side and confirm the GLV
  surfaces a usable error rather than hanging or leaking the connection.
- If the change is idiomatic-only (no wire/serialization impact), an embedded
  Java exercise is not required — state that in `functionalTest`.

## Interpret
- `checks.completeness` — distinguish genuinely missing steps from steps present
  under a language-specific name (Python `addV` vs Go `AddV` — same step).
- Divergence from the reference GLV — judge whether the host language justifies
  it. Goroutines where Python uses asyncio is fine; a different serialization
  format is a concern.
- `checks.coverageGaps` — expected for driver internals (connection management,
  serialization); traversal-step methods should have test coverage.

## Escape
- if not test_suite_passes(glv): stop("Cannot proceed — GLV must pass test suite first")
- if not exists(reference_glv(language_family(glv))): escalate("Need human familiar with language")
- if step_mapping_confidence < 0.7: escalate("Cannot reliably map methods to steps — need human verification of API surface")
