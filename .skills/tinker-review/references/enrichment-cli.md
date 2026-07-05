<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
-->

# Enrichment CLI — Command Reference

This is the human-readable reference for `scripts/enrichment/cli.js`: what each
command *does* and *when you'd reach for it* while writing a playbook or running
an enrichment pass. The command names are terse and not always self-descriptive
— this file is where the meaning lives.

**Three sources, three jobs (keep them in sync):**

| Source | Job |
|--------|-----|
| `scripts/enrichment/cli.js` — the `COMMANDS` registry | Single source of truth for **what commands exist** and whether each is `agent`- or `internal`-facing. |
| `scripts/enrichment/api.js` — JSDoc on each function | Canonical **semantics** (params, return shape, edge confidence), co-located with the implementation. |
| **this file** | The **narrative** — plain-language "what/when/gotchas" for a playbook author. |

`test/cli-docs.test.js` fails the build if these drift: every registry command
must have a `##` section here and a line in `cli.js --help`; agent-facing
commands must also appear in `SKILL.md`. Add a command → document it in all
three or CI goes red.

## Invocation

```bash
node scripts/enrichment/cli.js <command> --workDir /tmp/pr-review-<pr> [--key value ...]
```

Every command connects using `/tmp/pr-review-<pr>/session.json`, prints a JSON
result to stdout, and disconnects. Edges you create default to `INFERRED`
confidence unless the command says otherwise; pass `--confidence AMBIGUOUS` for
a flagged guess or `EXTRACTED` when the source states the fact directly.

---

## Agent-facing — read

These answer "what's in the graph?" and never mutate it. Use them to orient
before enriching and to pull the verification worklist.

### listFunctions
Lists functions the graph knows about. **Your first orientation read.**
`--changed true` narrows to just what the PR touched; `--visibility public`
narrows to the API surface. Returns each function's signature and line span so
you can open it in the worktree. Reach for it at the top of almost any playbook.

### listTypes
Lists types (classes, interfaces, enums), optionally filtered by `--kind` or
`--file`. Use it to see the types a PR defines or touches before drilling into
their functions.

### getCallsFrom
The direct callees of one function (its outgoing `calls` edges). **Keyed by
`--function` AND `--file`** because names repeat across the codebase. Reach for
it to trace what a changed function depends on — e.g. to confirm it no longer
calls a symbol the PR removed.

### getCanonicalSteps
The canonical Gremlin step vocabulary, parsed straight from `Gremlin.g4`. This
is the authoritative list of step names you map GLV methods onto with `mapStep`
— validate against it so you never invent a step that doesn't exist. Reads the
grammar file, not the graph. Central to the GLV playbook.

### auditConfidence
The edge-confidence distribution plus the list of `AMBIGUOUS` edges. **Run it
twice**: once at the start of the confidence pass, and again after you enrich, so
the audit reflects edges you added. Anything still `AMBIGUOUS` afterward belongs
in the report's `openQuestions`, not asserted as fact.

### listInferred
Your **verification worklist**: the name-resolved / agent-mapped edges worth a
source check, optionally narrowed with `--relation` (start with
`implements_step`, then `calls`). After reading the source, promote or downgrade
each with `setEdgeConfidence`. This is how INFERRED becomes EXTRACTED.

### listDeleted
The files the PR deleted (graph stubs marked `deleted: true`), each paired with
the symbol name it likely defined. **The entry point for the removal playbook** —
it gives you the symbols to grep the surviving tree for, and the valid `--toPath`
targets for `addReference`.

### listExternalRefs
Unresolved external callees — names the changed code calls that weren't defined
in the changed set. Shows each stub's `origin` (library/project/unresolved) and,
crucially, **flags any whose name matches a deleted symbol**: a changed file
still calling a just-removed name is a dangling reference the graph catches on
its own. Sorted so the dangerous ones surface first.

---

## Agent-facing — write

These mutate the graph. New edges default to `INFERRED`; grade honestly.

### addReference
**Manual escape hatch for the removal playbook.** The Phase-1 pass grepped for
*code-symbol* references automatically; use this to record the ones it can't see
— config strings, doc mentions, build-file references to a removed symbol.
`--toPath` must be a file `listDeleted` returned. Creates the same `references`
edge the automatic pass does.

### mapStep
Records that a GLV/host-language function implements a canonical Gremlin step, as
an `implements_step` edge (creating the Step vertex on first use). **The core
move of the GLV playbook.** Map only real traversal-step methods — never
language boilerplate (`toString`, `equals`, `close`) or internal helpers.
Validate `--step` against `getCanonicalSteps` first.

### setEdgeConfidence
Re-grades an existing edge after you verify it against source. Identify the edge
by `--relation` + `--fromName` (optionally pin `--fromFile`, narrow `--toName`).
**The other half of the `listInferred` loop**: promote a confirmed edge to
`EXTRACTED`, or downgrade a wrong name-resolution to `AMBIGUOUS` so it surfaces
in the report's review list.

### linkDiscussion
Attaches an external discussion you found (JIRA, dev-list thread, proposal) as a
Discussion vertex, linked from the PR discussion via `addresses`. Use it when
enrichment turns up prior context the Phase-1 discovery pass missed, so the
report can cite where the change was debated. `--source` is `jira | devlist |
proposal`.

### linkDoc
Records that a documentation file documents a graph entity, via a `documents`
edge from a Doc vertex to the named entity (`--entity` label + `--name`). Use it
to connect a step or feature to its reference docs / recipe, so the report can
flag a change whose docs weren't updated.

### addGrammarRule
Adds a `GrammarRule` vertex for a rule the PR introduces. Reach for it in the
grammar playbook when a `*.g4` change adds a production the graph should track so
later steps can link to it.

### linkRule
Links a step to the grammar production that defines it, as a `has_rule` edge
(Step → GrammarRule). Closes the loop `addGrammarRule` opens — both vertices must
exist first (`mapStep` for the Step, `addGrammarRule` for the rule). The grammar
playbook's `checks.completeness` on `has_rule` reports which steps still lack it.

### mapCoverage
Records that a test covers a step's behavior, as a `covers` edge (Test → Step).
Key the test by `--test` name **and** `--file` (names repeat across suites); the
Step must already exist (`mapStep`). The `new-step` playbook treats a step with
no `covers` as a coverage gap — this is how you record the coverage you find.

### annotate
Sets an arbitrary `--key`/`--value` property on a vertex identified by `--label`
+ `--name`. The **general-purpose escape hatch** for a fact the schema has no
dedicated edge for. Prefer a typed command when one fits; reach for this only
when none does.

---

## Internal (Phase 1)

`review.js` runs these itself while building the graph. They're exposed on the
CLI only for manual re-runs during debugging — **you do not call them during
enrichment**, and they intentionally do not appear in `SKILL.md`.

### classifyExternals
Tags each external callee stub with `origin` = `library | project | unresolved`.
`listExternalRefs` and the centrality check read that tag. Runs once in Phase 1.

### createPrDiscussion
Creates the root PR Discussion vertex that every other discussion links back to
via `addresses`. Idempotent (a second call is a no-op). Runs once in Phase 1.
