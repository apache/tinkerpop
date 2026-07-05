# PR Knowledge Graph Schema

## Vertices (9 labels)

### Code structure

**File** `{ path, language, changed }`
A source file in the PR. `changed: true` if modified in this PR.

*Stub Files* `{ path, language, changed: true, parsed: false, deleted }` are
markers for changed files the extractor didn't parse, so the PR's `modifies`
edge still lands. `deleted: true` means the PR removed the file (absent from the
PR-head worktree); `deleted: false` means it's present but an unparsed type
(non-code, or a non-primary language). Real Files have no `parsed` property, so
select materialized files with `.hasNot("parsed")` and markers with
`.has("parsed", false)`.

**Function** `{ name, signature, visibility, filePath, language, lines_start, lines_end, changed }`
A function or method. The primary unit of analysis. `language` is the source
language (java, python, javascript, go, csharp, dart); by-name edges (`calls`,
`tests`) resolve only within a language, so a multi-language PR never invents
cross-language edges.

*External stub Functions* `{ name, external: true, resolved: false, changed: false, origin?, definedIn? }`
are markers created when a `calls`/`tests` edge targets a function by name that
wasn't extracted (a library/JDK call, or a function in a file this PR didn't
change). They keep the edge from vanishing and let blast-radius/centrality see
the call; they lack `filePath`/`signature`/`visibility`. Filter them out with
`.has("external", false)` — or, since real Functions have no `external` property,
`.hasNot("external")` — when you only want materialized code.

`classifyExternals` tags each stub with `origin`: `library` (a known JDK/accessor
name — noise), `project` (a repo source declares a type with this name;
`definedIn` records the file), or `unresolved` (unknown). Centrality drops
`origin: library` calls from out-degree so ubiquitous accessor calls don't
inflate hotspots.

**Type** `{ name, kind, visibility, filePath, language, changed }`
A class, interface, struct, or enum. `kind` is one of: class, interface, struct,
enum. `changed: true` if the PR modified the file it's declared in. `language`
scopes `extends`/`implements` resolution so hierarchies never cross languages.

*External stub Types* `{ name, external: true, resolved: false }` are markers
created when an `extends`/`implements` edge names a supertype that wasn't
extracted — typically a JDK/library class outside the worktree. In-repo
supertypes are usually materialized by the extractor's hierarchy-neighborhood
expansion (which pulls a changed type's ancestors and descendants in as context),
so stubs mostly stand for third-party types. They lack `kind`/`filePath`; filter
them with `.has("external", false)` or `.hasNot("external")`.

### TinkerPop domain

**Step** `{ name, canonical_name }`
A Gremlin traversal step as a concept (e.g., "addV", "has", "out"). Created during enrichment when functions are mapped to steps.

**GrammarRule** `{ name, production }`
An ANTLR production in Gremlin.g4.

### Verification

**Test** `{ name, type, filePath, language }`
A test function. `type` is one of: unit, integration, suite. `language` scopes
the `tests` edge to same-language functions.

**Doc** `{ path, section }`
A documentation file or section that references code.

### Discussion

**Discussion** `{ url, source, title, body }`
Any discussion artifact. `source` is one of: devlist, jira, proposal, pr.
The PR itself is a Discussion with `source: "pr"`.

**Comment** `{ author, body, timestamp }`
A comment on a Discussion.

## Edges (16 implemented + 1 planned)

One edge is marked ⚠️ *planned* below (`depends_on`) — documented but intentionally
not populated.

### Edge confidence (every edge)

Every edge carries a `confidence` property recording how the relationship was
established, so downstream analysis and the reviewer can separate observed fact
from deduction:

| Value | Meaning | Examples |
|-------|---------|----------|
| `EXTRACTED` | Explicitly present in source or the git diff | `defines`, `modifies`, `has_comment`, an `addresses` link stated in the PR body/diff (`found_in: pr`/`diff`) |
| `INFERRED` | Reasonable deduction | `calls`/`tests` (resolved by name match), `proposed_in`, a cross-referenced `addresses` (`found_in: jira_body`/`devlist_body`), agent `implements_step`/`documents` mappings |
| `AMBIGUOUS` | Uncertain; flagged for human review | keyword-search `addresses` (`found_in: search`), low-confidence agent guesses |

Enrichment write commands (`mapStep`, `linkDiscussion`, `linkDoc`) accept an
optional `--confidence` flag (default `INFERRED`). `auditConfidence` reports the
distribution and lists every `AMBIGUOUS` edge; the review's structural appendix
renders this as the **Signal Confidence** panel.

### Code relationships

| Edge | From | To | Meaning |
|------|------|----|---------|
| `calls` | Function | Function | Function invokes another function |
| `defines` | File | Function or Type | File contains this definition |
| `declares` | Type | Function | Type's body declares this method (the membership edge; both endpoints pinned to the same file). `EXTRACTED` |
| `extends` | Type | Type | Subclass extends a superclass, or interface extends an interface. Supertype resolved by simple name; unresolved parents get an external Type stub. `INFERRED` |
| `implements` | Type | Type | Class implements an interface. Same resolution/stub behavior as `extends`. (Java splits `extends`/`implements` precisely; other languages label all bases `extends`.) `INFERRED` |
| `overrides` | Function | Function | A method overrides a same-named method declared by an ancestor type (transitive over `extends`/`implements`). Derived after population by `deriveOverrides`. `INFERRED` |
| `depends_on` | File | File | ⚠️ *planned, not populated.* File imports/requires another file. Intentionally omitted — call/defines edges already carry file connectivity (see `populate.js`). |
| `references` | File | File (deleted) | A surviving file still mentions a symbol from a file the PR deleted. Added during a removal review via `addReference`; carries `symbol` and `location` properties. |

### Domain relationships

| Edge | From | To | Meaning |
|------|------|----|---------|
| `implements_step` | Function | Step | This function is a GLV's implementation of a Gremlin step |
| `has_rule` | Step | GrammarRule | This step is defined by this grammar production. Written by `linkRule` (after `addGrammarRule` creates the rule vertex). `INFERRED` |

### Verification

| Edge | From | To | Meaning |
|------|------|----|---------|
| `tests` | Test | Function | This test exercises this function |
| `covers` | Test | Step | This test covers this step's behavior. Written by `mapCoverage`. The `new-step` playbook treats a missing `covers` as a coverage gap. `INFERRED` |
| `documents` | Doc | Step, Function, or Type | This doc describes this entity |

### Discussion

| Edge | From | To | Meaning |
|------|------|----|---------|
| `has_comment` | Discussion | Comment | Discussion contains this comment |
| `addresses` | Discussion | Discussion | One discussion references another (e.g., PR addresses a JIRA) |
| `proposed_in` | Discussion(proposal) | Discussion(pr) | A `docs/src/dev/future` proposal that this PR appears to relate to. Confidence tracks how it was found (see properties below). |
| `modifies` | Discussion(pr) | Function or File | The PR modifies this code |

#### `addresses` edge properties

| Property | Values | Meaning |
|----------|--------|---------|
| `found_in` | `pr`, `diff`, `search`, `jira_body`, `devlist_body` | Where the link was discovered |
| `found_via` | JIRA ID or URL | Which discussion contained the reference (for secondary links) |

#### `proposed_in` edge properties

| Property | Values | Meaning |
|----------|--------|---------|
| `matched_in` | `reference`, `title`, `body` | How the proposal was linked: an explicit path reference in the PR (`EXTRACTED`), a keyword in the proposal's title/heading (`INFERRED`), or keywords in its body (`AMBIGUOUS`). |
| `matched_keywords` | comma-separated terms | The keywords that matched, so the link is self-explanatory. |

