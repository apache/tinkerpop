# PR Knowledge Graph Schema

## Vertices (9 labels)

### Code structure

**File** `{ path, language, changed }`
A source file in the PR. `changed: true` if modified in this PR.

**Function** `{ name, signature, visibility, filePath, lines_start, lines_end, changed }`
A function or method. The primary unit of analysis.

**Type** `{ name, kind, visibility, filePath }`
A class, interface, struct, or enum. `kind` is one of: class, interface, struct, enum.

### TinkerPop domain

**Step** `{ name, canonical_name }`
A Gremlin traversal step as a concept (e.g., "addV", "has", "out"). Created during enrichment when functions are mapped to steps.

**GrammarRule** `{ name, production }`
An ANTLR production in Gremlin.g4.

**GLV** `{ language }`
A Gremlin Language Variant (e.g., "dart", "go", "python").

### Verification

**Test** `{ name, type }`
A test function. `type` is one of: unit, integration, suite.

**Doc** `{ path, section }`
A documentation file or section that references code.

### Discussion

**Discussion** `{ url, source, title, body }`
Any discussion artifact. `source` is one of: devlist, jira, proposal, pr.
The PR itself is a Discussion with `source: "pr"`.

**Comment** `{ author, body, timestamp }`
A comment on a Discussion.

## Edges (14 labels)

### Code relationships

| Edge | From | To | Meaning |
|------|------|----|---------|
| `calls` | Function | Function | Function invokes another function |
| `defines` | File | Function or Type | File contains this definition |
| `implements` | Function | Type | Function implements an interface |
| `depends_on` | File | File | File imports/requires another file |

### Domain relationships

| Edge | From | To | Meaning |
|------|------|----|---------|
| `implements_step` | Function | Step | This function is a GLV's implementation of a Gremlin step |
| `has_rule` | Step | GrammarRule | This step is defined by this grammar production |
| `provides` | GLV | Step | This GLV implements this step |

### Verification

| Edge | From | To | Meaning |
|------|------|----|---------|
| `tests` | Test | Function | This test exercises this function |
| `covers` | Test | Step | This test covers this step's behavior |
| `documents` | Doc | Step, Function, or Type | This doc describes this entity |

### Discussion

| Edge | From | To | Meaning |
|------|------|----|---------|
| `has_comment` | Discussion | Comment | Discussion contains this comment |
| `addresses` | Discussion | Discussion | One discussion references another (e.g., PR addresses a JIRA) |
| `proposed_in` | Step | Discussion | This step was proposed/discussed here |
| `modifies` | Discussion(pr) | Function or File | The PR modifies this code |

#### `addresses` edge properties

| Property | Values | Meaning |
|----------|--------|---------|
| `found_in` | `pr`, `diff`, `search`, `jira_body`, `devlist_body` | Where the link was discovered |
| `found_via` | JIRA ID or URL | Which discussion contained the reference (for secondary links) |

