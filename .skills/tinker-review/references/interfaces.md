# Graph Review Skill — Module Interfaces

Module contracts for the Phase 1 vertical slice. Each module is independently
implementable against these signatures and data shapes.

Design reference: `~/graph-review.md`

---

## Data Types

```typescript
// === Extraction Output ===

interface ExtractionResult {
  language: string;
  files: FileInfo[];
  functions: FunctionInfo[];
  types: TypeInfo[];
  calls: CallInfo[];
  imports: ImportInfo[];
}

interface FileInfo {
  path: string;          // relative to worktree root
  language: string;      // e.g., "dart", "java", "go"
  changed: boolean;      // true if modified in this PR
}

interface FunctionInfo {
  name: string;
  signature: string;     // full signature as string
  visibility: "public" | "private" | "protected" | "internal";
  filePath: string;      // which file this lives in
  linesStart: number;
  linesEnd: number;
  changed: boolean;      // true if modified in this PR
}

interface TypeInfo {
  name: string;
  kind: "class" | "interface" | "struct" | "enum";
  visibility: "public" | "private" | "protected" | "internal";
  filePath: string;
}

interface CallInfo {
  callerName: string;    // function making the call
  callerFile: string;
  calleeName: string;    // function being called
  line: number;          // line of the call site
}

interface ImportInfo {
  filePath: string;      // file containing the import
  importedPath: string;  // what is being imported
  importedName: string;  // specific symbol if applicable, or "*"
}

// === Infrastructure ===

interface ServerHandle {
  port: number;
  containerId: string;
  url: string;           // ws://localhost:${port}/gremlin
}

// === Graph Population ===

interface PopulationSummary {
  vertices: number;              // true count, queried from the graph after population
  edges: number;                 // true count, queried from the graph after population
  breakdown: {                   // per-type ATTEMPTED inserts (may exceed the real totals above)
    files: number;
    functions: number;
    types: number;
    tests: number;
    calls: number;
    defines: number;
    testsEdges: number;
    externalFunctions: number;   // stub Functions minted for unresolved callees
    stubFiles: number;           // stub Files minted for changed files that weren't parsed
  };
}

// === Pattern Results ===
//
// Each check's result type is defined — WITH per-field meaning — as a @typedef
// in the pattern file that produces it. Those typedefs are canonical; read them
// when interpreting a field. Do not re-declare them here (that is what drifts).
//
//   CompletenessResult   scripts/patterns/completeness.js
//   CoverageGapResult    scripts/patterns/coverage-gaps.js
//   CentralityResult     scripts/patterns/centrality.js
//   BlastRadiusResult    scripts/patterns/blast-radius.js
//   ClusterResult        scripts/patterns/cluster-analysis.js
//   ConfidenceResult     scripts/patterns/confidence-audit.js
//   ExternalsResult      scripts/patterns/classify-externals.js
//   OrphanResult         scripts/patterns/orphans.js
//   ArchitectureResult   scripts/patterns/architecture.js

// === Evidence (evidence.json — what Phase 1 writes; the fields Interpret cites) ===

interface Evidence {
  meta: {
    pr: number;
    title: string;
    domains: string[];           // e.g. ["general", "glv", "driver-server"]
    language: string;
    changedFileCount: number;
    timestamp: string;
  };
  graphStats: PopulationSummary;
  architecture: ArchitectureResult;
  checks: {
    completeness: CompletenessResult[];
    coverageGaps: CoverageGapResult;
    centrality:   CentralityResult;
    blastRadius:  BlastRadiusResult;
    clusters:     ClusterResult;
    confidence:   ConfidenceResult;
    externals:    ExternalsResult;
    orphans:      OrphanResult;
  };
  discussions: DiscussionsResult;   // jiras[], devList[], secondary[], proposals[], prComments{}
  changedFiles: string[];
}

// === ReportPackage (report.json — renderer input; Evidence + agent narrative) ===
// The agent adds these fields in Phase 5; render.js consumes the whole thing.

interface ReportPackage extends Evidence {
  summary: string;                       // HTML
  clusters: { assessment: string };      // narrative prose — distinct from checks.clusters
  guidedWalk: { title; badge; badgeText; body }[];
  findings: { title; snippet; body }[];
  openQuestions: { title; body; meta }[];
  functionalTest?: { plan; results: { name; pass; output }[]; observations };
    // plan/observations: HTML, theme-level. results rows are THEMES, each `name`
    // naming the scenario labels it spans — not one row per scenario.
  appendixFunctional?: { environment; testCode; fullOutput };
    // environment: HTML. testCode/fullOutput: RAW TEXT (renderer wraps in
    // <pre><code>; do not pre-wrap). testCode is the COMPLETE labeled battery.
}
```

---

## Module Signatures

### extraction/tree-sitter.js

```javascript
/**
 * Parse source files in a directory using Tree-sitter.
 * Returns structured extraction data for graph population.
 *
 * @param {string} directory - Absolute path to the PR worktree
 * @param {string} language - Primary language to parse (e.g., "dart")
 * @param {object} options
 * @param {string[]} [options.changedFiles] - List of files changed in PR (relative paths)
 * @returns {Promise<ExtractionResult>}
 */
export async function extract(directory, language, options = {}) {}
```

**Responsibilities:**
- Load the appropriate tree-sitter grammar for the language
- Walk the directory, parse each source file
- Run queries to extract functions, types, call sites, imports
- Mark `changed: true` on files/functions that appear in `options.changedFiles`
- Return the structured `ExtractionResult`

**Does NOT:**
- Connect to Gremlin Server
- Create graph vertices/edges
- Understand Gremlin step semantics

---

### infrastructure/docker.js

```javascript
/**
 * Start a Gremlin Server Docker container with TinkerGraph.
 * Uses a random available port. Polls until server is ready.
 *
 * @param {object} options
 * @param {string} [options.image] - Docker image (default: "tinkerpop/gremlin-server:4.0.1")
 * @param {number} [options.timeoutMs] - Max wait for readiness (default: 30000)
 * @returns {Promise<ServerHandle>}
 */
export async function startServer(options = {}) {}

/**
 * Stop and remove the Gremlin Server container.
 *
 * @param {ServerHandle} handle - Handle returned by startServer
 * @returns {Promise<void>}
 */
export async function stopServer(handle) {}
```

**Responsibilities:**
- Find a random unused port
- Run `docker run` with correct config (empty graph, GraphBinary)
- Poll the HTTP endpoint until ready
- Return connection details
- Clean up container on stop

**Does NOT:**
- Manage the gremlin-js client connection (that's the caller's job)
- Know anything about the graph schema

---

### graph/populate.js

```javascript
/**
 * Populate TinkerGraph with extraction data.
 * Creates vertices and edges matching the PR knowledge graph schema.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {ExtractionResult} extraction - Output from tree-sitter module
 * @returns {Promise<PopulationSummary>}
 */
export async function populate(g, extraction) {}
```

**Schema mapping:**

| Extraction data | Graph vertex | Key properties |
|---|---|---|
| `files[]` | `File` | path, language, changed |
| `functions[]` | `Function` | name, signature, visibility, lines_start, lines_end, changed |
| `types[]` | `Type` | name, kind, visibility |

| Extraction data | Graph edge | From → To |
|---|---|---|
| `calls[]` | `calls` | Function → Function (matched by name+file) |
| `functions[].filePath` | `defines` | File → Function |
| `types[].filePath` | `defines` | File → Type |
| `imports[]` | `depends_on` | File → File |

**Responsibilities:**
- Create all vertices with properties
- Create all edges (matching by name for call targets)
- Handle cases where call targets don't resolve (function in external dependency) — skip edge, don't fail
- Return population summary with counts

**Does NOT:**
- Create semantic edges (implements_step, tests, covers) — that's agent enrichment
- Parse source files — that's the extraction module's job

---

### patterns/completeness.js

```javascript
/**
 * Check that a vertex has all expected outgoing/incoming edge labels.
 *
 * @param {object} g - gremlin-js GraphTraversalSource
 * @param {object} params
 * @param {string} params.vertexLabel - Label of vertex to check (e.g., "Step")
 * @param {string} [params.vertexName] - Name property to filter by
 * @param {string[]} params.expectedEdges - Edge labels that should exist
 *   Prefix with "in:" or "out:" for direction (default: "out:")
 *   e.g., ["out:has_rule", "in:implements_step", "in:covers", "in:documents"]
 * @returns {Promise<CompletenessResult[]>}
 */
export async function completeness(g, params) {}
```

---

### patterns/coverage-gaps.js

```javascript
/**
 * Find changed functions that have no incoming 'tests' edge.
 *
 * @param {object} g - gremlin-js GraphTraversalSource
 * @param {object} params
 * @param {boolean} [params.changedOnly] - Only check functions with changed=true (default: true)
 * @returns {Promise<CoverageGapResult>}
 */
export async function coverageGaps(g, params = {}) {}
```

---

### renderer/render.js

```javascript
/**
 * Render an evidence package to a self-contained HTML page.
 *
 * @param {EvidencePackage} evidence - Structured evidence data
 * @returns {string} - Complete HTML document as a string
 */
export function render(evidence) {}
```

**Responsibilities:**
- Produce a single self-contained HTML string (embedded CSS, no external deps)
- Render header with PR metadata
- Render completeness results as a visual checklist
- Render coverage gaps as a list with file/line links
- Render graph population summary

**Does NOT:**
- Fetch data or run queries
- Produce graph visualizations (Phase 4)
- Generate the guided walk narrative (needs agent synthesis)

---

### review.js (orchestrator)

```javascript
/**
 * Execute a graph-based review of a PR.
 * This is the skill entry point.
 *
 * @param {object} params
 * @param {number} params.pr - PR number
 * @param {string} params.repoPath - Path to the git repository
 * @param {object} [params.options]
 * @param {string} [params.options.outputPath] - Where to write the HTML (default: ./pr-review-${pr}.html)
 * @returns {Promise<string>} - Path to the generated HTML file
 */
export async function review(params) {}
```

**Orchestration steps:**
1. `git fetch origin pull/${pr}/head:pr-review/${pr}`
2. `git worktree add /tmp/pr-review-${pr} pr-review/${pr}`
3. Determine changed files via `git diff --name-only ${base}...pr-review/${pr}`
4. `startServer()`
5. Connect gremlin-js to `handle.url`
6. `extract(worktreePath, language, { changedFiles })`
7. `populate(g, extraction)`
8. `completeness(g, { ... })`
9. `coverageGaps(g, { ... })`
10. Assemble `EvidencePackage`
11. `render(evidence)` → write to file
12. `stopServer(handle)`
13. `git worktree remove ...` + `git branch -D ...`

**Progress output** (emitted to stdout between steps):
```
[review] PR #${pr} — fetching...
[review] Starting Gremlin Server on port ${port}...
[review] Phase 1: Extracting structure...
[review] Phase 1 complete: ${vertices} vertices, ${edges} edges
[review] Running checks...
[review] Done. Guidebook: ${outputPath}
```

---

## Dependency Graph

```
INTERFACES.md (this file — defines contracts)
       │
       ├──→ extraction/tree-sitter.js  ──┐
       ├──→ infrastructure/docker.js   ──┤
       ├──→ renderer/render.js           │
       │                                 ▼
       │                    graph/populate.js
       │                                 │
       │                                 ▼
       │               patterns/completeness.js
       │               patterns/coverage-gaps.js
       │                                 │
       └──→ review.js (orchestrator) ◄───┘
```

Modules above the line can be implemented in parallel.
Modules below the line depend on those above.
