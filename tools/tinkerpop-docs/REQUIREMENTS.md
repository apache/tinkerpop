<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# TinkerPop Documentation Build - Requirements

## Overview

The documentation build system generates TinkerPop's reference documentation from AsciiDoc sources. It processes `[gremlin-groovy]` code blocks by executing them against a real Gremlin Console and producing tabbed output with console results and manual language variant tabs.

## Architecture

### Components

1. **`tinkerpop-docs`** - An AsciidoctorJ extension (standalone Maven module under `tools/`) that processes gremlin code blocks during AsciiDoc rendering.
2. **`bin/process-docs.sh`** - Shell script that orchestrates the full build: installs plugins, starts required services, and invokes Maven/Asciidoctor.
3. **Gremlin Console** - A long-running console subprocess used for code execution (one per document/book).
4. **Gremlin Server** - Started for remote connection examples.
5. **Gephi Mock** - A mock HTTP server (`bin/gephi-mock.py`) for Gephi plugin examples.

### Module Location

The `tinkerpop-docs` module lives at `tools/tinkerpop-docs/`. It uses the root `tinkerpop` pom as its Maven parent (for version inheritance) but is NOT part of the main reactor. It must be built separately before running the docs build.

### Extension Type

- **Treeprocessor**: Walks the AST after parsing, finds `[gremlin-groovy]` listing blocks by style attribute, replaces them with tabbed HTML pass blocks.
- **Postprocessor**: Applies callout fixes, removes empty comment spans, replaces `x.y.z` version placeholder in rendered HTML.
- Registered via SPI (`META-INF/services/org.asciidoctor.jruby.extension.spi.ExtensionRegistry`).
- Targets **AsciidoctorJ 2.5.x** for compatibility with existing asciidoctor-maven-plugin 2.2.4.

## Functional Requirements

### FR-1: Gremlin Code Block Processing

The extension SHALL walk the AsciiDoc AST and transform listing blocks with style `gremlin-groovy` into tabbed HTML output containing:

- A "console (<lang>)" tab showing the Gremlin prompt, input, and execution output (syntax highlighted as Groovy via coderay)

### FR-2: Console Execution

- The extension SHALL execute code blocks against a real Gremlin Console process to produce authentic console output.
- The console process SHALL be started once per document (book-scoped) and reused across all blocks, maintaining session state.
- The console SHALL have TinkerGraph, SPARQL, Hadoop, and Spark plugins activated.

**Note:** The old system used per-file console sessions. The new book-scoped approach is safe because every non-`existing` block reinitializes its graph. This scope change should be noted in commit messages and PR descriptions.

### FR-3: Graph Initialization

For each block, the extension SHALL inject initialization code before executing the block's content:

- `[gremlin-groovy,<graph>]`: `graph = TinkerFactory.create<Graph>(); g = graph.traversal()`
- `[gremlin-groovy]` (no graph): `graph = TinkerGraph.open(); g = graph.traversal()`
- `[gremlin-groovy,existing]`: No initialization (reuses current state)

Initialization also includes:
- Cleanup: delete `/tmp/neo4j` and `/tmp/tinkergraph.kryo` if they exist
- `:set max-iteration 100`
- Graph initialization SHALL be skipped when consecutive blocks use the same graph (NFR-4)

Supported graph names: `modern`, `classic`, `theCrew`, `kitchenSink`, `gratefulDead`

### FR-4: Error Handling in Code Blocks

- When a Gremlin statement produces a runtime error, the extension SHALL capture the error message and include it in the console output — this IS the expected output for documentation purposes.
- Errors in code blocks SHALL NOT fail the docs build.
- The "Display stack trace? [yN]" prompt SHALL be dismissed automatically without user intervention.

### FR-5: Manual Language Variant Tabs

- When `[source,<lang>]` blocks immediately follow a `[gremlin-groovy]` block (as consecutive AST siblings), they SHALL be consumed and rendered as additional tabs alongside the console output.
- Supported languages: `groovy`, `java`, `csharp`, `javascript`, `python`, `go`
- Consumption stops at the first non-matching sibling block.

### FR-7: Standalone Tab Groups

- `[source,<lang>,tab]` blocks SHALL be grouped into tabbed UI independently of gremlin blocks.
- Consecutive `[source,<lang>]` blocks following a `[source,<lang>,tab]` block are grouped together.

### FR-8: Dry-Run Mode

- When `gremlin-docs-dryrun` attribute is set, the extension SHALL skip console execution.
- Code blocks SHALL be formatted with `gremlin>` prompts but no execution output.
- Tab structure (manual tabs, standalone tab groups) SHALL still render fully.
- Dry-run mode SHALL NOT require a built Gremlin Console or Server.

## Out of Scope (Initial Release)

### FR-6: Auto-Translation (DEFERRED)

Auto-translation of Gremlin to language variants via GremlinTranslator is not included in the initial scope. Only manual language variant tabs (FR-5) are supported.

### FR-9: Rouge Syntax Highlighting (DEFERRED)

Existing coderay highlighting is retained. Rouge would fix C# highlighting but this is a pre-existing issue.

## Non-Functional Requirements

### NFR-1: Fail-Fast on Missing Dependencies

- If the `gremlin-docs-console-home` attribute is not set and the build is NOT in dry-run mode, the extension SHALL throw an error immediately with a clear message indicating the Console must be built.

### NFR-2: Timeout Safety Net

- Console communication SHALL have a 30-second timeout to prevent infinite hangs from unexpected console states.
- A timeout SHALL fail the build with a clear error message including the buffer contents at the time of timeout.

### NFR-3: Progress Visibility

- The extension SHALL log (at INFO level) each gremlin block being processed, including the first line of code.
- The extension SHALL log each statement being executed.
- Console stderr output SHALL be logged at INFO level for diagnostic visibility.

### NFR-4: Build Performance

- The Console communication protocol SHALL NOT introduce artificial delays (no polling timeouts for expected responses).
- Graph initialization SHALL be skipped when consecutive blocks use the same graph.

## Console Communication Protocol

### Prompt-Based Boundary Detection

The extension communicates with the Gremlin Console using prompt-based boundary detection:

1. The console outputs `gremlin>` (without trailing newline) after each statement completes.
2. The extension reads character-by-character and detects when the buffer ends with `gremlin>`.
3. Continuation prompts (`......N>`) are skipped — only `gremlin>` signals statement completion.

### Error Prompt Handling

When a statement errors, the console writes "Display stack trace? [yN]" to stderr and blocks reading from stdin:

1. A stderr-draining thread detects "Display stack trace?" and sets a flag.
2. The main thread's read loop checks this flag while waiting for stdout data.
3. When the flag is set, a blank line is sent to stdin to dismiss the prompt.
4. The console then prints `gremlin>` to stdout, unblocking the read.

### Pre-Statement Dismissal

Before each statement, any pending error prompt from a previous call is dismissed:

1. Check the `errorPromptPending` flag.
2. If set, send a blank line and wait for the resulting `gremlin>` prompt.

## Tabbed HTML Output

### Structure

Tab HTML uses CSS-only radio button tabs matching existing `tinkerpop.css` styles:
- `<section class="tabs tabs-N">` container
- Radio inputs + labels for tab switching
- `<div class="tabcontent">` panels
- Tab IDs use a deterministic sequential counter (no timestamps)

### Console Tab

- Labeled "console (<lang>)" (e.g., "console (groovy)")
- Content wrapped as `[source,groovy]` for coderay syntax highlighting
- Callout markers rendered as `// <N>` comments

### Plugin Conflict Management

Plugin conflicts between chapters are managed via AsciiDoc document attributes:

```asciidoc
// Exclude spark-gremlin to avoid Spark jar conflicts with Neo4j's Spark dependencies
:gremlin-docs-plugins-exclude: spark-gremlin
```

When the extension encounters this attribute, it shuts down the current console, toggles plugin directories, and starts a new console. Each usage must include an inline comment explaining the conflict.

## Build Invocation

### Full Build (with execution)

```
mvn clean install -pl :gremlin-server,:gremlin-console -am -DskipTests
bin/process-docs.sh
```

### Dry-Run (layout only, no Console needed)

```
bin/process-docs.sh --dry-run
```

### Output

Final HTML documentation is placed in `target/docs/htmlsingle/`.

## Dependencies

| Dependency | Purpose |
|---|---|
| Gremlin Console distribution | Code execution |
| Gremlin Server distribution | Remote connection examples |
| `sparql-gremlin` plugin | SPARQL examples |
| `hadoop-gremlin` plugin | Hadoop examples (local filesystem mode) |
| `spark-gremlin` plugin | Spark examples (local mode) |
| `gephi-mock.py` | Gephi plugin examples |

### Hadoop Configuration

The docs build does NOT require an external Hadoop cluster. A docs-specific `core-site.xml` configures `fs.defaultFS=file:///` so the `hdfs` binding operates on the local filesystem. `spark.master=local[4]` and `jarsInDistributedCache=false` ensure Spark runs in-process.

## Configuration

The extension is configured via Asciidoctor document attributes:

| Attribute | Description |
|---|---|
| `gremlin-docs-console-home` | Path to the unpacked Gremlin Console distribution |
| `gremlin-docs-hadoop-libs` | Path to Hadoop library jars for HADOOP_GREMLIN_LIBS |
| `gremlin-docs-dryrun` | When present, enables dry-run mode |
| `gremlin-docs-plugins-exclude` | Comma-separated list of plugins to exclude (triggers console restart) |

These attributes are passed via the Maven command line (resolved by `bin/process-docs.sh`):
```
-Dasciidoctor.attributes="gremlin-docs-console-home=/path/to/console gremlin-docs-hadoop-libs=/path/to/libs"
```
