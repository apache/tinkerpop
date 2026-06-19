---
name: tinker-dev
description: >
  Development guidance for the Apache TinkerPop monorepo. Use when building,
  testing, or contributing to TinkerPop's graph computing framework and its
  multi-language Gremlin ecosystem (Java, Python, JavaScript, .NET, Go).
  Covers coding conventions, build recipes, test evaluation, documentation,
  development environment setup, Gremlin MCP server usage, and beads (bd) —
  the agent planning and persistent memory system used by TinkerPop maintainers.
license: Apache-2.0
compatibility: Requires Java 11+, Maven 3.5.3+, Docker. Individual GLVs may need Python, Node.js, .NET SDK, or Go.
metadata:
  version: 1.0.0
  project: Apache TinkerPop
---

# TinkerPop Development Skill

## Project Overview

Apache TinkerPop is a graph computing framework providing a standard API (the Gremlin graph
traversal language) for graph databases and processors. The repository is a Maven multi-module
monorepo that wraps JVM code, Python, JavaScript/TypeScript, .NET, and Go under a single build.

Canonical project documentation (prefer local files over external URLs):

- `README.md` and `CONTRIBUTING.md` at the repo root
- Reference docs: `docs/src/reference/`
- Developer docs: `docs/src/dev/developer/`
- Provider docs and Gremlin Semantics: `docs/src/dev/provider/`
- IO and Serialization: `docs/src/dev/io/`
- Recipes: `docs/src/recipes/`
- Upgrade docs: `docs/src/upgrade/`
- Future plans: `docs/src/dev/future/`

## Definition of Done

A change is **not done** until a full Maven validation has passed locally. Run it before
presenting work for review — even when your targeted or unit tests already pass, and even if you
judge a full run unnecessary. Incremental testing during development is encouraged, but it does
**not** satisfy this gate.

Validation is two steps. First, rebuild and install the whole reactor so every module picks up
your changes:

```bash
mvn clean install -DskipTests
```

Then run `verify` scoped to what you changed, using the **broadest** rule that applies, and
always list every module you touched.

**Self-contained modules** — changing one of these only requires validating that module:

| Changed module | Validate command |
|---|---|
| Python GLV | `mvn verify -pl gremlin-python` |
| JavaScript GLV | `mvn verify -pl :gremlin-javascript` |
| .NET GLV | `mvn verify -pl :gremlin-dotnet,:gremlin-dotnet-source,:gremlin-dotnet-tests` |
| Go GLV | `mvn verify -pl :gremlin-go` |
| `gremlint` | `mvn verify -pl :gremlint` |
| `gremlin-mcp` | `mvn verify -pl :gremlin-mcp` |
| Other single JVM module (e.g. `tinkergraph-gremlin`, `gremlin-console`) | `mvn verify -pl <module> -DskipIntegrationTests=false` |

**Shared modules** — depended on by others, so changing them means validating the consumers too.
Use the broadest rule that matches:

- **`gremlin-server`, `gremlin-driver`, or `gremlin-util`** define the wire protocol and
  serialization that every GLV exercises → validate the changed module(s) **plus all GLVs**:
  `mvn verify -pl <changed>,gremlin-python,:gremlin-javascript,:gremlin-dotnet,:gremlin-dotnet-source,:gremlin-dotnet-tests,:gremlin-go -DskipIntegrationTests=false`
- **`gremlin-core`, `gremlin-test`, or anything they depend on** ripple across the whole project,
  including the OLAP engines (`hadoop-gremlin`, `spark-gremlin`) → run everything:
  `mvn clean install -DskipIntegrationTests=false`
- **Unsure?** Treat it as the `gremlin-core` case and run everything.

**Docs or code comments only** — no validation gate; this gate is for changes that affect behavior.

### Things that quietly invalidate a run

- **GLV tests are skipped unless activated.** A GLV is built and tested only when its `.glv`
  sentinel exists: `gremlin-python/.glv`, `gremlin-go/.glv`, and *both* `gremlin-dotnet/src/.glv`
  and `gremlin-dotnet/test/.glv` (JavaScript builds by default). Without the sentinel the module
  is silently skipped — a green run may have tested nothing of your change. Create the sentinel
  (or pass `-Pglv-python` / `-Pglv-go`) before validating a GLV change.
- **Grammar and feature-test changes need a reactor build to take effect.** The ANTLR grammar
  (`gremlin-language/src/main/antlr4/Gremlin.g4`) and the Gherkin features under `gremlin-test`
  drive code generation during `mvn clean install` — ANTLR parsers, and per-language test code
  generated from the feature corpus (e.g. `gremlin-python/build/generate.groovy`, with
  equivalents for Go, .NET, and JavaScript). After changing either, run `mvn clean install
  -DskipTests` before relying on *any* test, including fast native ones, or you will be testing
  stale generated code.
- **Judge pass/fail by the Maven exit code, not console text.** Do not `grep`/`tail` the build
  output — tests deliberately emit expected errors, so the log misleads. Exit `0` = pass. For
  Python, detailed per-suite results are in `gremlin-python/target/python3/python-reports/*.xml`.

If you cannot run the validation (for example, Docker is unavailable), say so explicitly and
report the change as **not validated** — do not present it as done.

## Repository Structure

Most module names map directly to their purpose (`gremlin-core`, `gremlin-server`,
`gremlin-go`, etc.). Two things that aren't obvious:

- The **JavaScript workspace lives under `gremlin-js/`** — it contains `gremlin-javascript/`
  (the `gremlin` npm driver), `gremlin-mcp/`, and `gremlint/`. There is no top-level
  `gremlin-javascript/` directory.
- **Maven orchestrates the build for every module, including the non-JVM ones** (Python,
  JavaScript, .NET, Go) — which is why the validation in the Definition of Done runs through
  Maven rather than each language's native test runner.

Cross-language Gherkin feature tests live in `gremlin-test/`.

## Basic Build Commands

Build everything:
```bash
mvn clean install
```

Build a specific module:
```bash
mvn clean install -pl <module-name>
```

For GLV-specific builds and the validation commands per module, see the **Definition of Done**
table above. For environment setup and GLV activation, see `references/dev-environment-setup.md`.

## Conventions

The general Do/Don't rules and "when in doubt" guidance live in the root `AGENTS.md` — the
single source of truth, not repeated here. The conventions below are the TinkerPop-specific
ones that are easy to miss:

- **License header**: every new file needs the ASF header. Canonical text: `bin/asf-license-header.txt`.
- **Test naming**: a test using a `GraphProvider` with `AbstractGremlinTest` is suffixed
  `Check`, not `Test`.
- **Gremlin language tests**: cross-language behavior is tested with Gherkin features under
  `gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/test/features/`
  (see `docs/src/dev/developer/for-committers.asciidoc`).
- **Docs are AsciiDoc** under `docs/src/` — never Markdown in the main docs tree. For writing or
  revising any documentation (voice, per-book style, executable code blocks, AsciiDoc wiring),
  use the **tinker-doc** skill. The mechanical minimum: add new content to the right book and update
  its `index.asciidoc`.
- **Changelog**: for user-visible or API changes, update `CHANGELOG.asciidoc` in the correct
  version section. Do not invent version numbers or release names.

Otherwise, match the existing code in neighboring files — explicit imports (no wildcards),
`final` where practical, SLF4J logging over `println`, Hamcrest matchers, and closing any
`Graph` you construct in a test.

## Beads Caveats

The general agent Do/Don't rules are in the root `AGENTS.md`. Two beads rules are easy to get
wrong and worth repeating here (full workflow in `references/beads-workflow.md`):

- Don't run `bd dolt push` — pushing to DoltHub is a maintainer action performed after PRs
  merge, not during active development.
- Don't close a beads issue when a PR is submitted — close it only after the PR merges to the
  target branch.

## Reference Guides

Build and validate commands live in the **Definition of Done** table above. For the remaining
task-specific guidance, see:

- [Development Environment Setup](references/dev-environment-setup.md) — fresh clone to working environment, prerequisites, GLV activation
- [Gremlin MCP Server](references/gremlin-mcp.md) — translation, formatting, querying via MCP
- [Beads Workflow](references/beads-workflow.md) — agent planning, persistent memory, TinkerPop-specific conventions and DoltHub push policy
