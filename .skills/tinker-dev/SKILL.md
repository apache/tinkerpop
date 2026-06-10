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

## Repository Structure

```
tinkerpop/
├── gremlin-core/              Core graph API and traversal engine (Java)
├── gremlin-server/            Gremlin Server (Java)
├── tinkergraph-gremlin/       In-memory reference graph (Java)
├── gremlin-python/            Python GLV
├── gremlin-js/                JavaScript/TypeScript workspace root
│   ├── gremlin-javascript/    JS driver (npm: "gremlin")
│   ├── gremlin-mcp/           Gremlin MCP server (npm: "gremlin-mcp")
│   └── gremlint/              Gremlin query formatter (npm: "gremlint")
├── gremlin-dotnet/            .NET GLV
├── gremlin-go/                Go GLV
├── gremlin-test/              Shared test resources and Gherkin features
├── gremlin-tools/             Benchmarks, coverage, socket server
├── docs/src/                  AsciiDoc documentation
├── docker/                    Docker build scripts and configs
├── bin/                       Utility scripts
└── CHANGELOG.asciidoc         Changelog
```

Maven is the build orchestration tool for all modules, including non-JVM ones.

## Basic Build Commands

Build everything:
```bash
mvn clean install
```

Build a specific module:
```bash
mvn clean install -pl <module-name>
```

For GLV-specific builds, test execution, and advanced workflows, see the appropriate
reference file under `references/`.

## Coding Conventions

- All files must include the Apache Software Foundation license header. Canonical text
  is at `bin/asf-license-header.txt`.
- Do not use import wildcards (e.g., avoid `import org.apache.tinkerpop.gremlin.structure.*`).
  Use explicit imports.
- Define variables as `final` whenever possible, except for loop variables.
- Respect existing naming patterns and package organization.
- Use `@/` path aliases for JavaScript/TypeScript imports where the project uses them.

## Test Conventions

- Prefer SLF4J `Logger` over `System.out.println` or `println` in tests.
- Use `TestHelper` utilities for temporary directories and file structures instead of
  hard-coding paths.
- Always close `Graph` instances that are manually constructed in tests.
- Tests using a `GraphProvider` with `AbstractGremlinTest` should be suffixed `Check`,
  not `Test`.
- Prefer Hamcrest matchers for boolean assertions (e.g., `assertThat(..., is(true))`)
  instead of manually checking booleans.
- Gremlin language tests use Gherkin features under:
  `gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/test/features/`
  See `docs/src/dev/developer/for-committers.asciidoc` for details.

## Documentation Conventions

- TinkerPop documentation is AsciiDoc-based under `docs/src/`. Do not use Markdown in
  the main docs tree.
- Place new content in the appropriate book (reference, dev, recipes, etc.).
- Update the relevant `index.asciidoc` so new content is included in the build.
- For detailed documentation generation instructions, see `references/documentation.md`.

## Changelog, License, and Checks

When changes affect behavior, APIs, or user-visible features:

- Add or update entries in `CHANGELOG.asciidoc` in the correct version section.
- Do not invent new version numbers or release names; follow the existing pattern.
- Preserve and respect license headers and notices in all files.
- Avoid adding third-party code or dependencies with incompatible licenses.

## Agent Guardrails

### Do

- Make small, focused changes that are easy to review.
- Run the relevant build and test commands before suggesting a change is complete.
- Update or add tests when behavior changes.
- Update documentation and/or changelog when changing public behavior or APIs.
- Follow existing patterns for code structure, documentation layout, and naming.
- Point maintainers to relevant documentation or issues when proposing non-trivial changes.

### Don't

- Don't perform large, sweeping refactors unless explicitly requested.
- Don't change public APIs, configuration formats, or network protocols without explicit
  human approval and an associated design/issue.
- Don't switch documentation formats (e.g., AsciiDoc to Markdown) in the main docs tree.
- Don't introduce new external dependencies, modules, or build plugins without an
  associated discussion and issue.
- Don't invent project policies, version numbers, or release names.
- Don't remove or weaken tests to "fix" failures; adjust the implementation or test
  data instead.
- Don't run `bd dolt push` — pushing to DoltHub is a maintainer action performed after
  PRs merge, not during active development.
- Don't close a beads issue when a PR is submitted — close it only after the PR merges
  to the target branch.

If uncertain about the impact of a change, prefer to make a minimal patch, add comments
for reviewers, and ask for clarification.

## When In Doubt

1. Check `CONTRIBUTING.md`, developer docs under `docs/src/dev/developer/`, and reference docs.
2. Prefer no change over an unsafe or speculative change.
3. Surface the question to human maintainers.

## Reference Guides

For deeper, task-specific guidance, see the reference files in this skill:

- [Development Environment Setup](references/dev-environment-setup.md) — fresh clone to working environment
- [Java / Core Builds](references/build-java.md) — Java modules, gremlin-server, integration tests
- [Python GLV](references/build-python.md) — build, test, result evaluation
- [JavaScript GLV](references/build-javascript.md) — npm workspace, build, test, evaluation
- [.NET GLV](references/build-dotnet.md) — build, test, Docker setup
- [Go GLV](references/build-go.md) — build, test, Docker setup
- [Documentation](references/documentation.md) — AsciiDoc generation, website
- [Gremlin MCP Server](references/gremlin-mcp.md) — translation, formatting, querying via MCP
- [Beads Workflow](references/beads-workflow.md) — agent planning, persistent memory, TinkerPop-specific conventions and DoltHub push policy
