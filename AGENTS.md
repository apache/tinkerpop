# AGENTS.md

You are a TinkerPop developer working across the entire monorepo—code, tests, docs, and website to implement, 
maintain, and validate Apache TinkerPop’s graph computing framework and its multi-language Gremlin ecosystem.

- `README.md`
- `CONTRIBUTING.md`
- Developer documentation at `docs/src/dev/**`

This file must not contradict those documents. If it appears to, treat them as canonical and update this file 
accordingly.

***

## 1. Project overview

Key project docs (prefer these local files over external URLs):

- Reference docs: `docs/src/reference/**`
- Recipes: `docs/src/recipes/**`
- Upgrade docs: `docs/src/upgrade/**`
- IO and Serialization docs: `docs/src/dev/io/**`
- Provider docs and Gremlin Semantics: `docs/src/dev/provider/**`
- Developer docs: `docs/src/dev/developer/**`
- Future plans: `docs/src/dev/future/**`

The project website mirrors these for released versions; use local files for branch‑accurate information.

***

## 2. Repository structure (high level)

Use these as anchors when exploring the repo:

- Core code and modules: Maven multi‑module layout under the repo root, though modules may not contain JVM-relevant code
  (i.e. Maven is simply the build orchestration tool wrapping other environments like Python or Go)
- Docs: `docs/src/**` (AsciiDoc “books”, reference, dev docs, recipes, etc.)
- Changelog: `CHANGELOG.asciidoc`
- Website: `docs/site/**`
- Scripts:
    - Docker scripts: `docker/*.sh`
    - Docs/site scripts: `bin/process-docs.sh`, `bin/generate-home.sh`

When adding or modifying files, follow the existing structure and conventions in the surrounding directory.

***

## 3. Nested AGENTS.md files

This repository uses additional `AGENTS.md` files in certain subdirectories (for example, 
`gremlin-python/AGENTS.md`, `gremlin-dotnet/AGENTS.md`, etc.) to provide stack‑specific guidance.

Agents should:

- Always read this root `AGENTS.md` for global rules.
- When working in a subdirectory, also look for the closest `AGENTS.md` in the current or parent 
directories and apply its instructions for that area.
- Treat more specific `AGENTS.md` files (deeper in the tree) as overriding or refining the generic 
guidance here when there is a conflict.

## 4. Build and test recipes

### 4.1 Basic build and test

- Build everything:

  ```bash
  mvn clean install
  ```

- Build a specific module:

  ```bash
  mvn clean install -pl <module-name>
  ```

  Example:

  ```bash
  mvn clean install -pl tinkergraph-gremlin
  ```

### 4.2 Integration and specialized builds

- Enable integration tests:

  ```bash
  mvn clean install -DskipIntegrationTests=false
  ```

- Include Neo4j tests:

  ```bash
  mvn clean install -DincludeNeo4j
  ```

### 4.3 Gremlin Language Variant (GLV) builds

Build Gremlin Server first:

```bash
mvn clean install -pl :gremlin-server -am -DskipTests
```

Each GLV has its own module structure:

- Python:

  ```bash
  mvn clean install -Pglv-python -pl gremlin-python
  ```

- .NET:

  ```bash
  mvn clean install -pl gremlin-dotnet,gremlin-dotnet-source,gremlin-dotnet-tests
  ```

- JavaScript:

  ```bash
  mvn clean install -Pglv-js -pl gremlin-javascript
  ```

- Go:

  ```bash
  mvn clean install -Pglv-go -pl gremlin-go
  ```

***

## 5. Documentation

TinkerPop’s documentation is AsciiDoc‑based and lives under `docs/src/**`.

When adding or updating docs:

- Use AsciiDoc (not Markdown) in the main docs tree.
- Place new content in the appropriate book (reference, dev, recipes, etc.).
- Update the relevant `index.asciidoc` so the new content is included in the build.

***

## 6. Coding and testing conventions

Agents should follow these conventions when generating or editing code and tests.

### 6.1 Code style

- All files should include the Apache Software Foundation license header.
- Do **not** use import wildcards (for example, avoid `import org.apache.tinkerpop.gremlin.structure.*`); prefer explicit imports.
- Respect existing naming patterns and package organization.
- Define variables as `final` whenever possible, except for loop variables.

### 6.2 Test guidelines

- Prefer SLF4J `Logger` for output instead of `System.out.println` or `println` in tests.
- Use `TestHelper` utilities to create temporary directories and file structures for file‑based tests, instead of hard‑coding paths.
- Always close `Graph` instances that are manually constructed in tests.
- Tests using a `GraphProvider` implementation with `AbstractGremlinTest` should be suffixed with `Check` instead of `Test`.
- Prefer Hamcrest matchers for boolean‑style assertions (for example, `assertThat(..., is(true))`) instead of manually checking booleans.
- For Gremlin language tests, see the "Gremlin Language Test Cases" section of `docs/src/dev/developer/for-committers.asciidoc`
for more details and use Gherkin tests under:

  ```text
  gremlin-tests/src/main/resources/org/apache/tinkerpop/gremlin/test/features
  ```

***

## 7. Changelog, license, and checks

When changes affect behavior, APIs, or user‑visible features:

- Add or update entries in `CHANGELOG.asciidoc` in the correct version section.
- Do not invent new version numbers or release names; follow the existing pattern.
- Preserve and respect license headers and notices in all files.
- Avoid adding third‑party code or dependencies with incompatible licenses.

***

## 8. Do and don’t for agents

These rules apply to any AI/IDE assistant operating on this repository.

### 8.1 Do

- **Do** make small, focused changes that are easy to review.
- **Do** run the relevant build and test commands before suggesting that a change is complete.
- **Do** update or add tests when behavior changes.
- **Do** update documentation and/or changelog when you change public behavior or APIs.
- **Do** follow existing patterns for code structure, documentation layout, and naming.
- **Do** point maintainers to relevant documentation or issues when proposing non‑trivial changes.

### 8.2 Don’t

- **Don’t** perform large, sweeping refactors (across many modules or files) unless explicitly requested.
- **Don’t** change public APIs, configuration formats, or network protocols without explicit human approval and associated design/issue.
- **Don’t** switch documentation formats (e.g., AsciiDoc to Markdown) in the main docs tree.
- **Don’t** introduce new external dependencies, modules, or build plugins without an associated discussion and issue.
- **Don’t** invent project policies, version numbers, or release names.
- **Don’t** remove or weaken tests to “fix” failures; adjust the implementation or the test data instead.

If you are uncertain about the impact of a change, prefer to:

- Make a minimal patch.
- Add comments or notes for reviewers.
- Ask for clarification.

***

## 9. When in doubt

If AGENTS.md does not clearly cover a situation:

1. Look for relevant information in:
    - `CONTRIBUTING.md`
    - Developer docs under `docs/src/dev/developer/**`
    - Reference docs and recipes
2. Prefer **no change** over an unsafe or speculative change.
3. Surface the question to human maintainers (for example, by leaving a comment, or drafting a minimal PR that asks for guidance).

This file is intended to help tools act like a careful, well‑informed contributor. When in doubt, defer to human 
judgment and the canonical project documentation.