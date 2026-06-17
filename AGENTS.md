# AGENTS.md

You are a TinkerPop developer working across the entire monorepo—code, tests, docs, and website to implement,
maintain, and validate Apache TinkerPop's graph computing framework and its multi-language Gremlin ecosystem.

## Primary Guidance: Agent Skills

This repository provides development guidance as an [Agent Skill](https://agentskills.io) named
`tinker-dev`. If your tool supports Agent Skills, **activate that skill** for detailed,
task-specific instructions covering build recipes, test evaluation, coding conventions, and
reference material for each Gremlin Language Variant.

If your tool does not discover the skill automatically, run `bin/agent-setup.sh --list` to
see how to configure it, or `bin/agent-setup.sh <agent>` to set up the integration.

## Canonical Documentation

These local documents are authoritative. If this file appears to contradict them, treat them as canonical.

- `README.md`
- `CONTRIBUTING.md`
- Developer documentation at `docs/src/dev/**`

## Licensing and Provenance

Apache TinkerPop is licensed under Apache License 2.0. Contributions must meet the 
[ASF's Generative Tooling guidance](https://www.apache.org/legal/generative-tooling.html). In particular:

* *Do not copy verbatim from incompatibly licensed sources.* This includes GPL / AGPL / LGPL code, proprietary code, 
unlicensed snippets, and Stack Overflow / blog / forum excerpts whose licensing is unclear. Reimplement from 
specifications, standards, or Apache-compatible sources (see the ASF 3rd Party Licensing Policy).
* *Every new source file needs the ASF license header.* See `bin/asf-license-header.txt` for the canonical form.
* *Attribute generated work in commits.* When AI tooling authored a non-trivial portion of a change, add a trailer 
of the form `Assisted-by: <agent>:<model>` to the commit message, where `<agent>` is the agent or IDE used 
(e.g. `Claude Code`, `Cursor`, `Kiro`, `GitHub Copilot`) and `<model>` is the model identifier 
(e.g. `claude-opus-4-7`, `gpt-5`). Append a bracketed entry per additional auxiliary tool (e.g. `[tinkerpop-mcp]`) 
only when something other than the primary agent contributed. For example, 
`Assisted-by: Claude Code:claude-opus-4-7`, `Assisted-by: Cursor:gpt-5`, or 
`Assisted-by: Claude Code:claude-opus-4-7 [tinkerpop-mcp]`. This aligns with the ASF's recommendation on AI 
provenance tracking.
* *The contributor remains responsible for what they submit.* Review generated output for licensing, correctness, and 
style before committing.

## Definition of Done

A change is **not done** until a full Maven validation has passed locally. Run it before
presenting work for review — even when your targeted or unit tests already pass, and even if you
judge a full run unnecessary. Incremental testing during development is encouraged, but it does
**not** satisfy this gate.

Match the validation to your changeset using a two-step pattern:

1. Rebuild and install the whole reactor without tests, so every module picks up your changes:
   `mvn clean install -DskipTests`
2. Run `verify` on the modules you changed, enabling integration tests where the change warrants
   them: `mvn verify -pl <changed-modules> [-DskipIntegrationTests=false]`

Examples:

- Python GLV → `mvn clean install -DskipTests` then `mvn verify -pl gremlin-python`
- `gremlin-server` + `gremlin-driver` → `mvn clean install -DskipTests` then
  `mvn verify -pl gremlin-driver,gremlin-server -DskipIntegrationTests=false`
- Broad or core changes, or when unsure → `mvn clean install -DskipIntegrationTests=false`

See the `tinker-dev` skill (and its `references/build-*.md`) for the full changeset-to-command
mapping and per-GLV details. If you cannot run the validation (for example, Docker is
unavailable), say so explicitly and report the change as **not validated**.

## Essential Rules

These rules apply to any AI/IDE assistant operating on this repository.

### Do

- Make small, focused changes that are easy to review.
- Before presenting any change as complete, satisfy the **Definition of Done** above — a full
  Maven validation matched to your changeset, not just targeted or unit tests.
- Update or add tests when behavior changes.
- Update documentation and/or changelog when you change public behavior or APIs.
- Follow existing patterns for code structure, documentation layout, and naming.
- If code is ready, stop and ask to commit, push or merge manually.

### Don't

- Don't perform large, sweeping refactors unless explicitly requested.
- Don't change public APIs, configuration formats, or network protocols without explicit human approval.
- Don't switch documentation formats (e.g., AsciiDoc to Markdown) in the main docs tree.
- Don't introduce new external dependencies, modules, or build plugins without discussion.
- Don't invent project policies, version numbers, or release names.
- Don't remove or weaken tests to "fix" failures; adjust the implementation or test data instead.
- Don't push to any branch.
- Don't merge any PR or branch.
- Don't create tags or releases.
- Don't add `@author` javadoc (or similar) tags for new files, but do not remove existing ones either.

### When In Doubt

1. Prefer no change over an unsafe or speculative change.
2. Ask for clarification.
