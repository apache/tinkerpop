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

## Essential Rules

These rules apply to any AI/IDE assistant operating on this repository.

### Do

- Make small, focused changes that are easy to review.
- Run the relevant build and test commands before suggesting that a change is complete.
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
