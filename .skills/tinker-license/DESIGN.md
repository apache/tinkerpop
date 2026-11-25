# tinker-license — Design

How this skill is structured and how to change it. The review procedure itself lives in
`SKILL.md`; this document covers only the shape of the skill and where changes go.

## Structure

| Path | Role |
|------|------|
| `SKILL.md` | the procedure: scope, build, inventory, per-component decisions, report, fix |
| `scripts/license-inventory.py` | the mechanical facts about each built artifact |
| `references/asf-rules.md` | the ASF and developer-doc rules, condensed into decisions |
| `references/pitfalls.md` | how gaps have actually arisen in this repository |

The split follows `tinker-merge`. Whatever can be decided mechanically is in the script, so two
agents reviewing the same build see the same facts. Whether a component needs an entry is a
judgment from its license and what is inside it, so it stays in `SKILL.md` and the references.

## Invariants

- **The script never decides compliance.** It reports what is packed, what is referenced and what
  each pom declares. `--strict` fails only on mechanical mismatches: a packed file that differs
  from its source, a reference with no file or a file with no reference, or a coordinate entry
  that names something not bundled at that version.
- **Every review is of a fresh build.** The script runs `mvn clean package` for the reviewed
  modules before any check and rejects an artifact older than that build, so stale `target/`
  output is never reviewed. `--skip-build` is the one exception, for re-running checks against
  a build just made, and its output names each artifact's build time. It writes only to `target/` directories and its build log, and uses
  `package` rather than `install` so `~/.m2` is not changed.
- **The version comes from the root `pom.xml`.** The script reviews whatever version the tree
  declares, a SNAPSHOT before a release or the release version during one, and builds that
  version's artifacts by exact name.
- **Canonical rules stay canonical.** The developer docs and the ASF guide are the sources.
  `references/asf-rules.md` condenses them and points back, and is corrected when they change.
- **Pitfalls are patterns, not a ledger.** Each entry explains how a gap arises, using the case
  that taught it. Results of a particular review belong in the review's report or a bead.

## How to change it

- **A new binary artifact** → an entry in `ARTIFACTS` in the script, and its module in `BUILD`
  if `-am` does not already reach it, and in the table in step 2 of
  `SKILL.md`. Change both together.
- **A new language-variant package** → an entry in `GLVS` in the script, naming its directory
  and how its LICENSE and NOTICE get into the published package.
- **A new kind of gap** → a check in the script if it can be decided from the artifact alone;
  otherwise a pattern in `references/pitfalls.md` and, if it changes the procedure, a line in
  step 4 of `SKILL.md`.
- **A policy change** in the developer docs or at the ASF → `references/asf-rules.md` first, then
  any step in `SKILL.md` that applies it.
