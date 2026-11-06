# tinker-merge — Design

How this skill is structured and how to change it. The merge procedure itself lives in
`SKILL.md`; this document covers only the shape of the skill and where changes go.

## Structure

| Path | Role |
|------|------|
| `SKILL.md` | what the agent does around the script: the testing question, JDKs, conflicts, handoff |
| `scripts/merge-forward.sh` | the git and Maven work, start to finish |

The split is deliberate. Everything that can be deterministic is in the script, so two agents
asked to merge the same work run the same commands in the same order. `SKILL.md` covers only
what needs judgment or the operator: which tests to run, conflict resolution, reading the commit
list, and the push.

## Invariants

- **The script never pushes.** It ends by printing `git push --atomic`. The operator reviews
  the local result first, which keeps a bad merge off the shared remote.
- **Plain `git merge`.** No `--no-ff`, matching how committers have always merged forward.
- **A refusal moves nothing.** Every check runs before any branch is updated or checked out.
- **`--abort` restores the starting shas** recorded in the state file, not `<remote>/<branch>`,
  so a direct commit the operator asked to merge forward survives an abort.
- **Every step can be repeated.** A merge is skipped when it is already an ancestor, and the
  state file records the next branch to test, which is what makes `--continue` work.
- **The run's test plan is fixed at the start.** The test command and the branches named by
  `--skip-test-on` are saved with the run. `--continue --test-command` replaces the command for
  one test of the branch being resumed and is never saved, so the next branch always gets the
  run's command.
- **The body runs inside `main`.** The script checks out other branches, which can replace the
  file while it runs. Bash parses `main` in full before executing it.

## How to change it

- **The release branch list** is `RELEASE_BRANCHES` at the top of the script. Change it when a
  release line opens or reaches end of life, in step with the Branches section of
  `docs/src/dev/developer/for-committers.asciidoc`.
- **The test options** are `test_command` in the script and step 2 of `SKILL.md`. Change both
  together. Any other command goes through `--test-command`, so a new preset is only worth
  adding when operators choose it routinely.
- **New handling of a stop condition** goes into the script as a new exit code if it can be
  decided mechanically. Only judgment calls belong in `SKILL.md`.
