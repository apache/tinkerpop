---
name: tinker-merge
description: >
  Merge work forward through Apache TinkerPop's release branches toward master
  (e.g. 3.7-dev, then 3.8-dev, then master), testing each branch locally and
  stopping before the push. Use only when the operator explicitly asks to
  merge a pull request, a feature branch, or commits already on a release
  branch.
license: Apache-2.0
compatibility: Requires git and Maven, plus the gh CLI to merge a pull request. Each release branch needs the JDK its tinker-dev skill names.
metadata:
  version: 1.1.0
  project: Apache TinkerPop
---

# TinkerPop Merge Skill

TinkerPop merges through its oldest release branch toward `master`: work that lands on `3.7-dev`
is merged into `3.8-dev`, and `3.8-dev` into `master`. `scripts/merge-forward.sh` performs that
chain deterministically. Your part is the one question to the operator, the conflicts, and the
handoff. The branch rules behind it are in the Branches and Pull Requests sections of
`docs/src/dev/developer/for-committers.asciidoc`.

**Never push.** The run ends with local merges and a printed push command. The operator reviews
the result and pushes it. That rule in `AGENTS.md` has no exception here.

## 1. Identify the source

The operator names what to merge. Pass it to the script as is:

| The operator says | Source argument |
|---|---|
| a pull request number | `#1234` |
| a feature branch | the branch name, e.g. `TINKERPOP-1234` |
| "merge 3.7-dev through" after committing to it directly | the release branch, e.g. `3.7-dev` |

The script works out the chain itself: from the pull request's base branch, from the release
branch a feature branch was cut from, or from the named release branch. Pass `--onto <branch>`
only when the operator states the target or the script says it cannot tell.

## 2. Ask how to test — the only questions

Ask the operator exactly this, with the default first:

1. `mvn clean install` (default) → `--test default`
2. `mvn clean install -DskipIntegrationTests=false` → `--test integration`
3. Skip all testing → `--test skip`
4. A command the operator gives → `--test-command '<command>'`

Unless testing is skipped, also ask which branches in the chain to test, all of them by default.
Pass the ones the operator leaves out as `--skip-test-on <branch>[,<branch>]`: they are merged
but not tested, as when the operator trusts a build of the oldest branch to cover the rest.

Ask nothing else before starting. Whichever command is chosen runs from the repository root on
every tested branch in the chain, after its merge, and a non-zero exit counts as a test failure.
Pass an operator's command through exactly as given, quoted as one argument.
GLV modules are tested only where the clone has activated them (see the `tinker-dev` skill), so
say so if the operator chose testing and a GLV changed but its `.glv` sentinel is absent.

## 3. Check the JDK for each branch

Release branches build on different JDKs. Each branch's own `tinker-dev` skill states which one
in its `compatibility` line:

```bash
git show <remote>/<branch>:.skills/tinker-dev/SKILL.md | grep '^compatibility'
```

When the JDK on the path does not fit a branch in the chain, pin an installed one that does
before starting, and tell the operator which you pinned:

```bash
git config tinker-merge.<branch>.javaHome <path-to-jdk>
```

If no suitable JDK is installed, stop and say so. Skip this step for branches that are not
tested.

## 4. Run it

```bash
.skills/tinker-merge/scripts/merge-forward.sh <source> [--test <mode> | --test-command '<cmd>'] \
    [--skip-test-on <branch>[,<branch>]] [--onto <branch>]
```

A tested run takes a long time, since each branch gets a full build. Run it in the background
with no short timeout. Before the first merge it prints the commits being merged. Read them: for
a pull request or a feature branch, every commit must belong to the work
(`for-committers.asciidoc` makes that the committer's responsibility). Raise any that do not
with the operator and `--abort` unless they confirm.

The exit code says what happened:

| Exit | Meaning | Do |
|---|---|---|
| `0` | Every branch merged and tested | Go to step 5 |
| `1` | Refused before anything moved | Report the message. Do not work around it. |
| `2` | Merge conflict, left in progress on that branch | Resolve per the conflict rules below, then `--continue` |
| `3` | Tests failed on a branch; its log path is printed | Report the failure and stop |

`--continue` resumes where the run stopped, and `--abort` returns every branch to where it was
before the run, keeping any local commits the operator made before asking for the merge.

### Conflicts

Commit a resolution with git's default merge message (`git commit --no-edit`), then run
`--continue`.

- **Changelog and version files** (`CHANGELOG.asciidoc`, version numbers in `pom.xml` and the
  GLV equivalents) are the expected conflicts. Keep both sides of a changelog conflict, with the
  incoming entry under its own release's section. Keep the target branch's version numbers.
- **Any other file:** resolve it, and in your report name each file and what you chose.
- **If testing was skipped and the conflict is outside changelog and version files,** stop and
  ask the operator before committing, since nothing would check the resolution.

### Test failures

A failure stops the run. Report the branch, the failing module and the relevant part of the log,
then wait. Fix nothing unless the operator asks. If they do, commit the fix on the branch that
failed first. Never weaken or skip a test to get through.

`--continue` tests the failed branch again, as many times as the operator asks:

- **Plain `--continue`** reruns the run's test command in full.
- **`--continue --test-command '<cmd>'`** runs `<cmd>` instead, once, on that branch only. The
  usual case is a Maven build resumed from the module that failed, such as
  `mvn clean install -rf :gremlin-server`, since the modules before it already passed. The next
  branch goes back to the run's test command. Use it only when the operator gives the command or
  asks to resume.

## 5. Hand off

Report what each branch now holds beyond the remote, give the push command the script printed
(`git push --atomic ...`, newest branch first), and stop. If the operator decides against the
result, run `--abort`.

When the operator says the push has landed and the work has a root bead, continue with the
pin-at-merge step in `.beads/PRIME.md` (section 4). The commit hashes are final at that point.
