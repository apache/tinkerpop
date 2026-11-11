<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
-->

# Blind Design Pass — Reference

The mechanics behind SKILL.md's blind design pass. The gate and the grading live
in `playbooks/general.md` (**Inspect**, under *High-impact design*, and
**Interpret**); this file covers how to
pick the surfaces, what goes in the subagent's bundle, what it must return, and
how its objections are reconciled against the PR's beads.

## Why it exists

The reviewer reads the PR's beads before it judges the design, so it inherits
the author's framing: the problem as they stated it, the options they weighed,
the reasons they gave. Decision beads are trusted by default — contributors
reason them out with care — and the fact check in `general.md` (does the code do
what the bead says) applies to all of them. What the fact check cannot do is
notice an objection nobody wrote down. A subagent that never sees the beads or
the PR's own argument is the source of those objections. It produces candidates
only; reconciliation decides which survive.

## 1. Pick the high-impact surfaces (from the diff)

Work from the changed files, not from the beads, so that a high-impact change no
bead covers is still picked up. A surface is one coherent thing the change
commits the project to. Tag each with one kind:

| Kind | What counts | Examples |
|------|-------------|----------|
| `api` | A public Java/GLV type, method signature or interface contract; a configuration key or format; a wire protocol or request/response field | a changed `execute()` return type; a new server setting |
| `behavior` | Documented, user-visible behaviour: query results, row counts, defaults, error behaviour, step semantics | a step that now returns one row instead of two; a new default |
| `grammar` | Syntax added to or changed in `Gremlin.g4`, `GQL.g4` or another parsed language | a new quantifier production; a new keyword |
| `format` | A serialization, persisted or interchange format | a GraphBinary type code; a storage file layout |

A surface that is internal — implementation structure, test harnesses, build
plumbing, private helpers — is not high impact, even when the change to it is
large. If the diff has no high-impact surface, the pass is skipped: say so in
`appendixBlindDesign.skipped` and move on.

Keep the list short and coarse — three to six surfaces for a large PR, one or two
for a small one. One surface per decision a user or provider would have to live
with, not one per file.

## 2. Build the bundle

Write everything the subagent may read into `/tmp/pr-review-<pr>/blind/`. It
reads only that directory.

| File | Contents |
|------|----------|
| `surfaces.md` | The surface list: id (`S1`, `S2`, …), kind, one line naming it |
| `S<n>-after.*` | The surface's contract as the PR leaves it. `api`: the public signatures and their javadoc/docstrings, not method bodies. `behavior`: the reference and upgrade doc text that describes it, plus the Gherkin scenarios that pin it. `grammar`: the changed rules. `format`: the format's spec or doc text. |
| `S<n>-before.*` | The same surface on the base branch, or `(new)` if it did not exist. This is what lets the subagent judge what breaks. |
| `release.md` | Per surface, whether it exists in the most recent release tag of the base branch's line, so the subagent can tell a one-way door from a change to unreleased work. |
| `issue.md` | The JIRA text (title and description) the PR addresses, without comments. |
| `standards.md` | The external standards the surfaces implement, named only (`ISO GQL quantified path patterns`, `RFC 7231`). Omit it if none apply. |

Gather the before contract and the release status with plain git, from the
review worktree:

```bash
git -C /tmp/pr-review-<pr>/src show <baseSha>:<path>                 # before contract
git -C /tmp/pr-review-<pr>/src tag --list '<line>.*' --sort=-v:refname | head -1   # latest release tag
git -C /tmp/pr-review-<pr>/src cat-file -e <tag>:<path> && echo shipped             # file present at that tag
```

When the revision and path come from shell variables, brace and quote them —
`git show "${sha}:${path}"` — because zsh reads `$sha:g…` as a history modifier
and silently produces the wrong argument.

For a member of a file that already shipped, check the member itself
(`git show <tag>:<path> | grep -n '<signature>'`), since a file can predate the
surface it now holds.

**Withheld** — never put these in the bundle or the prompt:

- the PR's beads, decision or otherwise;
- the PR description and the PR and JIRA comment threads (they argue the design
  as well as state it);
- the knowledge graph, `evidence.json` and its checks;
- the reviewer's Inspect notes and candidate findings;
- the functional test's battery and results;
- implementation bodies and test code.

The subagent judges design, not bugs; implementation pulls attention to bugs,
which the other phases already cover.

## 3. Brief the subagent

Brief it as an **experienced TinkerPop committer who also implements a graph
provider**, seeing this change for the first time with only the bundle. (The
functional tester is the opposite persona — a newcomer with only the docs; the
two complement each other.) The prompt asks, per surface:

1. **Commitment** — one line: who depends on this surface (users, providers,
   GLV maintainers) and what changing it after release would break.
2. **Objections** — at most three, each with:
   - `type`: `premise` (a claim the surface rests on is wrong, e.g. about a
     standard or released behaviour), `counterargument` (a consequence the
     design creates that someone would object to), or `alternative` (a credible
     different design);
   - `claim`: the objection in one or two sentences;
   - `consequence`: what goes wrong for whom if the objection holds;
   - `settledBy`: the evidence that would confirm or dismiss it;
   - `fromMemory`: `true` when the claim rests on the subagent's recollection of
     a standard or spec rather than on text in the bundle.
3. **No objection** is a valid answer for a surface, and the prompt must say so.
   An objection invented to fill the slot costs the reviewer more than a missing
   one.

The subagent returns JSON:

```json
{
  "surfaces": [
    {
      "id": "S1",
      "commitment": "…",
      "objections": [
        { "id": "S1.a", "type": "alternative", "claim": "…", "consequence": "…",
          "settledBy": "…", "fromMemory": false }
      ]
    }
  ]
}
```

## 4. Reconcile against the beads

The reviewer — who has read the beads — takes each objection in turn, finds the
decision (if any) that governs its surface, and applies the challenge questions
from `general.md`'s Inspect:

| The objection… | Outcome |
|----------------|---------|
| contradicts a decision's premise, and checking the source (the spec, the release, the code) confirms the objection | **finding**, high |
| contradicts a premise, but the source does not confirm it, or the claim is `fromMemory` and the source is unavailable | **open question**, stating what could not be checked |
| raises a consequence the decision's reasoning never engages | **open question** naming the decision and the consequence |
| proposes an alternative missing from the decision and its rejected siblings | **open question** naming the decision and the alternative |
| is already answered by a decision or a rejected sibling | **dropped**, silently — the record holds |
| concerns a surface no decision governs | **open question**: a high-impact choice with no recorded reasoning |

When the PR has no beads at all, there is no record to reconcile against, and
the last row does not apply: a PR without beads is not a finding (see
`general.md`). Weigh each objection on its own instead — a premise the source
confirms is wrong is still a high finding, and a counterargument or alternative
is an open question addressed to the author.

"Answered" means the record engages the objection, not that the reviewer would
have reached the same conclusion. A decision the operator settled by choosing
between recorded options is answered; disagreeing with the choice is not a
reason to raise it.

The reviewer may add challenges of its own from reading the beads; they go
through the same table. **At most three challenges reach the report**, blind and
reviewer-raised together, ranked by the impact of the surface. When the blind
pass and the reviewer raise the same point, merge them and rank it first. If more
than three survive, that is itself a signal: name it once as an open question
about the PR's scope rather than listing them all.

Record every objection and its outcome in `appendixBlindDesign` (see
`references/interfaces.md`) so a reader can trace each surviving challenge back
to the objection that raised it, and see which ones the beads answered.
