# TinkerPop Beads Workflow

Beads is TinkerPop's planning system **and its long-term memory**. It records not just what
changed, but why — decisions made, alternatives rejected, and directions abandoned. Treat
every bead as something a contributor will read in three years.

## Workflow

An index. **The section is the rule and holds the exceptions** — do not act on a line here
without reading it.

0. **Think in graph; capture every road not taken — throughout, not a step** — section 0
1. **Bind to a root bead before you write code** — section 1
2. **Plan the work with the operator as a dependency graph** — section 2
3. **Claim before editing, close as work finishes** — section 3
4. **At merge, close the root and pin the whole subtree** — section 4
5. **Never rewrite or discard history** — section 5
6. **Records, edge types and bead IDs follow fixed conventions** — section 6
7. **Never invent a label** — section 7

---

## 0. Core rules

**Think in graph.** Work, decisions, external artifacts and the relations between them are
nodes and edges. The plan lives in beads and nowhere else: not `TodoWrite`, not `TaskCreate`,
not a markdown plan file. Those are session-scoped, so nothing in one is memory. Your harness
may prompt you to use them. Decline.

**A decision needs a bead to hang off**, so create the root when the conversation starts, not
when the code does.

**Watch for these five things. They are observable events, not judgment calls:**

1. **The operator redirects you** — "no, do X instead", "we tried that", "that breaks
   providers". Highest signal. Capture every time.
2. **What you built diverged from the JIRA / proposal / dev@ thread.**
3. **An approach was tried and abandoned.**
4. **You presented options** — a decision point exists by construction.
5. **A discovery contradicted an assumption.**

**Then pick the instrument. The only test is whether a road was not taken:**

| What happened | Do |
|---|---|
| A specific course was considered and **not taken** — a design, a scope item, a validation step, a target branch, a task you wrote and threw away | Decision bead **plus** its rejected sibling, now |
| Something is simply true, with no fork in it — evidence, a measurement, a discovery, a constraint | `bd comment <root> "..."` |

**The rejected thing does not have to be a design.** Before writing any comment, try to name
what was *not* done — a course declined, a branch not targeted, an approach dropped. If you
can, it is a decision, so write both beads. "The operator declined X" and "we were going to
target master, we targeted 3.7-dev instead" both qualify. Check what the phrasing refers to,
though: a choice about *the work* is a decision, while "the test frames HashMap instead of
OptionsStrategy" describes code and stays a comment.

```bash
# Only when something was actually ruled out. No fork = implementation; the code documents that.
bd create --type=decision --parent=<root> --title="Chose X" --metadata '{"rejected":false}' \
          --design="why, and what X rules out"
# An approach you tried and abandoned is the strongest sibling — someone already walked it.
bd create --type=decision --parent=<root> --title="Y" --metadata '{"rejected":true}' \
          --design="what Y concretely was, why it lost, what settled it"
bd dep add <decision> <alternative> -t related    # never put either of these on a task bead
bd close <decision> <alternative>   # both: a decision is resolved the moment you write it
```

**Close a decision as you create it.** Left `open` it lands in `bd ready`, advertising
reasoning as startable work, and its `closed_at` ends up stamped with the merge date — dating
the decision to a day it was not made. It gets pinned with the rest of the subtree at merge
(section 4).

**A rejected alternative's `--design` names three things: what the option concretely was, why it
lost, and what settled it.** The verdict is already in `rejected`, so the reason is the whole
value — give it as a mechanism ("it leaves the shared database with a vocabulary no single
PRIME.md describes"), never a judgment ("rejected as worse"). A mechanism can be checked again
later: when the problem it names no longer applies, the option is worth reconsidering. A
judgment cannot, so the option stays dead by default. The chosen decision needs a reason too.

**If you cannot say why the operator rejected it, ask before writing the bead.** An inferred
reason is indistinguishable from a recorded one, so a guess does not read as a guess three years
later — it reads as fact and gets trusted. "The operator preferred the other one" is the verdict
again, not a reason. Ask what the chosen option buys and what the rejected one would have cost,
and write that answer. Ask while the conversation is live: section 5 forbids editing a design in
place, so a reason invented now is permanent. Doubt is the trigger — if you are reconstructing
rather than recalling, you are guessing.

**Cite code as `file[sha]`** — `GryoPool.java[bece4a34c7]`, with enough path to be unambiguous
(`pom.xml` is 36 different files). No line numbers, no symbol names: both drift, and the
sentence around the citation already says what it points at. The sha has to be reachable from a
published branch, because a local commit can be rebased away before it merges.

**Record what actually happened.** If you cannot point to the moment, do not write the bead.
When you sense a decision you were not party to, create a bead labelled `human` posing the
question instead of inventing an answer — `bd human respond <id>` turns the reply into a
comment.

---

## 1. Start here — bind to a root

Every session works under one **root bead**. Find it before writing code.

```bash
bd list --status=all --json      # filter client-side: no parent, open/in_progress
bd children <root>               # recursive — the whole subtree
```

- Show the operator open/`in_progress` beads with **no parent**, most recently updated
  first, and ask which one. That is your root for this session.
- **Read `bd children <root>` before resuming work.** It is the only thing that makes you
  notice a bead the work has since outgrown.
- **If no root is selected, you are starting something new — create the root before writing
  code.**
- **Ask for the external artifact whenever you create a root** — JIRA, PR, proposal, dev@
  thread. Only the operator knows which one, and a record is the sole link from the work back
  to the code (section 6). A small fix may genuinely have none, so take that answer and move on.
- A small fix is a lone bead. It is its own root; don't hunt for a parent.
- The operator may decline binding to a bead, in which case ignore these rules.

`bd query "parent=none"` does not work. Filter on the `parent` field client-side.
Re-ask after a compaction rather than guessing.

---

## 2. Plan as a graph, not a list

The plan is built **with the operator**; what you weigh and reject while building it is
captured as you go (section 0), not once you start executing.

Tasks are not a checklist. Wire the order between them so the graph itself says what can run
in parallel.

```bash
bd dep add <task> <blocker>          # <task> waits for <blocker> — NOT "task blocks blocker"
bd dep add --file - <<< '{"from":"tp-a","to":"tp-b"}'   # wire a whole plan at once
bd ready --parent <root>             # what can start now, in YOUR subtree
bd blocked --parent <root>           # what is waiting, and on what
bd dep cycles                        # a plan with a cycle cannot execute
```

**Always scope `bd ready` and `bd blocked` to your root.** Unscoped they span the whole
database and hand you other people's work.

- **Wire the order in the same pass as `bd create`.**
- **Re-planning is normal; record it.** `bd dep remove` deletes an edge with no trace in the
  graph, so a restructure erases the shape you started with. If you rewire because you found
  a better path, that is a road not taken — write the decision bead.
- **A task with no blocker asserts it can start immediately.** The absence of an edge is a
  claim, not an oversight — decide it deliberately for every task.
- **A blocked task is released when its blocker closes**, so tasks must close as they finish.
- **Link a decision to the work it caused** — `bd dep add <task> <decision> -t caused-by`.
  Without it there is no path from a task back to the reasoning that shaped it.

**Show the operator a summary of the graph and get approval before executing it.**

---

## 3. While working — claim, then close

Adjusting the plan mid-flight is normal — section 2 applies again when you do, and section 0
applies the whole time.

**Before you touch code for a bead, claim it. Every time, no exceptions:**

```bash
bd update <id> --claim            # sets assignee to you, status to in_progress
```

That window **is** the memory: a later session runs `bd list --status=in_progress` and learns
what was underway, who had it, and where it stopped. A bead that jumps from `open` straight to
`closed` loses all three.

If you are editing files and nothing is `in_progress`, you have already lost that. Stop and
claim the bead you are actually working on.

**Close a task the moment its work is done — do not wait for the merge.** Closing is what
releases the tasks that were waiting on it, so a task left `in_progress` out of caution
stalls everything downstream. The root is the exception: it represents the deliverable and
closes at merge (section 4).

---

## 4. At merge — close the root, then pin

Tasks closed as they finished (section 3), decisions as they were written (section 0). The
**root** is the only thing still open at merge — it is the deliverable.

```bash
bd children <root>               # the whole subtree; nothing should still be in_progress
bin/beads-commits.py --root <root> --branch 3.7-dev --suggest   # propose; operator confirms
bin/beads-commits.py --root <root> --branch 3.7-dev --commits <sha>...
bd close <root>                  # an epic root needs --force; see below
bd update <id1> <id2> ... -s pinned
bd dolt pull && bd dolt push
```

**The operator says when the work has merged**, the way they say a JIRA issue is ready to
resolve. Never infer it. Before the merge the shas are not final, because squash and rebase
rewrite a branch until it lands; after it, TinkerPop's forward merges carry the published sha
unchanged into 3.8-dev and master, so what is recorded then stays correct.

**Then suggest, confirm, record.** `--suggest` finds the commits from the root's own JIRA and PR
records, lists what else landed in the same window, and writes nothing. Show what it returns, take the operator's corrections, and only then
run the recording form. It refuses any sha not reachable from `origin/<branch>`, so a refusal
means the work has not actually landed — say so and leave the root open.

**An `epic` root needs `bd close <root> --force`.** The commit record just written is a
child and is pinned, and the gate counts only `closed` children as done, so the record is
what blocks the close. Do not reorder the two steps to dodge it: the record write is what
refuses a sha unreachable from `origin/<branch>`, and closing first would resolve the root
before that check runs. `task` and `feature` roots do not gate.

**Pin every bead in the subtree** — root, decisions, records, tasks. No judgment about
which ones matter: the work shipped, so all of it is the project's history. Show the
operator the list first if they want a review gate.

Pinning is what makes a bead permanent — every destructive operation keys on
`status=closed`, and pinned beads are never eligible. Push freely as a checkpoint.

---

## 5. Never

- **Never `bd flatten`, `bd compact`, or `bd admin compact`.** They rewrite or discard
  history irreversibly. `admin compact` destroys `--design` text specifically. `bd gc` only
  with `--skip-decay`.
- **Never edit an existing bead's `--design` in place.** Add a comment, or create a new
  decision bead with a `supersedes` edge. Field rewrites are invisible to history and lose
  the reasoning that was there.
- `bd prune` / `bd purge` / `bd gc` are release-time maintainer operations. Don't run them.
- Don't use `bd edit` — it opens `$EDITOR` and blocks.

---

## 6. Structure

```
root (feature/epic/task)
  ├─relates-to──▶ record             TINKERPOP-3456
  ├─relates-to──▶ record             apache/tinkerpop#2891
  ├─relates-to──▶ record             apache/tinkerpop@374b0c76d0   (written at merge)
  ├─parent-child─▶ decision  "chose X"            {rejected: false}
  │                  └─related─▶ decision "Y"     {rejected: true}
  ├─parent-child─▶ task A "implement X" ──caused-by──▶ decision "chose X"
  ├─parent-child─▶ task B ──blocks──▶ task A     (B waits for A)
  └─parent-child─▶ task C                        (no blocker: starts with A)
```

`parent-child` gives membership, `blocks` gives order. A subtree with no `blocks` edges is
a list, and `bd ready` cannot tell you anything useful about it.

- `--parent` builds the tree, and copies the parent's labels onto the child once, at birth —
  see section 7.
- **`record` beads** hold external artifacts — JIRA, PR, dev@ thread, proposal, and the
  commits a landing merged as. The ticket, URL or `owner/repo@sha` goes in `--external-ref`,
  which identifies the kind as well. Attach them to the **root**, not to every bead. Create
  them pinned. Search first — duplicates are the risk.
- Records are the only link between beads and code; commit messages carry no bead ID. The
  commit record written at merge (section 4) is what makes that link work in reverse —
  `bd query 'notes="<sha>"' --all` returns the record and its `parent`, for any commit in the
  landing. Quote the sha: an unquoted one starting with a digit is lexed as a number and the
  query fails to parse.
- Only **one dependency type per pair** — `blocks` and `discovered-from` cannot coexist
  between the same two beads.
- Never construct a bead ID; use whatever `bd create` returns. Child IDs encode birth
  position (`<root>.1.2`) but do not update on reparenting — traverse `parent` for truth,
  treat the ID as a hint.

---

## 7. Labels

Labels are categorization **orthogonal to type and priority**, giving cross-cutting views the
tree cannot.

**Never invent a label.** What is listed below is the entire vocabulary. 

```bash
bd create --labels="gremlin-core,3.8"    # at creation
bd label add|remove <id> <label>
bd label list <id>
bd label list-all                        # what is in use — includes drift; this file is the authority
```

**Set every label on the root when you create it, before any child exists.**
`bd create --parent` copies the *parent's* labels onto a child at birth and never again, so a
root labelled up front propagates to the whole subtree for free — and a root labelled afterwards
propagates to nothing. If a label must change later, `bd label add|remove` on the root does not
backfill: apply the change to every existing descendant by hand. Skip that and siblings born
either side of the change disagree.

**A fact about the bead itself is metadata, not a label.** `--metadata '{"rejected":true}'`
marks the road not taken — both siblings are `type=decision`, and nothing else tells them
apart. Metadata never inherits, which is why it suits such facts. `bd query` compares it with
`=` only and case-sensitively, and hides closed beads unless you pass `--all` — which every
decision is, so the flag is mandatory when looking for one. `human` stays a label because bd's own
`bd human list/respond/dismiss` queries that literal string — never rename it.

Every label is descriptive and several may apply.

**Module** — a unit of code. The list spans every maintained branch, so it includes modules
this branch does not have. Beads outlive branches, and one vocabulary keeps the database
readable from all of them.

```
gql-gremlin  gremlin-annotations  gremlin-archetype  gremlin-console  gremlin-core
gremlin-dotnet  gremlin-driver  gremlin-go  gremlin-groovy  gremlin-javascript
gremlin-js  gremlin-language  gremlin-python  gremlin-server  gremlin-shaded
gremlin-test  gremlin-tools  gremlin-util  hadoop-gremlin  neo4j-gremlin
spark-gremlin  sparql-gremlin  tinkergraph-gremlin  docs
gremlint  gremlator  gremlin-mcp
```

**Release** — the official release version, not the branch - examples: `3.7.7`, `4.0.0-beta.2`

**Concern** — a cross-cutting property or feature.

```
breaking-change  deprecation  security  serialization  protocol  performance  
release  build  
```

---

## Essential commands

```bash
bd children <root>               # the subtree, recursive
bd ready --parent <root>         # startable now; ALWAYS scope to your root
bd blocked --parent <root>       # what is waiting, and on what
bd show <id>                     # one bead with dependencies
bd query "status=open AND type=decision"    # should be empty; an open decision was left unclosed
bd query "metadata.rejected=true" --all     # roads not taken; --all or closed beads are hidden
bd comment <id> "..."            # a fact with no fork in it (never on a task bead)
bd create --type=... --parent=<root> --design=... --labels=...
bd dep add <task> <blocker>      # default type is blocks: <task> waits for <blocker>
bd dep add <a> <b> -t caused-by|related|discovered-from|supersedes
bd dep cycles                    # a plan with a cycle cannot execute
bd update <id> --claim | -s pinned | --external-ref=TINKERPOP-NNNN
bd search <text>
bd query 'notes="<sha>"' --all   # which root did this commit come from; quote the sha
```

Priority is `0-4` (0 = critical), never "high"/"medium"/"low".
