# TinkerPop Beads Workflow

Beads is TinkerPop's planning system **and its long-term memory**. It records not just what
changed, but why — decisions made, alternatives rejected, and directions abandoned. Treat
every bead as something a contributor will read in three years. This file is what must 
survive context compaction.

## Core rules

- **Default** — beads is the tracker for **all** work: `bd create`, `bd ready`, `bd close`.
- **Prohibited** — do **not** track work in `TodoWrite`, `TaskCreate`, or a markdown plan
  file. They are session-scoped: nothing in one survives, so nothing in one is memory. Your
  harness may prompt you to use them. Decline.
- **Lifecycle** — create the bead **before** writing code, `--claim` it **before you edit**,
  `bd close` it **as soon as that task's work is done**, and pin the whole subtree at merge.
  A bead that never enters `in_progress` is one no later session can resume, and one that
  never closes leaves everything downstream of it blocked. Status is not paperwork; it is
  both the handoff and the gate.
- **Plan mode** — fine, and the plan file your harness writes is not yours to avoid. But it
  lives outside the repo and outside the graph. Anything you weighed and rejected while
  planning belongs in a bead **before you start executing**, not after.

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
  code.** Work with no bead is the failure that makes every other rule pointless.
- A small fix is a lone bead. It is its own root; don't hunt for a parent.

`bd query "parent=none"` does not work. Filter on the `parent` field client-side.
Re-ask after a compaction rather than guessing.

---

## 2. Plan as a graph, not a list

Tasks are not a checklist. Wire the order between them so the graph itself says what can run
in parallel — that is the whole reason the plan lives in beads instead of prose.

```bash
bd dep add <task> <blocker>          # <task> waits for <blocker> — NOT "task blocks blocker"
bd dep add --file - <<< '{"from":"tp-a","to":"tp-b"}'   # wire a whole plan at once
bd ready                             # only `blocks` gates this; parent-child and related never do
bd blocked                           # what is waiting, and on what
bd dep cycles                        # a plan with a cycle cannot execute
```

- **Wire the order in the same pass as `bd create`.** Retrofitting it after work starts is
  how you end up with a flat star and no parallelism.
- **A task with no blocker asserts it can start immediately.** The absence of an edge is a
  claim, not an oversight — decide it deliberately for every task.
- **A blocked task is released when its blocker closes**, so tasks must close as they finish
  rather than at merge. Hold them all open until the PR lands and the graph never advances.
- **Link a decision to the work it caused** — `bd dep add <task> <decision> -t caused-by`.
  Without it there is no path from a task back to the reasoning that shaped it.

Before you start executing, run `bd ready`. If it returns every task you created, you built
a list and called it a plan.

---

## 3. While working — claim first, then capture as you go

**Before you touch code for a bead, claim it. Every time, no exceptions:**

```bash
bd update <id> --claim            # sets assignee to you, status to in_progress
```

That window **is** the memory: a later session runs `bd list --status=in_progress` and learns
what was underway, who had it, and where it stopped. A bead that jumps from `open` straight
to `closed` records that the work happened but never that it was yours, never where you were
when context ran out.

If you are editing files and nothing is `in_progress`, you have already lost that. Stop and
claim the bead you are actually working on.

**Close a task the moment its work is done — do not wait for the merge.** Closing is what
releases the tasks that were waiting on it, so a task left `in_progress` out of caution
stalls everything downstream. The root is the exception: it represents the deliverable and
closes at merge (section 4).

**Then watch for these five things. They are observable events, not judgment calls:**

1. **The operator redirects you** — "no, do X instead", "we tried that", "that breaks
   providers". Highest signal. Capture every time.
2. **What you built diverged from the JIRA / proposal / dev@ thread.**
3. **An approach was tried and abandoned.**
4. **You presented options** — a decision point exists by construction.
5. **A discovery contradicted an assumption.**

**Then pick the instrument. The only test is whether a road was not taken:**

| What happened | Do |
|---|---|
| A specific course was considered and **not taken** — a design, a scope item, a validation step, a target branch, a task you wrote and threw away | Decision bead **plus** its `rejected-alternative` sibling, now |
| Something is simply true, with no fork in it — evidence, a measurement, a discovery, a constraint | `bd comment <root> "..."` |

**The rejected thing does not have to be a design.** "The operator declined X" is a road not
taken. So is "we were going to target master, we targeted 3.7-dev instead." If you can name
what was *not* done, it is a decision — write both beads.

**Self-check before writing any comment: name what was *not* done.** If you can name it — a
course declined, a branch not targeted, an approach dropped — it is a decision bead, not a
comment. Wording like "the operator declined" or "X rather than Y" is the tell, but check
what it refers to: a choice about *the work* is a decision, while "the test frames HashMap
instead of OptionsStrategy" is just describing code and stays a comment.

```bash
# Only when something was actually ruled out. No fork = implementation; the code documents that.
bd create --type=decision --parent=<root> --title="Chose X" --design="why, and what X rules out"
# The sibling is the road not taken — an approach you tried and abandoned counts, and is stronger
# evidence than a hypothetical, because someone already walked it.
bd create --type=decision --parent=<root> --title="Y" --labels="rejected-alternative" \
          --design="why Y was rejected"
bd dep add <decision> <alternative> -t related    # never put either of these on a task bead
```

**Record what actually happened.** If you cannot point to the moment, do not write the bead.
When you sense a decision you were not party to, create a bead labelled `human` posing the
question instead of inventing an answer — `bd human respond <id>` turns the reply into a
comment.

---

## 4. At merge — close the root, then pin

Tasks closed as they finished (section 3). What is left at merge is the **root** — the
deliverable — plus any decision beads, which are not work and never closed on their own.

```bash
bd children <root>               # the whole subtree; nothing should still be in_progress
bd close <root> <decision-ids>   # whatever the work itself did not close
bd update <id1> <id2> ... -s pinned
bd dolt pull && bd dolt push
```

**Pin every bead in the subtree** — root, decisions, records, tasks. No judgment about
which ones matter: the work shipped, so all of it is the project's history. Show the
operator the list first if they want a review gate.

Pinning is what makes a bead permanent — every destructive operation keys on
`status=closed`, and pinned beads are never eligible.

Push freely as a checkpoint; pinning is what marks the durable record.

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
  ├─relates-to──▶ record [jira]      TINKERPOP-3456
  ├─relates-to──▶ record [pr]        apache/tinkerpop#2891
  ├─parent-child─▶ decision  "chose X"
  │                  └─related─▶ decision "Y" [rejected-alternative]
  ├─parent-child─▶ task A "implement X" ──caused-by──▶ decision "chose X"
  ├─parent-child─▶ task B ──blocks──▶ task A     (B waits for A)
  └─parent-child─▶ task C                        (no blocker: starts with A)
```

`parent-child` gives membership, `blocks` gives order. A subtree with no `blocks` edges is
a list, and `bd ready` cannot tell you anything useful about it.

- `--parent` builds the tree. Labels inherit downward — see section 7.
- **`record` beads** hold external artifacts — JIRA, PR, dev@ thread, proposal. Kind is a
  **label** (`jira`, `pr`, `dev-list`, `proposal`); the URL or ticket goes in
  `--external-ref`. Attach them to the **root**, not to every bead. Create them pinned.
  Search first — duplicates are the main risk.
- Records are the only link between beads and code. There is no bead ID in commit messages.
- Only **one dependency type per pair** — `blocks` and `discovered-from` cannot coexist
  between the same two beads.
- Never construct a bead ID; use whatever `bd create` returns. Child IDs encode birth
  position (`<root>.1.2`) but do not update on reparenting — traverse `parent` for truth,
  treat the ID as a hint.

---

## 7. Labels

Labels are categorization **orthogonal to type and priority** — a bead carries as many as
apply, giving cross-cutting views the tree cannot.

> **Never invent a label.** What is listed below is the entire vocabulary. A label that
> exists in the database but is not listed here is drift, not precedent — do not copy it.
> If a bead genuinely needs something absent from these lists, that is the operator's
> decision, not yours: raise it with a `human` bead and proceed without the label.

```bash
bd create --labels="gremlin-core,3.8"    # at creation
bd label add|remove <id> <label>
bd label list <id>
bd label list-all                        # what is in use — includes drift; this file is the authority
```

**Set module and release labels once on the root** — children inherit them. Labels added to
a root *after* its children exist do not backfill, so label the root first.

### Structural labels — part of the data model, never optional

| Label | Why it exists |
|---|---|
| `human` | bd's own contract: `bd human list/respond/dismiss` query this exact string |
| `rejected-alternative` | the chosen decision and the road not taken are **both** `type=decision`; this label is the only thing telling them apart |
| `jira` `pr` `dev-list` `proposal` | which kind of external artifact a `record` bead holds — exactly one per record |

### Dimensions — descriptive; several may apply

**Module** — the Maven module name, verbatim. If it is not a directory with a `pom.xml`, it
is not a module label. Sub-trees use their parent's label.

```
gql-gremlin  gremlin-annotations  gremlin-console  gremlin-core     gremlin-dotnet
gremlin-driver  gremlin-go  gremlin-groovy  gremlin-js  gremlin-language
gremlin-python  gremlin-server  gremlin-shaded  gremlin-test  gremlin-tools
gremlin-util  hadoop-gremlin  spark-gremlin  tinkergraph-gremlin  docs
```

**Release** — the release line, not the branch. Branches get renamed; the bead outlives them.

```
3.7   3.8   4.0
```

**Concern** — a cross-cutting property that changes what someone must do about the change.

```
breaking-change  deprecation  security  serialization  protocol  performance  release  build
```

---

## Essential commands

```bash
bd children <root>               # the subtree, recursive
bd ready                         # what can be started now (blocks-aware)
bd blocked                       # what is waiting, and on what
bd show <id>                     # one bead with dependencies
bd query "status=open AND type=decision"
bd comment <id> "..."            # a fact with no fork in it (never on a task bead)
bd create --type=... --parent=<root> --design=... --labels=...
bd dep add <task> <blocker>      # default type is blocks: <task> waits for <blocker>
bd dep add <a> <b> -t caused-by|related|discovered-from|supersedes
bd dep cycles                    # a plan with a cycle cannot execute
bd update <id> --claim | -s pinned | --external-ref=TINKERPOP-NNNN
bd search <text>
```

Priority is `0-4` (0 = critical), never "high"/"medium"/"low".
