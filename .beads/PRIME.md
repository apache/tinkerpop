# TinkerPop Beads Workflow

Beads is TinkerPop's planning system **and its long-term memory**. It records not just what
changed, but why — decisions made, alternatives rejected, and directions abandoned. Treat
every bead as something a contributor will read in three years.

## Workflow

An index. Each line names a section; **the section is the rule, and the section holds the
exceptions.** Do not act on a line here without reading it.

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

These hold throughout — while planning with the operator and while executing.

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
          --design="what Y concretely was, why it lost, what settled it"
bd dep add <decision> <alternative> -t related    # never put either of these on a task bead
bd close <decision> <alternative>   # both: a decision is resolved the moment you write it
```

**Close a decision as you create it.** It is a record of something already settled, not work
waiting to happen, and a rejected alternative is a road already closed. Leaving it `open` puts
it in `bd ready` — the queue then advertises reasoning as startable work — and stamps its
`closed_at` with the merge date, which says the decision was made on a day it was not. It gets
pinned with the rest of the subtree at merge (section 4).

**A rejected alternative's `--design` names three things: what the option concretely was, why it
lost, and what settled it.** The verdict is already in the label, so the reason is the whole
value — give it as a mechanism ("it leaves the shared database with a vocabulary no single
PRIME.md describes"), never a judgment ("rejected as worse"). A mechanism can be checked again
later: when the problem it names no longer applies, the option is worth reconsidering. A
judgment cannot, so the option stays dead by default. The chosen decision needs a reason too.

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
- A small fix is a lone bead. It is its own root; don't hunt for a parent.
- A human may decline binding to a bead, in which case ignore these rules.

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

**Before proceeding to the next step, obtain human approval.** - Show the human a summary of
the beads graph for review.

---

## 3. While working — claim, then close

Adjusting the plan mid-flight is normal — section 2 applies again when you do, and section 0
applies the whole time.

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

Status is not paperwork. It is both the handoff and the gate.

---

## 4. At merge — close the root, then pin

Tasks closed as they finished (section 3), decisions as they were written (section 0). The
**root** is the only thing still open at merge — it is the deliverable.

```bash
bd children <root>               # the whole subtree; nothing should still be in_progress
bd close <root>
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

**Never invent a label.** What is listed below is the entire vocabulary. 

```bash
bd create --labels="gremlin-core,3.8"    # at creation
bd label add|remove <id> <label>
bd label list <id>
bd label list-all                        # what is in use — includes drift; this file is the authority
```

**Set every dimension label on the root when you create it, before any child exists.**
`bd create --parent` copies the *parent's* labels onto a child at birth and never again, so a
root labelled up front propagates to the whole subtree for free — and a root labelled afterwards
propagates to nothing. If a label must change later, `bd label add|remove` on the root does not
backfill: apply the change to every existing descendant by hand. Skip that and siblings born
either side of the change disagree, and each new level snapshots whichever version its parent
happened to hold. Structural labels are the exception; they describe the bead itself and belong
wherever they apply.

### Structural labels — part of the data model, never optional

| Label | Why it exists |
|---|---|
| `human` | bd's own contract: `bd human list/respond/dismiss` query this exact string |
| `rejected-alternative` | the chosen decision and the road not taken are **both** `type=decision`; this label is the only thing telling them apart |
| `jira` `pr` `dev-list` `proposal` | which kind of external artifact a `record` bead holds — exactly one per record |

### Dimensions — descriptive; several may apply

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
bd query "status=open AND type=decision"   # should be empty; an open decision was left unclosed
bd comment <id> "..."            # a fact with no fork in it (never on a task bead)
bd create --type=... --parent=<root> --design=... --labels=...
bd dep add <task> <blocker>      # default type is blocks: <task> waits for <blocker>
bd dep add <a> <b> -t caused-by|related|discovered-from|supersedes
bd dep cycles                    # a plan with a cycle cannot execute
bd update <id> --claim | -s pinned | --external-ref=TINKERPOP-NNNN
bd search <text>
```

Priority is `0-4` (0 = critical), never "high"/"medium"/"low".
