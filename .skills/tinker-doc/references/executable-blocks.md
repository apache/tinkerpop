# Executable Code Blocks

The defining feature of TinkerPop documentation is that most Gremlin examples are
**executed when the docs are built**. Each executable block is fed to a live
Gremlin Console, run against a real graph, and its actual `==>` output is inlined
automatically. This is what keeps the documentation honest, and it has a direct
consequence for how examples are written: the author supplies the Gremlin, never
the output.

## Block types

| Block opener | Executed? | Use for                                                                                                      |
|---|---|--------------------------------------------------------------------------------------------------------------|
| `[gremlin-groovy,modern]` | Yes, against the **modern** toy graph | The default for most examples                                                                                |
| `[gremlin-groovy,existing]` | Yes, against the **existing** graph | Examples that build on or mutate graph state from the immediately previous block                             |
| `[gremlin-groovy]` | Yes, with **no** graph bound | Setup that does not need a graph                                                                             |
| `[source,groovy]` | No | Illustrative Groovy that must not run (hypotheticals, two-version comparisons); **all upgrade-doc examples** |
| `[source,text]` | No | Console sessions, error cases, or specification examples shown verbatim                                      |

Only `modern` and `existing` are available as graph names. The **modern** graph
is the standard six-element toy graph used almost everywhere. Reach for `existing`
only when an example genuinely needs to persist or mutate state that a later block
depends on.

### Never invent output

For any **executed** block, write only the Gremlin lines. Do **not** type the
`==>` result lines yourself. They are filled in automatically. Hand-written output
in an executable block is wrong by definition and will be overwritten or will
conflict with reality.

Conversely, in a non-executed `[source,text]` or `[source,groovy]` block you
write the whole thing, prompts and output included, because nothing runs it. This
is how upgrade docs show before/after behavior and how the semantics document
specifies results.

### Upgrade documentation always uses static blocks

Upgrade documentation must **never** use an executable `[gremlin-groovy]` block.
Every example in the upgrade docs is a static `[source,groovy]` or `[source,text]`
block whose output is written by hand. An upgrade example is a snapshot of how
something behaved at a particular point in the project's history, frequently
showing an old version next to a new one. Regenerating it against the current
build would defeat its purpose, so these examples are deliberately kept static.

## Output suppression: the `;[]` idiom

When a line in an executable block is setup whose output would be noise (a
variable assignment, a schema-building call), append `;[]` to it. The statement
still executes, but it returns an empty list, so no `==>` line is emitted:

```
[gremlin-groovy,modern]
----
v1 = g.V(1).next();[]
v2 = g.V(2).next();[]
g.V(v1).bothE().where(otherV().is(v2))
----
```

Here the two assignments run silently and only the final traversal produces
visible output.

## Numbered callouts

To explain specific lines without interrupting the example, use AsciiDoc
callouts. Mark lines with `<1>`, `<2>`, and so on, then list the explanations
immediately after the block:

```
[gremlin-groovy,modern]
----
g.V(1).bothE()                                   <1>
g.V(1).bothE().where(otherV().hasId(2))          <2>
----

<1> There are three edges from the vertex with the identifier of "1".
<2> Filter those edges with `where()` ...
```

Align the callout markers into a column for readability, as the existing recipes
do. This is the primary explanatory device in the recipes book.

## The multi-language story

The reference examples are written in Gremlin-Groovy. When documenting a step,
write the Groovy example. Do not hand-maintain parallel copies in every language
inside a step section unless the surrounding content already does so. The
dedicated per-language material (connecting, imports, configuration) lives in
`reference/gremlin-variants.asciidoc`, organized under `[[gremlin-python]]`,
`[[gremlin-javascript]]`, and similar anchors.
