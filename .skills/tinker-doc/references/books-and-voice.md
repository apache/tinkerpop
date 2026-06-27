# Books and Voice

Each book in `docs/src/` serves a different reader at a different moment. Getting
a document right starts with knowing which book it belongs to and writing in that
book's voice. The house-style rules in `SKILL.md` (no em-dash or sentence-breaking
semicolon, formal third person, paragraphs over lists, no hype) apply throughout.
The guidance below is what is specific to each book.

---

## Upgrade Documentation

`docs/src/upgrade/` — one file per minor line (`release-3.7.x.asciidoc`, etc.),
newest release section at the top, split into "Upgrading for Users" and, where
relevant, provider-facing notes.

Upgrade documentation **announces** a release. It is the place where a feature is
introduced to the world. This shapes everything about how it reads.

**It is not a coding reference; it tells a story.** An upgrade entry gives the
reader a clear, engaging account of what a feature is and why it matters, then
lets them follow the `See:` references for the full detail when they want it. The
goal is to make the feature interesting and understood, not to specify it. State
the point of the change and show one good example, then stop.

**Do not catalog every corner of the feature.** This is the most common way an
upgrade entry goes wrong. Resist enumerating every overload, every argument
variation, and every runtime behavior (what happens with zero, with a partial
result, when nothing remains, and so on). That exhaustive cataloging is precisely
what the reference and semantics docs are for, and the `See:` link sends the
reader there. A well-chosen example already demonstrates the important behaviors,
so a prose paragraph that narrates each case after it is usually redundant and
should be cut.

A typical upgrade entry is short: a heading, a paragraph or two on what changed
and why it matters, one example, and the `See:` line. The clearest warning sign is
a paragraph after the example that walks through edge cases such as an empty
result, a zero argument, or an exhausted iterator. That reads as specification
rather than story, and it is what the reference docs behind the `See:` link are
for. The test for any sentence is whether it conveys why the feature is useful or
merely specifies how it behaves. The latter belongs in the reference docs.

**Announce without hype.** The tone is positive and informative, never
marketing. Avoid "powerful," "blazing fast," "game-changing," and similar.
Describe the capability plainly and let it be interesting on its own terms.

**Frame past shortcomings positively.** When a release fixes a bug or removes a
limitation, describe it as an improvement to how TinkerPop works now, not as a
confession of how broken it used to be. State what the old behavior was factually,
then what the new behavior is, without self-criticism.

**Lead major changes with history.** When a release changes how TinkerPop does
something fundamental, breaks with tradition, or alters a public API, open the
section with a short account of how the project arrived here. That context is what
makes a disruptive change feel reasoned rather than arbitrary. This history is
often more nuanced than the changeset reveals, so do not try to reconstruct it by
searching the internet or inferring it from the diff. If the history is not
already at hand, ask the human for it.

**Compare old and new syntax in examples.** The most useful upgrade example shows
the before and the after side by side. The established pattern uses comment
labels inside a single block:

```
[source,groovy]
----
// 3.7.6
gremlin> g.inject([null]).conjoin("-")
==>null

// 3.7.7
gremlin> g.inject([null]).conjoin("+")
==>
----
```

**Upgrade examples must always be static blocks.** Use `[source,groovy]` or
`[source,text]`, never an executable `[gremlin-groovy]` block. An upgrade example
is a snapshot of behavior at a specific moment in the project's history, often
contrasting two versions at once. It must not be regenerated against the current
build, which would erase the very before-and-after the example exists to show.

Static does not mean output-free. A static upgrade example should still include
its results, written in by hand, so the reader sees the payoff of the change:

```
[source,groovy]
----
gremlin> g.V().has('age', gt(__.V(1).values('age'))).values('name')
==>josh
==>peter
----
```

When reviewing upgrade docs, a query example shown without its expected results
is a real gap, but the fix is to **add the hand-written `==>` output**, never to
convert the block to executable. Converting it would defeat the snapshot purpose
and is always the wrong call in this book.

**Examples should be impactful, not exhaustive or contrived.** Choose a use case
that catches attention and makes the value obvious. One well-chosen, realistic
example beats five that exercise every option.

**Breaking changes are the priority.** Breaking changes to public APIs and to
Gremlin semantics are the most important thing upgrade documentation does. For
each one, state clearly what breaks, why, and exactly how a user minimizes the
impact (what to change in their code, what to check for). A breaking change
documented without a migration path is incomplete.

**Close an entry with a `See:` block.** Most upgrade entries end with a `See:`
line that sends the reader to where the change can be explored further: the JIRA
issue, the relevant reference documentation, a `[DISCUSS]` mailing-list thread, a
design proposal, or several of these together. The JIRA issue is the most common
and uses the bare ticket id as its link text (`See:
link:...[TINKERPOP-XXXX]`). The full format, including how multiple targets are
listed, is in
[asciidoc-and-wiring.md](asciidoc-and-wiring.md#see-blocks-upgrade-documentation).

**Pin cross-reference links to a concrete version.** This is the one place where
the usual `x.y.z` placeholder rule does *not* apply. Because an upgrade entry is a
snapshot tied to a specific release, a link out to the reference docs (or any
other versioned documentation) must name the actual version it refers to, not
`x.y.z` and not `current`:

```
See: link:https://tinkerpop.apache.org/docs/3.7.4/reference/#metrics[Reference Documentation - Metrics]
```

A `current` or `x.y.z` link would drift as new releases ship and eventually point
the reader at documentation that no longer matches the change being described.
This pinning is peculiar to upgrade documentation. Everywhere else, use the
`x.y.z` placeholder as normal.

---

## Reference Documentation

`docs/src/reference/` — the authoritative, complete description of TinkerPop.
`the-traversal.asciidoc` documents the steps; other files cover variants,
applications, and implementations.

Reference documentation **instructs**. Where upgrade docs introduced a feature,
reference docs are the focal point for its proper, ongoing use. The style is
**user-friendly, complete, informative, and formal**.

The established shape of a step section:

```
[[coalesce-step]]
=== Coalesce Step

The `coalesce()`-step evaluates the provided traversals in order and returns the
first traversal that emits at least one element.

[gremlin-groovy,modern]
----
g.V(1).coalesce(outE('knows'), outE('created')).inV().path().by('name').by(label)
----

*Additional References*

link:++https://tinkerpop.apache.org/javadocs/x.y.z/.../GraphTraversal.html#coalesce(...)++[`coalesce(Traversal...)`]
```

Conventions to follow:

- A dedicated anchor (`[[coalesce-step]]`) and a `=== Title Case Step` heading.
- An opening sentence that defines the step in third person, naming it as
  `` `coalesce()`-step `` and, where the docs do so for its neighbors, tagging
  its category in bold (for example `*filter*` or `*map*`).
- One or more **executable** `[gremlin-groovy,modern]` examples that demonstrate
  real behavior on the toy graph.
- An `*Additional References*` block linking to the javadoc, and, when the step
  has an entry in the Gremlin Semantics documentation, a `` `Semantics` `` link to
  `dev/provider/#<step>-step`. Both use the `x.y.z` version placeholder (see
  `asciidoc-and-wiring.md`).

Completeness matters here in a way it does not in the upgrade docs. The reference
is where a user goes to learn how a step actually behaves in all its normal uses.

---

## Recipes Documentation

`docs/src/recipes/` — one file per pattern.

Recipes share Reference's voice (user-friendly, complete, informative, formal)
but are organized around a **task** rather than a feature. A recipe states a
problem a user genuinely has, then builds the traversal that solves it,
explaining the reasoning as it goes.

The signature recipe device is the **numbered callout**, which attaches prose
explanation to specific lines without breaking the flow of the example:

```
[gremlin-groovy,modern]
----
g.V(1).bothE()                                   <1>
g.V(1).bothE().where(otherV().hasId(2))          <2>
----

<1> There are three edges from the vertex with the identifier of "1".
<2> Filter those three edges using the `where()`-step ...
```

Recipes often layer from a simple case to progressively richer ones, which suits
the "build up a solution" framing. Keep the prose between examples flowing and
formal.

---

## Tutorials

`docs/src/tutorials/` — `getting-started`, `the-gremlin-console`,
`gremlins-anatomy`, `gremlin-language-variants`, each in its own directory.

Tutorials are **story-driven** and cover one focused topic, taking a reader from
unfamiliar to capable. Beyond that narrative arc, they follow the
Reference/Recipes style: complete, informative, and formal. Tutorials make heavy
use of `link:` cross-references out to the published site (with the `x.y.z`
placeholder) to connect the story to the reference material, and they open with
`:docinfo: shared` directives and a logo image.

Note that some existing tutorial prose slips into second person ("inspire you to
new levels"). That is legacy phrasing, not the standard. New and revised tutorial
content should stay in formal third person like the rest of the documentation.

---

## Provider Documentation

`docs/src/dev/provider/` — for graph providers implementing TinkerPop, and to
some extent for advanced users. The voice matches Reference and Recipes:
informative, complete, formal.

### The semantics document

`gremlin-semantics.asciidoc` is the canonical specification of how Gremlin
behaves: equality, comparability, orderability, equivalence, type promotion, and
the semantics of each construct. **It must be updated whenever Gremlin steps, the
grammar (`Gremlin.g4`), or the semantics code change in any way.** Treat it as
part of the change, not an afterthought.

The semantics document is language-agnostic: it specifies Gremlin behavior for
every GLV. Do not name Java-specific types, exception classes, syntax, or library
functions in normative prose. The reference implementation is linked from each
step's `See:` block, and that is the appropriate place for any Java-flavored
specifics. For exceptions, name the TinkerPop error category (`Argument Error`,
`State Error`, `Type Error`, `Arithmetic Error`, `Unsupported Operation`) rather
than a Java exception class.

The document has two parts. The conceptual sections near the top (equality,
comparability, orderability, equivalence, type promotion) specify the
cross-cutting behaviors and change rarely. The large `== Steps` section documents
each Gremlin step individually, and that is where most edits land: per-step
coverage is still incomplete, and new steps, changed semantics, and new overloads
all show up here. Expect to spend your time in `== Steps`, not in the equality and
comparability material.

When adding or revising a step, **follow the per-step template already in use.**
Each step is anchored as `[[<step>-step]]` under a `=== stepName()` heading and
fills in the same labeled fields, in order:

- `*Description:*` — one or two sentences on what the step does.
- `*Syntax:*` — each overload as a backticked signature, multiple overloads
  separated by `|` (for example `` `asString()` | `asString(Scope scope)` ``).
  Use TypeScript-style `name: TYPE` parameter form with GType names for
  primitives (`STRING`, `INT`, `BOOLEAN`, etc.), `any` for unconstrained
  values, and PascalCase for TinkerPop concept types (`Traversal`,
  `Traverser`, `Scope`, `P`, `T`, `GType`, etc.). Varargs use the trailing
  ellipsis form (`STRING...`). Document only the overloads that appear in
  the ANTLR grammar (`gremlin-language/src/main/antlr4/Gremlin.g4`); do
  not document method-level overloads that exist only as Java
  `GraphTraversal` sugar.
- A `[width="100%",options="header"]` table with the columns
  `Start Step | Mid Step | Modulated | Domain | Range`.
- `*Arguments:*` — one bullet per argument. (A genuine enumeration, so a list is
  the right choice here.) Use `None` as the body for steps that take no arguments.
- `*Modulation:*` — describes the modulators the step accepts (such as
  `from()`/`to()` or `by()`). Use `None` as the body for steps that take no
  modulators.
- `*Considerations:*` — prose covering edge cases, grammar restrictions, and any
  GLV-specific notes.
- `*Exceptions:*` — the conditions under which the step raises an error, named by
  the TinkerPop error category rather than a Java exception class. Use `None` as
  the body for steps that raise no errors.
- A closing `See:` line linking to the step's source file(s) and its reference
  entry, using the `x.y.z` placeholder.

A new overload usually means adding its signature to `*Syntax:*` and describing
the new argument under `*Arguments:*`. A semantic change usually means revising
`*Considerations:*` or `*Exceptions:*`. Match a nearby existing step rather than
inventing a new structure, and fill the gap when documenting a step that has no
entry yet.

---

## Developer Documentation

`docs/src/dev/developer/` — internal notes for contributors (contributing,
development environment, for-committers, release, administration).

This is in-project material written for people working on TinkerPop itself. The
formal house style still applies. There is little beyond that to special-case:
keep it accurate, keep it current with the actual process, and match the
structure of the surrounding committer documentation.

---

## IO Documentation

`docs/src/dev/io/` — one file per serialization format (`graphbinary.asciidoc`,
`graphson.asciidoc`, `graphml.asciidoc`, `gryo.asciidoc`).

IO documentation specifies the wire and file formats that providers and advanced
users implement against. It carries the same formal, complete, precise voice as
Provider documentation. Accuracy is paramount: this is a specification, so type
mappings, byte layouts, and format versions must match the implementation
exactly. Match the structure already used for the format being edited rather than
reorganizing it, and keep examples illustrative (`[source,text]` or
`[source,json]`) rather than executable.

---

## Future Documentation

`docs/src/dev/future/` — numbered design proposals (`proposal-*.asciidoc`) for
changes under consideration.

Future documentation captures proposed and planned work, so it reads differently
from the rest. A proposal argues for a direction: it lays out motivation, the
proposed design, alternatives, and open questions. The formal house style still
applies, but the content is forward-looking and provisional rather than a
description of how TinkerPop behaves today. When adding a proposal, follow the
numbering and structure of the existing `proposal-*` files.
