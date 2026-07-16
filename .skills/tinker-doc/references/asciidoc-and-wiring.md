# AsciiDoc and Wiring

TinkerPop documentation is AsciiDoc, never Markdown. This reference covers the
structural conventions an agent gets wrong most often. For voice see
`books-and-voice.md`; for runnable examples see `executable-blocks.md`.

## Anchors and cross-references

Define an anchor on its own line above the heading it names, then link to it from
anywhere with `<<anchor,display text>>`:

```
[[coalesce-step]]
=== Coalesce Step

... see the <<where-step,where()-step>> for a related filter.
```

Give every section a stable, descriptive anchor and prefer cross-referencing an
existing section over restating its content.

**`<<anchor>>` only resolves inside its own book.** Each book under `docs/src/`
builds as a separate document, so a cross-reference from one book to another does
not work even when the anchor exists. It also fails *silently* rather than
breaking the build: `<<subgraph-step>>` renders as the literal text
`[subgraph-step]`, and `<<gremlin-java-pdt,Gremlin-Java>>` renders as a dead
same-page link to a fragment the page does not contain.

To point at another book, use a full `link:` to the published site instead:

```
link:https://tinkerpop.apache.org/docs/x.y.z/reference/#labels-step[labels()]
```

This matters most in the upgrade book, which is nearly always talking about
reference, provider, or IO material that lives elsewhere. Note that upgrade docs
pin a concrete version rather than `x.y.z` — see the exception below.

## The `x.y.z` version placeholder

Links into version-specific resources use the literal placeholder `x.y.z` in
place of a release number. The build substitutes the real version. This applies
to javadoc links, links into the published site, and GitHub source links:

```
link:https://tinkerpop.apache.org/docs/x.y.z/reference/#gremlin-console[Gremlin Console]
link:++https://tinkerpop.apache.org/javadocs/x.y.z/core/.../GraphTraversal.html#coalesce(...)++[`coalesce(Traversal...)`]
```

Never hard-code a concrete version like `3.7.6` in these links. The `++...++`
double-plus wrapping is used to escape URLs that contain characters AsciiDoc would
otherwise interpret (such as the parentheses in a javadoc method signature).

**Exception: upgrade documentation.** Links from the upgrade docs to other
versioned documentation are deliberately pinned to a concrete version (for
example `.../docs/3.7.4/reference/...`), not `x.y.z` and not `current`, because an
upgrade entry is a snapshot in time. This exception applies *only* to the upgrade
book. See
[books-and-voice.md](books-and-voice.md#upgrade-documentation) for the full
reasoning.

If you need actually use `x.y.z` as a literal value, prefer `xx.yy.zz`.

## Additional References blocks

Reference step sections end with a pointer block under a bold label. It always
carries the javadoc link, and when the step has an entry in the Gremlin Semantics
documentation it also links there with the link text `` `Semantics` ``:

```
*Additional References*

link:++https://tinkerpop.apache.org/javadocs/x.y.z/core/.../GraphTraversal.html#conjoin(java.lang.String)++[`conjoin(String)`]
link:++https://tinkerpop.apache.org/docs/x.y.z/dev/provider/#conjoin-step++[`Semantics`]
```

The semantics link points to `dev/provider/#<step>-step` (the anchor in
`gremlin-semantics.asciidoc`), uses the `x.y.z` placeholder, and is wrapped in
`++...++`. Whenever a step's behavior is specified in the semantics document,
include this link so the reference and the specification stay connected. Match
this format when adding a new step so the reference stays uniform.

## See blocks (upgrade documentation)

Upgrade documentation has its own closing convention. An upgrade entry ends with a
`See:` line that points the reader to the sources where the change can be explored
in more depth. Where reference sections use `*Additional References*`, upgrade
entries use `See:`.

A single pointer:

```
See: link:https://issues.apache.org/jira/browse/TINKERPOP-3225[TINKERPOP-3225]
```

Several pointers are comma-separated, each `link:` on its own line, all under one
`See:`:

```
See: link:https://issues.apache.org/jira/browse/TINKERPOP-2672[TINKERPOP-2672],
link:https://tinkerpop.apache.org/docs/3.7.1/reference/#asString-step[asString()-step],
link:https://tinkerpop.apache.org/docs/3.7.1/reference/#length-step[length()-step]
```

Conventions observed in the upgrade docs:

- **JIRA issues** are the most common target. The link text is the bare ticket
  id, `TINKERPOP-XXXX`, linking to `https://issues.apache.org/jira/browse/TINKERPOP-XXXX`.
- **Reference (and other doc) links** are version-pinned, not `x.y.z` (see the
  upgrade exception above), with descriptive link text such as
  `Reference Documentation - Metrics` or the step name `trim()-step`.
- **Mailing-list threads** point to `https://lists.apache.org/thread/...` with
  link text naming the thread, often beginning `[DISCUSS]`.
- **Proposals** link to the proposal file on GitHub, for example
  `https://github.com/apache/tinkerpop/blob/master/docs/src/dev/future/proposal-scoping-5.asciidoc`,
  with a descriptive title as the link text.

Use `See:` with the colon. A scattering of older entries omit it; the colon is the
standard.

## Book wiring: `index.asciidoc`

Each book has an `index.asciidoc` that sets document attributes and pulls in its
content files with `include::` directives, in reading order:

```
:docinfo: shared
:toc-position: left

include::the-graph.asciidoc[]
include::the-traversal.asciidoc[]
```

A **new content file is invisible until it is included.** After creating a file,
add an `include::` for it to that book's `index.asciidoc` at the correct position.
Forgetting this is the most common reason new documentation does not appear.

## Admonitions

AsciiDoc admonitions call out information without breaking into a list. The four
in regular use, in rough order of frequency, are `NOTE:`, `IMPORTANT:`, `WARNING:`,
and `TIP:`. Use them for genuine asides and cautions, not as a substitute for
well-structured prose:

```
IMPORTANT: This tutorial assumes the Gremlin Console is installed.
```

## Images

Images are referenced with `image::file.png[width=500]` for block images and
`image:file.png[width=130]` (single colon) for inline ones. Image files live
under `docs/static/images/`. Set a sensible `width`, and `align="center"` where
the surrounding content does.

## Quick checklist for a new or changed doc

The list below is a fixed, enumerable set of mechanical steps, which is exactly
the case where a list is appropriate:

1. Right book, right file, AsciiDoc not Markdown.
2. Stable `[[anchor]]` on every new section.
3. Executable examples use `[gremlin-groovy,modern]` (or `existing`); no
   hand-written `==>` output. Non-executed examples use `[source,text]`.
4. Version-specific links use the `x.y.z` placeholder.
5. New files added to the book's `index.asciidoc` via `include::`.
6. Semantics, grammar, or step changes reflected in
   `gremlin-semantics.asciidoc`.
7. User-visible or breaking changes reflected in `CHANGELOG.asciidoc` and the
   upgrade docs.
