---
name: tinker-doc
description: >
  Writing, editing and reviewing documentation for the Apache TinkerPop project. Use 
  when  authoring or revising any content under docs/src/ — reference docs, recipes,
  tutorials, upgrade/release notes, provider and developer guides — or the
  CHANGELOG. Covers the TinkerPop documentation voice and the per-book style
  expectations, the executable Gremlin code-block pipeline, and AsciiDoc
  conventions. For building and validating code changes, see tinker-dev.
license: Apache-2.0
compatibility: Documentation is AsciiDoc under docs/src/, never Markdown.
metadata:
  version: 1.0.0
  project: Apache TinkerPop
---

# TinkerPop Documentation Skill

This skill is about the **craft** of TinkerPop documentation: getting the voice,
audience, and shape of each kind of document right. The mechanics that keep the
build green (executable code blocks, AsciiDoc wiring) are covered in the
references and matter, but they serve the writing, not the other way around.

All project documentation lives under `docs/src/` and is **AsciiDoc, never
Markdown**. It is organized into several books, each with its own audience and
purpose. The four directories under `docs/src/dev/` are themselves separate
books, not one:

| Book | Path | Audience | Purpose |
|---|---|---|---|
| Reference | `docs/src/reference/` | Users | The complete, authoritative description of every feature and step |
| Recipes | `docs/src/recipes/` | Users | Reusable problem→solution patterns for real tasks |
| Tutorials | `docs/src/tutorials/` | Users (learning) | Story-driven walkthroughs of a focused topic |
| Upgrade | `docs/src/upgrade/` | Users (upgrading) | Announces what changed in a release and how to adapt |
| Provider | `docs/src/dev/provider/` | Graph providers | Implementation guidance, provider policies, and the canonical Gremlin semantics |
| IO | `docs/src/dev/io/` | Providers and advanced users | The serialization formats (GraphBinary, GraphSON, GraphML, Gryo) |
| Developer | `docs/src/dev/developer/` | Contributors | Contributing, development environment, committer and release process |
| Future | `docs/src/dev/future/` | Contributors | Design proposals for planned or possible changes |

The full per-book voice guide, with grounded examples from the real docs, is in
[references/books-and-voice.md](references/books-and-voice.md). Read it before
writing in any book you have not written in before. The short version of each
book's character:

- **Upgrade** announces. It tells a feature's story — what changed and why it
  matters — and defers exhaustive detail to the reference docs via `See:` rather
  than cataloging every edge case itself. A typical entry is short: a paragraph or
  two and one example. It frames past shortcomings positively and treats breaking
  changes as a priority.
- **Reference and Recipes** instruct. They are the focal point for correct
  usage and established patterns: complete, user-friendly, and formal.
- **Tutorials** teach through a story but otherwise follow the Reference/Recipes
  style.
- **Provider** documents internals in the Reference/Recipes style. The
  `gremlin-semantics.asciidoc` file is special and load-bearing (see below).
- **IO** specifies the serialization formats in the same formal, complete style
  as Provider.
- **Developer** docs are internal notes, instructions and standard processes for contributors.
- **Future** holds design proposals for changes under consideration.

## House style (applies to every book)

These rules hold across all documentation. Existing prose does not always obey
them; write to the standard, not to the weakest nearby example.

1. **No em-dash or semicolon to break a sentence.** Prefer two separate
   sentences, or find a transition word that lets the thought flow. (This is a
   prose rule. Semicolons inside code, such as the `;[]` output-suppression
   idiom, are unaffected.)
2. **Formal means no first or second person.** Do not address the reader as
   "you" or speak as "I" or "we." Write about the subject, not about the author
   or audience. "The `coalesce()`-step evaluates..." not "You can use
   `coalesce()` to...".
3. **Prefer flowing paragraphs over lists.** Use a bulleted or numbered list
   only when it itemizes something clearly quantifiable, such as the members of
   an enumeration or a fixed set of options, each needing description. Do not use
   a list for open-ended or subjective points (for example, "the benefits of a
   traversal pattern"). Those belong in prose.
4. **Avoid hype and marketing language.** Describe what something does and why it
   matters in plain, positive terms. Let the capability speak for itself.

## Examples must be real and runnable

TinkerPop documentation is unusual: most Gremlin examples are **executed at build
time** against a real graph, and their output is inlined automatically. A block
marked `[gremlin-groovy,modern]` is run against the modern toy graph and its
output is filled in for you. Never invent or hand-write the `==>` output of such
a block.

The two graphs available to executable blocks are `modern` and `existing`. Use
a bare `[gremlin-groovy]` block for setup that should run without a graph. When
an example is purely illustrative and must *not* be executed (a hypothetical, an
error case, console session text), use `[source,text]` or `[source,groovy]`
instead.

**Exception — upgrade documentation is always static.** Every example in
`docs/src/upgrade/` is a hand-written `[source,groovy]` or `[source,text]` block,
**never** an executable `[gremlin-groovy]` block, because an upgrade entry is a
snapshot of behavior at a fixed point in history (often an old-vs-new
comparison). This reverses the general rule above: in upgrade docs you **do**
write the `==>` output by hand. "Static" does not mean "no output" — a static
upgrade example should still show its results so the reader sees the payoff. If an
upgrade example is missing its output, the fix is to write the output in, **not**
to convert the block to executable. Do not be talked out of this by the rest of
the docs being executable, and do not assume the upgrade book contains executable
blocks — it does not.

The authoring details — graph choice, output suppression, `<1>` callouts, and
the multi-language story — are in
[references/executable-blocks.md](references/executable-blocks.md). AsciiDoc
structure (anchors, cross-references, section-title length limits for the TOC,
the `x.y.z` version placeholder, per-book `index.asciidoc` wiring, admonitions,
images) is in
[references/asciidoc-and-wiring.md](references/asciidoc-and-wiring.md).

## The semantics document is load-bearing

`docs/src/dev/provider/gremlin-semantics.asciidoc` is the canonical description
of Gremlin's semantics. **It must be updated whenever Gremlin steps, the Gremlin
grammar (`Gremlin.g4`), or the semantics code change in any way.** When adding to
it, follow the patterns already established there for how a concept or step is
documented rather than inventing a new structure. See
[references/books-and-voice.md](references/books-and-voice.md#provider-documentation)
for what those patterns are.

## Don't forget the changelog and upgrade docs

Behavioral and API changes usually need more than reference updates:

- **`CHANGELOG.asciidoc`** (repo root) gets a concise entry in the correct
  version section. One short clause, written for what the user will see on
  release day — the net user-facing change, from a usage perspective — not the
  implementation detail. Do not invent version numbers or release names.
  - The changelog is not a commit log. Not every commit earns an entry, and a
    feature built over many commits should have **one** entry, not one per
    increment. When extending an unreleased feature, fold the change into its
    existing entry rather than appending a new line; if a change has no
    user-visible effect, it needs no entry at all.
- **Upgrade docs** get an entry for user-visible features and especially for
  breaking changes to public APIs or Gremlin semantics. A breaking change
  without clear migration guidance in the upgrade docs is incomplete.
