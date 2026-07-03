# tinker-doc — Design

How this skill is structured and how to change it. The documentation guidance
itself — per-book voice, house style, the executable-block rules — is the skill's
*content* and lives in `SKILL.md` and `references/`. This document is only the
shape of the artifact and where new content goes.

## Structure

A guidance skill, not a program:

| Path | Role |
|------|------|
| `SKILL.md` | the book map, house style, and the load-bearing rules up front |
| `references/*.md` | the voice guide and authoring mechanics, loaded on demand |

## How to change it — where content goes

Two rules govern the shape, so every change is a routing decision:

- **Single source of truth — defer, don't duplicate.** Guidance that overlaps
  development (build/validation, changelog discipline) defers to **tinker-dev**
  rather than being restated. Canonical documentation lives under `docs/src/`.
- **Progressive disclosure.** `SKILL.md` holds the rule; the details live in a
  `references/` file linked from it.

Applying them:

- A **new book or audience** → a row in the book map in `SKILL.md`, plus a voice
  entry in `references/books-and-voice.md`.
- A **new authoring mechanic** → `references/executable-blocks.md` or
  `references/asciidoc-and-wiring.md`; keep `SKILL.md` to the rule, not the how.
- A **new house-style rule** → the House style list in `SKILL.md`.
