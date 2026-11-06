# tinker-dev — Design

How this skill is structured and how to change it. The development guidance
itself — build recipes, the validation matrix, conventions, beads rules — is the
skill's *content* and lives in `SKILL.md` and `references/`. This document is only
the shape of the artifact and where new content goes.

## Structure

A guidance skill, not a program:

| Path | Role |
|------|------|
| `SKILL.md` | the operational index — the guidance a reader needs up front |
| `references/*.md` | deep, task-specific material, loaded on demand |
| `scripts/check-env.sh` | environment preflight |

It carries no logic of its own; its effect is what a reader does after reading it.

## How to change it — where content goes

Two rules govern the shape, so every change is a routing decision:

- **Single source of truth — defer, don't duplicate.** General agent rules live
  in the root `AGENTS.md`; canonical facts live in the repo (`CONTRIBUTING.md`,
  `docs/src/`, `bin/asf-license-header.txt`). New guidance points at its source
  and repeats only the TinkerPop-specific thing that is easy to miss. A fact
  copied here drifts from its source — so it isn't.
- **Progressive disclosure.** `SKILL.md` stays a lean index; anything deep or
  task-specific becomes a `references/` file linked from it, never inlined.

Applying them:

- A **general** agent rule → root `AGENTS.md`, not here.
- A **TinkerPop-specific, easy-to-miss** convention → a bullet in `SKILL.md`,
  deferring to the canonical file.
- A **new validation rule** → the Definition-of-Done matrix in `SKILL.md`.
- A **new deep topic** → a `references/` file, linked from `SKILL.md`.
