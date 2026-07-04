# Playbook: Removal / Deprecation

## Context
The PR deletes code — a feature, a module, a dependency, or a deprecated API.
The change set is deletion-heavy (many removed files, net-negative diff). The
central review question is not "is the new code correct?" but **"does anything
left behind still depend on what was removed?"** A removal that leaves dangling
references, stale config, or orphaned docs is worse than no removal — it breaks
the build or misleads users.

Load this playbook when `listDeleted` returns entries (or the PR is dominated by
deletions). It runs in addition to `general.md` and any module playbook.

## Enrich
- `listDeleted` — the removed files and the symbol each likely defined
  (`Krb5Authenticator.java` -> `Krb5Authenticator`). The entry point; its results
  are the valid `--toPath` targets for `addReference`.
- Read `checks.removalRefs` (and `checks.removalRefs.externalCallers`) — Phase 1
  already grepped the surviving worktree for each removed code symbol and wrote a
  `references` edge per hit. Once you've classified each (see Interpret), confirm
  or downgrade it with `setEdgeConfidence`.
- `addReference` — record the non-code hits the automatic pass can't see, grepped
  off `listDeleted`: config/resources (`*.yaml`, `*.conf`, `*.properties`), build
  files (`pom.xml`, `*.gradle`), docs (`docs/src/**/*.asciidoc`, `CHANGELOG.asciidoc`),
  Docker/CI setup (`*.sh`, compose files).

## Inspect
None specific to removal — the review judgment here is structural (classifying
the `references` edges recorded in Enrich) and is handled in Interpret rather
than by reading changed source.

## Interpret
The `references` edges in `checks.removalRefs` (plus any you added) and
`checks.coverageGaps` on surviving code are the primary outputs.
Classify each surviving reference and state where it lives:
- Active code / build / config / live docs referencing a removed symbol —
  blocking; the build breaks or the feature is half-removed.
- Historical release notes (e.g. `docs/src/upgrade/release-3.x.asciidoc`)
  mentioning the removed symbol — expected and correct; note as verified-benign.
- No current upgrade/CHANGELOG line announcing the removal — a finding; users
  need to know the feature is gone.

## Escape
- if a removed symbol is still referenced by active source or build files —
  "Removal is incomplete: <symbol> still referenced in <file>; the build or
  feature is broken until this is resolved."
- if the removal drops a dependency or public API without an upgrade-doc entry —
  "User-facing removal needs an upgrade/CHANGELOG note."
