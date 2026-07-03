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
1. Run `listDeleted` to get the removed files and the symbol each likely defined
   (e.g. `Krb5Authenticator.java` -> `Krb5Authenticator`). Deleted files are
   already in the graph as `File { deleted: true }` markers.

2. **Phase 1 already found the code-symbol references.** For every deleted *code*
   file it grepped the surviving worktree for that class/method name and wrote a
   `references` edge per hit — read them from `checks.removalRefs` (and
   `checks.removalRefs.externalCallers` for changed code still calling a removed
   name). Your job on these is judgment, not discovery: classify each (see
   Interpret). They are `INFERRED`; confirm or downgrade with `setEdgeConfidence`.

3. **Grep for what the automatic pass skips** — the non-code supporting cast that
   removals commonly leave behind, keyed off `listDeleted`:
   - config/resources (`*.yaml`, `*.conf`, `*.properties`), ports, feature flags
   - build files (`pom.xml`, `*.gradle`) — was the dependency actually dropped?
   - docs (`docs/src/**/*.asciidoc`) and `CHANGELOG.asciidoc`
   - Docker/CI setup (compose files, `*.sh`)

   Record any surviving hit with `addReference --fromPath <file> --toPath
   <deletedFile> --symbol <name> --location <where>` — the escape hatch for the
   cases the code-symbol pass cannot see.

## Interpret
Read the structural signals from evidence.json (schema in
[references/interfaces.md](../references/interfaces.md)); the `references` edges
in checks.removalRefs (plus any you added by hand) and checks.coverageGaps on
any surviving code are the primary structural outputs here.

Not every surviving reference is a defect — classify each:
- **Active code / build / config / live docs** referencing a removed symbol is a
  **blocking finding**: the build breaks or the feature is half-removed.
- **Historical release notes** (e.g. `docs/src/upgrade/release-3.x.asciidoc`)
  mentioning the removed symbol are **expected and correct** — they record when
  the feature existed. Note them as verified-benign, not as a problem.
- A **current** upgrade/CHANGELOG entry should *gain* a line announcing the
  removal. Its absence is a finding (users need to know the feature is gone).

Weight findings by where the reference lives, and say so explicitly in the
report so the reviewer isn't left guessing whether a hit matters.

## Escape
- if a removed symbol is still referenced by active source or build files —
  "Removal is incomplete: <symbol> still referenced in <file>; the build or
  feature is broken until this is resolved."
- if the removal drops a dependency or public API without an upgrade-doc entry —
  "User-facing removal needs an upgrade/CHANGELOG note."
