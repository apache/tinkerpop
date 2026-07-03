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

2. Run `listExternalRefs`. Any external callee whose `matchesDeletedSymbol` is
   true is a dangling reference the changed code itself still makes — a
   smoking gun visible in the graph with no grep needed. Record each with
   `addReference` and treat as a finding.

3. **Grep the surviving worktree** (`/tmp/pr-review-<pr>/src`) for every removed
   symbol, excluding the deleted files themselves. Search code *and* the
   supporting cast that removals commonly miss:
   - source (`*.java`, GLV sources) and build files (`pom.xml`, `*.gradle`)
   - config/resources (`*.yaml`, `*.conf`, `*.properties`)
   - docs (`docs/src/**/*.asciidoc`) and `CHANGELOG.asciidoc`
   - Docker/CI setup (compose files, `*.sh`)

   For each surviving hit, record it with `addReference --fromPath <file>
   --toPath <deletedFile> --symbol <name> --location <where>`.

4. Confirm the removal is complete on the *other* side too: was the dependency
   dropped from `pom.xml`? Were the config keys, ports, and doc sections that
   described the feature removed, not just the classes?

## Checks
- coverage_gaps(pr.tests(), pr.modified())
- (references edges created above are the primary structural output)

## Interpret
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
