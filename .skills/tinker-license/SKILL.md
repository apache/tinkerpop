---
name: tinker-license
description: >
  Review Apache TinkerPop's LICENSE and NOTICE files against what its source and binary
  distributions actually bundle, following the ASF licensing how-to and the Dependencies
  section of the developer docs. Use when asked to check, audit or fix LICENSE/NOTICE, during
  the release pre-flight check, or after a change to a pom.xml, a shade configuration or a
  vendored file.
license: Apache-2.0
compatibility: Requires git, Maven, Python 3 and the JDK the branch's tinker-dev skill names. Network access is optional.
metadata:
  version: 1.0.0
  project: Apache TinkerPop
---

# TinkerPop License Review Skill

TinkerPop ships one source distribution and several binary artifacts, and each carries its own
LICENSE and NOTICE. The governing rule is the ASF's: **LICENSE and NOTICE must exactly represent
the contents of the distribution they reside in.** Something bundled but not declared is a
compliance gap. Something declared but not bundled is also wrong, because the guide says "Do not
add to LICENSE and NOTICE dependencies which are not in the distribution." The first kind can
block a release vote. The second rarely does, but still gets fixed.

The rules come from two canonical sources. Read them rather than relying on memory:

- the Dependencies section of `docs/src/dev/developer/for-committers.asciidoc`, which the
  pre-flight check in `release.asciidoc` points to;
- the ASF guide at https://infra.apache.org/licensing-howto.html.

[references/asf-rules.md](references/asf-rules.md) condenses both into decision rules, and
[references/pitfalls.md](references/pitfalls.md) records what earlier reviews of this repository
actually found. Read the pitfalls before starting; most real gaps are one of them.

**Report, then wait.** A review ends with findings. Change files only when the operator asks, and
never commit or push.

## 1. Fix the scope

Settle two things with the operator before building anything:

- **Which branch.** Fixes land on the oldest release branch that has the problem and merge
  forward (see the `tinker-merge` skill). Review that branch first.
- **What changed.** For a release pre-flight check, list what changed since the last release
  tag on the branch:

  ```bash
  git -P log --no-merges --format='%h %s' <last-tag>..HEAD -- '*pom.xml' '*package.json' \
      '*pyproject.toml' '*go.mod' '*.csproj' '*/static/LICENSE' '*/static/NOTICE' LICENSE NOTICE \
      licenses '*/static/licenses/*'
  ```

  A full audit ignores this list and checks everything bundled. Either way, read each listed
  commit: a version bump changes a LICENSE line, a new dependency may need an entry, and a shade
  change can add or drop whole components.

## 2. Run the inventory

```bash
python3 .skills/tinker-license/scripts/license-inventory.py [--strict] [--skip-build] [--only <keys>]
```

Check first that the JDK on the path suits the branch (see the `tinker-dev` skill). The script
builds what it reviews, so do not build separately and do not point it at another run's output: a
`target/` directory can hold artifacts from an earlier version or tree. It runs
`mvn clean package -DskipTests` for the driver, console and server and the modules they depend
on. Every artifact below is produced by the shade and assembly plugins in the `package` phase,
so later phases add nothing to review. The build takes a few minutes and logs to
`target/license-inventory-build.log`. It checks the version the root `pom.xml` declares, SNAPSHOT
or release.

| Key | What it reviews | LICENSE/NOTICE source |
|---|---|---|
| `shaded` | `gremlin-shaded/target/gremlin-shaded-<v>.jar` | `gremlin-shaded/src/main/static` |
| `driver` | `gremlin-driver/target/gremlin-driver-<v>-shaded.jar` | `gremlin-driver/src/main/static` |
| `console-uber` | `gremlin-console/target/gremlin-console-<v>-uber.jar` | `gremlin-console/src/main/static` |
| `server-zip` | `gremlin-server/target/apache-tinkerpop-gremlin-server-<v>-distribution.zip` | `gremlin-server/src/main/static` |
| `console-zip` | `gremlin-console/target/apache-tinkerpop-gremlin-console-<v>-distribution.zip` | `gremlin-console/src/main/static` |
| `source` | the git tree | root `LICENSE`, `NOTICE`, `licenses/` |
| `glv` | `gremlin-python`, `gremlin-javascript`, `gremlin-dotnet`, `gremlin-go`, `gremlint` | each package's own `LICENSE`, `NOTICE` and any `licenses/` |

The shaded jars merge dependency classes into one jar and replace the dependencies' own
LICENSE/NOTICE files with TinkerPop's. The zips ship dependency jars intact, but the zip's
top-level LICENSE must still account for each one. The language-variant packages declare their
dependencies rather than bundling them, so each LICENSE should be the plain Apache license and
each NOTICE the boilerplate. The script cannot see one case there: a .NET source-only package,
one whose files sit under `contentFiles/` in the NuGet cache, compiles its source into
Gremlin.Net's DLL even with `PrivateAssets` set to `all`. Check the `PackageReference` entries
in `Gremlin.Net.csproj` for these, and decide each under step 3.

`--only` narrows the checks, not the build. `--only source` and `--only glv` skip the build,
because neither needs one. `--skip-build` reuses the artifacts already in `target/` and prints
when each was built. Pass it only when the operator asks, or to re-run checks right after a fresh
build of an unchanged tree, and say in the report that the artifacts were not rebuilt.

| Exit | Meaning | Do |
|---|---|---|
| `0` | The checks ran, and passed if `--strict` was given | Go to step 3 |
| `1` | A check failed under `--strict`, the build failed, or the script was run outside the repository root | For a check, go to step 3. Otherwise report the reason and stop. |
| `2` | `--skip-build` found artifacts missing from `target/`, or the arguments were invalid; nothing was checked | For missing artifacts, ask the operator whether to build. If they agree, run again without `--skip-build`. |

The last line counts mechanical failures and leads. A lead count above zero always needs step 3.
The script reports facts and decides nothing. For each binary artifact it:

- compares the packed LICENSE and NOTICE with their sources;
- cross-checks every `licenses/<name>` the LICENSE refers to against the files actually packed,
  both ways;
- checks every `group:artifact:version` written anywhere in the LICENSE, including "bundled in"
  lines, against what is bundled at that version, and lists the entries with no coordinates for
  you to confirm by hand;
- lists bundled Maven artifacts with the license their pom declares, following parent poms in
  `~/.m2`, and flags any non-Apache or unknown license whose artifactId LICENSE never names in
  full. A jar that embeds other artifacts' poms is judged per pom;
- for a zip, lists each bundled jar's NOTICE as none, boilerplate only, or its non-boilerplate
  lines;
- lists relocated packages (`*/shaded/*`, `*jarjar*`), which are where sub-components hide;
- for a shaded jar, lists license files inside each original dependency jar whose text the
  shaded jar no longer carries under any name.

For the source tree it maps each root LICENSE entry to the tracked files that look like it, and
says when the name is only an npm dependency. For each language-variant package it checks that the
published package carries LICENSE and NOTICE (through `package.json` and `.npmignore`, `MANIFEST.in`
or the `.csproj`), that every `licenses/` reference resolves inside the package, and it lists any LICENSE
text past the Apache license, any non-boilerplate NOTICE line and any non-ASF copyright line in
the package's tracked files. `--strict` exits 1 on any mechanical failure: the first three checks
above, a language-variant package missing LICENSE or NOTICE or a reference with no file, or an
artifact the build did not produce. Every flag is a lead, not a verdict.

## 3. Decide each component

Work through every bundled component the inventory lists, plus everything it flags. For each,
find its license from the jar or its pom, never from memory, and apply
[references/asf-rules.md](references/asf-rules.md):

1. **Is it in the distribution?** Confirm it from classes, jar names or files, not from the
   dependency tree. A shade `<exclude>`, a `provided` or `test` scope, an npm devDependency, or
   a file the assembly never copies all mean it is not bundled.
2. **Is the license allowed?** Category A (Apache, MIT, BSD, ISC, Boost and the like) may be
   bundled. Category B (EPL, for example) may be bundled in binary form only. Category X may not
   be bundled at all. Stop and tell the operator about any Category X or unknown license.
3. **Does LICENSE need an entry?** Apache-licensed code does not, as long as nothing inside it is
   under another license. Anything else bundled needs an entry in the existing pattern, with its
   version, and a license file under `licenses/` that the entry points to. Before adding one for
   an unusual license, read its conditions for the form TinkerPop ships. Some waive their own
   notice requirement for compiled code: Boost 1.0 does for "machine-executable object code
   generated by a source language processor", which covers javac output. When the waiver applies
   to everything TinkerPop ships of that component, no entry is needed. MIT, BSD and EPL have no
   such waiver. See [references/asf-rules.md](references/asf-rules.md).
4. **Does NOTICE need a change?** Only a bundled Apache-licensed dependency's own NOTICE
   propagates. Copy only what goes beyond the boilerplate. A non-Apache dependency's copyright
   lines stay in its license file.
5. **What is inside it?** Look at the relocated packages and the dependency's own license and
   NOTICE files. A sub-component under another license needs its own entry even when the outer
   dependency is Apache-licensed.

Check the reverse direction too. Every LICENSE entry, every packed license file and every NOTICE
section must correspond to something bundled, at the stated version.

## 4. Report

Report one table per artifact with the problems first, in this order:

1. **Bundled but not declared.** The blocking kind.
2. **Declared but not bundled.** Stale entries and license files.
3. **Wrong details.** A version, URL or license name that does not match what ships.
4. **Confirmed correct.** One line per component category, so the operator sees the coverage.

For each problem give the artifact, the evidence (class path, jar name, pom license or file
content), the rule it breaks, and the fix. Say what you did not check, such as code a
dependency embeds without saying so in its license files, and whether each problem is new since
the last release or was already shipped.

## 5. When asked to fix

- **Fix on the oldest affected branch.** Build the patch against `<remote>/<branch>`, not a local
  branch that may be behind, and hand it over as a patch or uncommitted changes.
- **Copy license texts verbatim** from the dependency's jar or source release. Never retype or
  reconstruct one.
- **Keep entries compact.** When an entry's only detail is its license file, put
  `- for details, see licenses/<name>` on the entry's own line. Use indented `- ` lines only when
  there is more to say, such as where it is bundled or what it is shaded to.
- **Change the shade configuration with the files.** A new `licenses/` file in a shaded module
  needs an `IncludeResourceTransformer`, and the copy that arrives from `gremlin-shaded` needs a
  `DontIncludeResourceTransformer` in `gremlin-driver` and `gremlin-console`. A zip picks up its
  module's whole `src/main/static/licenses` directory through the assembly descriptor.
- **Run the inventory with `--strict`**, without `--skip-build`, until it passes. Each such run
  rebuilds, so it always checks the edited files, and check that RAT still
  reports no unapproved files.
- **Check the merge forward** with the `tinker-merge` skill, passing
  `--test-command 'python3 .skills/tinker-license/scripts/license-inventory.py --strict'` so
  every branch in the chain is rebuilt and re-inventoried after its merge. Versions differ
  between branches, so a hunk can merge cleanly and still name the wrong version there. Tell the
  operator about every such line.
