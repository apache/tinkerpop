# Pitfalls

Patterns earlier reviews of this repository actually found. Each describes how a gap arises, so it
stays useful after the specific case is fixed. Add a new pattern when a review finds one; do not
record one-off results here.

## Shading drops a dependency's sub-licenses

Each shade configuration excludes the dependencies' own files with
`DontIncludeResourceTransformer` entries such as `<resource>LICENSE</resource>`. That transformer
matches on the end of the path, so it drops every file whose name ends in `LICENSE` or `NOTICE`,
not just the top-level ones. `jackson-core` 2.15 and later bundles
FastDoubleParser (MIT, itself containing fast_float under MIT, fast_double_parser under Boost 1.0
and bigint under BSD-2) and Schubfach (MIT), and declares them in
`META-INF/FastDoubleParser-LICENSE`, `FastDoubleParser-ThirdParty-LICENSE` and
`Schubfach-LICENSE`. None of those reached `gremlin-shaded`, so every artifact built on it
bundled MIT and Boost code with no LICENSE entry.

Check: the inventory's "sub-licenses ... not carried into this jar" line, and the relocated
packages under `*/shaded/*`. The line compares text, so it goes quiet once the verbatim license is
packed under `licenses/`, whatever the file is called. An edited copy keeps it showing.

## Relocated sub-components inside Apache dependencies

An Apache-licensed jar can carry other code under another license. Groovy relocates ASM (BSD-3)
to `groovyjarjarasm` and ANTLR 4 to `groovyjarjarantlr4`. Ivy's `Scp` class derives from the
ganymed ssh library (BSD). Each needs a LICENSE entry, even though the outer jar needs none.
Nesting goes deeper than one level: `kryo-shaded` embeds ReflectASM (BSD-3), which embeds ASM,
and `jansi` 1.17 embeds `hawtjni-runtime`, whose SWT-derived classes are EPL 1.0. Neither carries
a license file of its own, so only the embedded pom or the relocated package name shows it.

Check: relocated packages (`*jarjar*`, `*/shaded/*`), the dependency's own LICENSE file, and its
NOTICE, which often points at the sub-license.

## Shade excludes that the LICENSE never followed

`gremlin-driver` excludes `slf4j-api` and `logback-classic` from its shade, yet its LICENSE listed
both and packed the SLF4J license. Only `logback-core` arrives, transitively. When a shade
`<exclude>` or a scope change removes a component, remove its entry and license file in the same
change.

Check: class counts per package in the shaded jar, not the pom's dependency list.

## Entries for things that are not in the distribution

- **devDependencies.** The root LICENSE listed chai, which `gremlin-javascript` uses only as an npm
  devDependency. No chai file is committed, and `node_modules` is not part of the source release.
- **Assets that never reach the zip.** The Server and Console LICENSE files listed the Foundation
  stylesheet and normalize.css and packed their licenses. Those files exist only in the docs
  stylesheets of the source tree, not in either zip.

Check: the inventory's source mapping and "loose web files" line. Name similarity proves nothing;
find the actual bytes.

## Language-variant packages drift from their packaging

- **Entries for devDependencies.** gremlin-javascript's package LICENSE listed chai, a
  devDependency, and pointed at a `licenses/chai` the package never had.
- **A files allowlist that drops NOTICE.** npm always packs LICENSE but packs NOTICE only when
  `files` allows it. gremlint's `files` is `lib/**/*`, so its npm package carries no NOTICE.
- **Source-only NuGet packages.** `Nullable` (MIT) puts its attribute classes into Gremlin.Net's
  netstandard2.0 DLL at compile time. `PrivateAssets` keeps it out of the dependency graph, not
  out of the binary.

Check: the inventory's `glv` section, and for .NET, the `contentFiles/` of each package reference.

## License files without entries

`licenses/bigint` and `licenses/ganymed-ssh` were packed with no LICENSE entry pointing to them.
A packed license file with no entry declares nothing. Either the entry is missing or the file is
stale. Find out which before deleting anything: in both of these cases the component was really
bundled, and the entry was what was missing.

## Upstream NOTICE text can be wrong

TinkerPop copied Jackson's NOTICE, which calls fast_float and fast_double_parser "Apache 2.0" and
points fast_double_parser at the fast_float repository. FastDoubleParser's own license files say
fast_float is MIT and fast_double_parser is Boost 1.0. When an upstream NOTICE and the license
files of the code it describes disagree, follow the license files and correct the propagated text.

## Decided: fast_double_parser is not declared

FastDoubleParser, inside `jackson-core`, derives part of its code from fast_double_parser, which
is under Boost 1.0. Boost waives its notice for compiled code, and TinkerPop ships that code only
as bytecode, so the maintainers chose not to declare it. The packed `licenses/fastdoubleparser`
and `licenses/fastdoubleparser-thirdparty` omit its paragraph and the Boost section on purpose.
The inventory's "sub-licenses ... not carried" line for `jackson-core` reports this every run and
is expected. It needs attention only if it names a file other than those two.

## Boilerplate NOTICEs are not owed

Every bundled Apache Commons jar (beanutils, collections, configuration2, io, lang3, text,
logging) and httpclient/httpcore carry boilerplate-only NOTICEs. They need nothing in TinkerPop's
NOTICE, and adding their lines goes against the guide. The ASF jars whose NOTICE does carry
content are Groovy (ANTLR 4 attribution), Ivy (Jayasoft grant, JSch, ganymed) and commons-codec
(the Beider-Morse port, Console only). A NOTICE section can mention code that is not itself
bundled, as Ivy's does for JSch. That is propagated text, not a LICENSE obligation.

## Versions drift

LICENSE lines carry versions, and nothing updates them on a dependency bump. Earlier reviews found
`logback 1.2.3` listed while 1.2.13 shipped, and `antlr4-runtime 4.9.1` listed on a branch that
moved to 4.13.2. Write a version as `group:artifact:version`, including in "bundled in" lines, so
the inventory checks it against what ships. A version written any other way needs a manual check.

## A clean merge is not a correct merge

Branches bundle different versions. A LICENSE fix written on `3.7-dev` that names Groovy 4.0.25
merged into `3.8-dev`, which ships 4.0.32, without a conflict and was wrong there. A hunk where
both branches had already edited the same line conflicted. Rebuild and rerun the inventory on
every branch the fix merges into.
