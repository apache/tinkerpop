<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Tinkeradoc Extension

An [AsciidoctorJ](https://asciidoctor.org/) extension that renders the executable Gremlin code blocks in the
TinkerPop documentation. It is a build-time tool for producing the docs under `docs/src` — it is not part of the
TinkerPop distribution and is never published to Maven Central.

This is a standalone Maven project rather than a module of the root reactor. The root `pom.xml` consumes it as a
plugin dependency of `asciidoctor-maven-plugin` under the `asciidoc` profile, so it must already be installed in the
local repository before the docs can be generated.

## What It Does

The extension registers with AsciidoctorJ through the SPI (`GremlinDocsExtension`) and contributes two processors:

- **`GremlinTreeprocessor`** walks the parsed AsciiDoc AST, finds `[gremlin-groovy]` listing blocks, executes their
  contents against a long-lived Gremlin Console subprocess, and replaces each block with the captured console
  session. It also collects adjacent `[source,<lang>]` blocks for the supported languages (`groovy`, `java`,
  `csharp`, `javascript`, `python`, `go`) into a single tabbed widget via `TabbedHtmlBuilder`.
- **`GremlinPostprocessor`** cleans up the rendered HTML: it drops the empty comment spans CodeRay emits and
  substitutes the `x.y.z` version placeholder with the real TinkerPop version.

`GremlinConsole` manages the console subprocess, driving it over stdin/stdout with prompt-based boundary detection
and dismissing `Display stack trace?` prompts. `ConsoleRestartHandler` and `PluginDirectoryRestartHandler` restart
that subprocess when a book needs a different plugin set.

## Block Syntax

A `gremlin-groovy` block takes an optional graph name as its second positional attribute, which seeds `graph` and `g`
before the block runs:

    [gremlin-groovy,modern]
    ----
    g.V().has('name','marko').out('knows').values('name')
    ----

Recognized graph names are `modern`, `classic`, `crew`/`theCrew`, `grateful`, `sink`, and `theZoo`. Use `existing` to
continue against whatever state the previous block left behind instead of re-initializing.

## Configuration

The extension reads AsciiDoc document attributes, which the root `pom.xml` wires to Maven properties:

| Attribute | Maven property | Purpose |
|---|---|---|
| `gremlin-docs-console-home` | `gremlin.docs.console.home` | Path to the Gremlin Console distribution to launch |
| `gremlin-docs-hadoop-libs` | `gremlin.docs.hadoop.libs` | Hadoop libraries made available to the console |
| `gremlin-docs-dryrun` | `gremlin.docs.dryrun` | When `true`, blocks are processed but not executed |
| `gremlin-docs-plugins-exclude` | — | Console plugins to exclude, per document or per block |

Note that these attribute names and their backing Maven properties retain the `gremlin-docs` / `gremlin.docs` prefix
even though the code now lives in `org.apache.tinkerpop.tinkeradoc`.

## Building

    mvn clean install -f docs/tinkeradoc-extension/pom.xml

## Generating the Docs

Do not invoke this project directly to build documentation. `bin/process-docs.sh` is the entrypoint: it validates the
console and server distributions, installs the console plugins, starts a Gremlin Server and a Gephi mock, and then
runs Maven with the `asciidoc` profile so this extension executes. See the
[Documentation Environment](../src/dev/developer/development-environment.asciidoc) section of the developer docs for
the full workflow.
