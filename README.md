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
# Apache TinkerPop

[![Maven Central](https://img.shields.io/maven-central/v/org.apache.tinkerpop/gremlin-driver?color=brightgreen)](https://mvnrepository.com/artifact/org.apache.tinkerpop/gremlin-driver)
[![NuGet](https://img.shields.io/nuget/v/Gremlin.Net?color=brightgreen)](https://www.nuget.org/packages/Gremlin.Net)
[![PyPI](https://img.shields.io/pypi/v/gremlinpython?color=brightgreen)](https://pypi.org/project/gremlinpython/)
[![npm](https://img.shields.io/npm/v/gremlin?color=brightgreen)](https://www.npmjs.com/package/gremlin)
[![Go.Dev](https://badge.fury.io/go/github.com%2Fapache%2Ftinkerpop%2Fgremlin-go%2Fv3.svg)](https://pkg.go.dev/github.com/apache/tinkerpop/gremlin-go/v3)

[![Codecov](https://codecov.io/gh/apache/tinkerpop/branch/master/graph/badge.svg?token=TojD2nR5Qd)](https://codecov.io/gh/apache/tinkerpop)

[![TinkerPop3](https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/tinkerpop3-splash.png)](https://tinkerpop.apache.org)

Apache TinkerPop™ is a graph computing framework for both graph databases (OLTP) and graph analytic systems (OLAP). It 
provides the Gremlin graph traversal language, drivers, and tools for working with property graphs across a wide variety
of underlying data systems.

## Project overview

TinkerPop defines a common interface and language (Gremlin) so that applications can work against many different graph 
systems without being locked into a single vendor. It includes a reference in‑memory graph database (TinkerGraph), 
Gremlin Server, language variants, and a rich collection of recipes and documentation.

Key resources:

- Website: https://tinkerpop.apache.org
- Documentation Index: https://tinkerpop.apache.org/docs/current/index.html
- Upgrade documentation: https://tinkerpop.apache.org/docs/current/upgrade/
- Reference documentation: https://tinkerpop.apache.org/docs/current/reference/
- Recipes: https://tinkerpop.apache.org/docs/current/recipes/
- Provider documentation and Gremlin Semantics: https://tinkerpop.apache.org/docs/current/dev/provider
- Developer documentation: https://tinkerpop.apache.org/docs/current/dev/developer
- IO Documentation: https://tinkerpop.apache.org/docs/current/dev/io
- Roadmap and proposals: https://tinkerpop.apache.org/docs/current/dev/future/
- Javadocs: https://tinkerpop.apache.org/javadocs/current/full/, [core javadoc](https://tinkerpop.apache.org/javadocs/current/core/)

## Building and Testing

TinkerPop uses [Maven](https://maven.apache.org/) and requires Java 11/17 for proper building and proper operations. To 
build, execute unit tests and package Gremlin Console/Server run:

```bash
mvn clean install
```

Please see the [Building on Windows](docs/src/dev/developer/development-environment.asciidoc#building-on-windows) 
section for Windows-specific build instructions.

The zip distributions can be found in the following directories:

1. `gremlin-server/target`
2. `gremlin-console/target`

Please see the CONTRIBUTING.md file for more detailed information and options for building, test running and developing 
TinkerPop.

## Get Started

Download [Gremlin Console](https://tinkerpop.apache.org/download.html) (compatible with Java 11/17) and unzip to a 
directory, then:

```bash
$ bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
plugin activated: tinkerpop.server
plugin activated: tinkerpop.utilities
plugin activated: tinkerpop.tinkergraph
gremlin> Gremlin.version()
==>3.8.0
gremlin> graph = TinkerFactory.createModern()
==>tinkergraph[vertices:6 edges:6]
gremlin> g = traversal().with(graph)
==>graphtraversalsource[tinkergraph[vertices:6 edges:6], standard]
gremlin> g.V().has('name','vadas').valueMap()
==>[name:[vadas], age:[27]]
```

From the Gremlin Console, you can connect to a TinkerGraph instance and run your first traversals. Refer to the 
[Getting Started](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/) for detailed walkthroughs and 
examples.

## Using TinkerPop

Common ways to use TinkerPop include:

- Embedding **TinkerGraph** in your application for development, testing, or lightweight graph workloads.
- Connecting to a **Gremlin‑enabled graph database** via drivers or [Gremlin Server](https://tinkerpop.apache.org/docs/current/reference/#gremlin-server).
- Running **Gremlin traversals** from the JVM, or via Gremlin Language Variants (Python, .NET, JavaScript, Go, etc.).
- Using the **Gremlin Console** for interactive exploration, debugging, and learning.

See the [Reference Documentation](https://tinkerpop.apache.org/docs/current/reference/) for supported features,
configuration options, and other details.

## Documentation

The full TinkerPop documentation is published on the project website and is also maintained in this repository under 
`docs/src/` as AsciiDoc “books.”

When changing or adding documentation, follow the existing AsciiDoc structure in `docs/src/**` and update the 
relevant `index.asciidoc` files so new content is included in the build.

## Contributing

Contributions to Apache TinkerPop are welcome. The [Developer Documentation](https://tinkerpop.apache.org/docs/current/dev/developer)
and contributing guide describe how to set up a development environment, run tests, and submit changes.

- Contribution guidelines: `CONTRIBUTING.md` in the repository root.
- Developer documentation: https://tinkerpop.apache.org/docs/current/dev/developer/

Before opening a pull request, please:

- Discuss larger changes on the appropriate Apache mailing list.
- Ensure tests pass locally and, where appropriate, add new tests and documentation.
- Update `CHANGELOG.asciidoc` and upgrade docs when behavior or public APIs change.

## Using AI and IDE assistants

If you use AI coding agents or IDE assistants when working on TinkerPop, please consult `AGENTS.md`. That file 
summarizes:

- Recommended build and test commands.
- Code style and testing conventions.
- “Do and don’t” guidance specific to automated tools.

`AGENTS.md` is a concise guide for tools and tool‑using contributors, while `CONTRIBUTING.md` and the 
[Developer Documentation](https://tinkerpop.apache.org/docs/current/dev/developer) remain the canonical sources for 
project policies and processes.

## License

Apache TinkerPop is an open source project of The Apache Software Foundation and is licensed under the Apache License, 
Version 2.0. See the `LICENSE` file in this repository for details.