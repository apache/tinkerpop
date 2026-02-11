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
# Contributing to Apache TinkerPop

Thank you for your interest in contributing to Apache TinkerPop™! Contributions of code, tests, documentation, examples, 
and ideas are all welcome.

Contributions via GitHub pull requests are gladly accepted from their original author. By submitting any copyrighted 
material via pull request, email, or other means, you agree to license the material under the project's open source 
license and warrant that you have the legal authority to do so.

Please see the "Developer Documentation" for more information on 
[contributing](http://tinkerpop.apache.org/docs/current/dev/developer/#_contributing) to TinkerPop.

## Ways to contribute

You can help TinkerPop in many ways, including: [tinkerpop.apache](https://tinkerpop.apache.org/docs/current/dev/developer/)

- Reporting bugs and proposing improvements.
- Contributing code changes and tests.
- Writing and improving documentation and examples.
- Helping users on mailing lists, issue trackers, and forums.
- Sharing recipes, tutorials, and best practices.

If you are new to the project, unresolved issues marked as “trivial” in [JIRA](https://issues.apache.org/jira/browse/TINKERPOP)
are a good place to start.

## Getting started

. Fork the [Apache TinkerPop repository](https://github.com/apache/tinkerpop) on GitHub and clone your fork locally.
. Set up a development environment following the [Developer Documentation](https://tinkerpop.apache.org/docs/current/dev/developer/)
. For non‑trivial work, tie your changes to an existing JIRA issue or create a new one if needed.

## Contribution process

The general workflow for code or documentation changes is: [tinkerpop.apache](https://tinkerpop.apache.org/docs/current/dev/developer/)

1. **Discuss (recommended)**
    - For larger or potentially breaking changes, start a discussion on the TinkerPop dev mailing list and/or in JIRA.

2. **Implement**
    - Make your changes in a feature branch in your fork.
    - Include tests for code changes whenever possible.
    - Update documentation, recipes, and upgrade notes when behavior or APIs change.

3. **Build and test**
    - Run the relevant Maven builds and tests as described in the Developer Documentation (for example, `mvn clean install`).
    - For significant contributions, consider running the broader test and documentation build described in the development‑environment guide.

4. **Prepare your pull request**
    - Reference the associated JIRA issue in your commit message/pull request description.
    - Keep pull requests focused and reasonably small where possible.
    - Update `CHANGELOG.asciidoc` and the upgrade documentation if your change affects user‑visible behavior or public API.

5. **Submit and respond to review**
    - Open a GitHub pull request against the appropriate branch.
    - Automated builds and tests will run via [GitHub Actions](https://github.com/apache/tinkerpop/actions); please address any failures.
    - Committers and other contributors may request changes; you can add more commits to the same branch to update the pull request.
    - Please be patient; we will do our best to move your contribution forward.

## Documentation contributions

TinkerPop documentation is maintained primarily as AsciiDoc under `docs/src/**` and published per release. 

When changing documentation:

- Edit the appropriate AsciiDoc files in `docs/src/`.
- Use the existing structure (books and index files) and update `index.asciidoc` entries so new content is included.
- For project documentation, submit changes via pull request, just like code.

The Developer Documentation includes detailed information on how to build and preview the documentation locally.

The TinkerPop website is also maintained in the repository under `docs/site` as HTML files. Contributions to that are
also welcome via pull request. 

## Using AI and IDE assistants

If you use AI coding agents or IDE assistants when working on TinkerPop, please also consult `AGENTS.md`. That file 
summarizes:

- Recommended build and test commands.
- Coding and testing conventions.
- “Do and don’t” guidance specific to automated tools.

Please consider [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html) when making 
contributions with these tools.

## Community and communication

TinkerPop is an Apache Software Foundation project and follows [ASF community guidelines](https://community.apache.org/contributors/).

For more detail on:

- [Mailing lists](https://lists.apache.org/list.html?dev@tinkerpop.apache.org) and project communication.
- Roles and responsibilities (contributors, committers, PMC).
- Release and voting processes.

please see the Developer Documentation’s contributing and governance sections:  
https://tinkerpop.apache.org/docs/current/dev/developer/