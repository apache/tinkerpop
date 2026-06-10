<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Documentation

## Format and Location

TinkerPop documentation is AsciiDoc-based and lives under `docs/src/`:

```
docs/src/
├── reference/          Main reference documentation
├── dev/
│   ├── developer/      Developer guides (environment, contributing, releases)
│   ├── provider/       Graph provider documentation, Gremlin semantics
│   ├── io/             IO and serialization formats
│   └── future/         Future plans
├── recipes/            Gremlin recipes and patterns
├── tutorials/          Getting started tutorials
└── upgrade/            Version upgrade guides
```

Do not use Markdown in the main docs tree. Use AsciiDoc.

## Adding or Updating Documentation

1. Place new content in the appropriate book (reference, dev, recipes, etc.).
2. Update the relevant `index.asciidoc` so the new content is included in the build.
3. Follow existing patterns for section structure and formatting.
