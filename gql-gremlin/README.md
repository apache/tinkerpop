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

# gql-gremlin

`gql-gremlin` is an optional Apache TinkerPop module that provides **TinkerGQL**, a deliberate
minimal subset of [ISO GQL](https://www.iso.org/standard/76120.html) `MATCH` syntax, as a portable graph-pattern execution engine 
for the `match(String)` step.

Any TinkerPop graph provider can add TinkerGQL support to their graph and provide deeper 
optimizations than those provided by TinkerPop's default implementation. TinkerGraph ships with 
the default out of the box.

---

## What is TinkerGQL?

TinkerGQL is the named dialect implemented by `gql-gremlin`. It covers enough of the GQL `MATCH`
grammar to express multi-hop path patterns, inline property filters, parameterized queries, and
multi-pattern joins.

Full syntax reference: [the-traversal.asciidoc TinkerGQL](../docs/src/reference/the-traversal.asciidoc).

---

## Quick start (using TinkerGraph)

TinkerGraph registers TinkerGQL automatically. No configuration is needed:

```groovy
graph = TinkerFactory.createModern()
g = graph.traversal()

g.match("MATCH (a:person)-[:knows]->(b:person)").
  select("a", "b").by("name")
// ==>[a:marko, b:vadas]
// ==>[a:marko, b:josh]
// ...
```

---

## Maven dependency

```xml
<dependency>
    <groupId>org.apache.tinkerpop</groupId>
    <artifactId>gql-gremlin</artifactId>
    <version>x.y.z</version>
</dependency>
```
