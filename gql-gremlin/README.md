# gql-gremlin

`gql-gremlin` is an optional Apache TinkerPop module that provides **TinkerGQL**, a deliberate
minimal subset of [ISO GQL](https://www.iso.org/standard/76120.html) `MATCH` syntax, as a portable graph-pattern execution engine 
for the `match(String)` step.

Any TinkerPop graph provider can add TinkerGQL support to their graph in minutes. TinkerGraph
ships with it out of the box.

---

## What is TinkerGQL?

TinkerGQL is the named dialect implemented by `gql-gremlin`. It covers enough of the GQL `MATCH`
grammar to express multi-hop path patterns, inline property filters, parameterized queries, and
multi-pattern joins — the most common declarative graph query patterns — while deliberately
omitting features (aggregations, variable-length paths, `WHERE`/`RETURN` clauses) that are
better served by Gremlin steps.

Full syntax reference: [the-traversal.asciidoc §TinkerGQL](../docs/src/reference/the-traversal.asciidoc).

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
