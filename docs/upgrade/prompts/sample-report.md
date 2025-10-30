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
### Apache TinkerPop 3.8.0 Risk Assessment

This report is only intended as a general guide for identifying possible upgrade issues between 3.7.4 and 3.8.0. There may be other problems to resolve. Be familiar with the [Upgrade Documentation](https://tinkerpop.apache.org/docs/3.8.0/upgrade/#_tinkerpop_3_8_0_2) and review your code carefully.

#### Scope of Review
- File analyzed: `docs/upgrade/java/src/main/java/org/apache/tinkerpop/example/cmdb/graph/TraversalBuilders.java`
- Goal: Identify usages of APIs, methods, or Gremlin syntax affected by breaking changes in TinkerPop 3.8.0 and outline concrete risks plus remediation guidance.

---

### Finding: none() and discard()
- Upgrade doc: https://tinkerpop.apache.org/docs/3.8.0/upgrade/#none-and-discard
- Summary of change:
    - Prior to 3.8.0, `none()` discarded traversers. In 3.8.0, that functionality is renamed to `discard()`, and `none()` is repurposed to `none(P)` as a quantifier complementing `any(P)` and `all(P)`.
- Occurrences in your code:
    - Line 90-92:
      ```java
      public static Function<GraphTraversalSource, GraphTraversal<?, Vertex>> noneAndDiscard() {
          return g -> g.V().none();
      }
      ```
- Risk:
    - In 3.8.0, `g.V().none()` no longer discards traversers and will be interpreted differently. This is a breaking change that will change behavior or fail.
- Recommendation:
    - Replace `none()` with `discard()` for the discard behavior:
      ```java
      return g -> g.V().discard();
      ```
- Confidence: High

---

### Finding: split() on Empty String
- Upgrade doc: https://tinkerpop.apache.org/docs/3.8.0/upgrade/#split-on-empty-string
- Summary of change:
    - `split("")` now splits a string into its characters (e.g., `"Hello" -> ["H","e","l","l","o"]`). In earlier versions it returned the entire string as a single element.
- Occurrences in your code:
    - Line 94-97 (method `firstAndLast`):
      ```java
      return g -> g.V().hasLabel("team").values("name").
              split("").map(__.union(__.limit(Scope.local,1), tail(Scope.local,1)).fold());
      ```
- Risk:
    - The traversal is currently splitting each team name into characters before the `map(...)`. This changes the cardinality and semantics of subsequent steps. If the intent was to extract first and last full team names, `split("")` in 3.8.0 will break that logic.
- Recommendation:
    - If the goal is to get the first and last team names (not characters), remove `split("")` entirely:
      ```java
      return g -> g.V().hasLabel("team").values("name")
              .map(__.union(__.limit(Scope.local, 1), tail(Scope.local, 1)).fold());
      ```
    - If you actually intended to operate on characters, adjust downstream logic accordingly (and also see the next finding about `limit(local)`/`tail(local)` return types).
- Confidence: High

---

### Finding: Consistent Output for range(), limit(), tail()
- Upgrade doc: https://tinkerpop.apache.org/docs/3.8.0/upgrade/#consistent-output-for-range-limit-tail
- Summary of change:
    - `range(local)`, `limit(local)`, and `tail(local)` now consistently return collections. Previously, if the result had a single element, the step returned the element itself (unwrapped).
- Occurrences in your code:
    - Line 94-97 (`firstAndLast`):
      ```java
      ... map(__.union(__.limit(Scope.local,1), tail(Scope.local,1)).fold());
      ```
- Risk:
    - In 3.7.x, `limit(Scope.local,1)` and `tail(Scope.local,1)` often yielded single elements which then produced a flat two-element list after `union(...).fold()` like `[first, last]`.
    - In 3.8.0, both steps yield collections even for one element, turning the result to `[[first], [last]]` before the final `fold()`, thus changing the output structure and any downstream consumers.
- Recommendation:
    - If you want the previous flat structure, explicitly `unfold()` each collection before folding:
      ```java
      .map(__.union(
              __.limit(Scope.local, 1).unfold(),
              tail(Scope.local, 1).unfold()
          ).fold())
      ```
    - Alternatively, use `head(local)` once available or use `range(local, 0, 1).unfold()` as a uniform pattern.
- Confidence: High

---

### Finding: repeat() Step Global Children Semantics Change
- Upgrade doc: https://tinkerpop.apache.org/docs/3.8.0/upgrade/#repeat-step-global-children-semantics-change
- Summary of change:
    - `repeat()` now consistently behaves with global-child semantics. Previously it behaved like a hybrid between local and global, which could affect ordering and interactions with barriers/side-effects inside `repeat()`.
- Occurrences in your code:
    - Lines 45-52 (`incidentBlastRadius`):
      ```java
      .emit()
      .repeat(
          __.union(
              __.out("dependsOn"),
              __.out("deployedAs").in("deploymentOf")
          )
      )
      .times(d)
      .path().by(__.valueMap(true));
      ```
    - Lines 61-63 (`serviceDependencies`):
      ```java
      .emit()
      .repeat(__.out("dependsOn")).times(d)
      .valueMap(true);
      ```
- Risk:
    - While these `repeat()` bodies don’t include explicit barriers like `order()` or side-effect steps, the traversal may produce a different ordering of paths or intermediate traversers in 3.8.0 compared to 3.7.x due to the global semantics change, especially when combined with `emit().times(d)`.
    - If any downstream logic relies on specific ordering of `path()` elements or on determinism in the emitted iterations, results could differ.
- Recommendation:
    - If you depended on earlier local-like iteration ordering, wrap the repeat in `local()` to emulate the old behavior:
      ```java
      .local(repeat(...).times(d))
      ```
    - Prefer making downstream consumers robust to ordering, or add an explicit `order()` where ordering is required.
- Confidence: Medium

---

### Finding: New Default DateTime Type (possible impact)
- Upgrade doc: https://tinkerpop.apache.org/docs/3.8.0/upgrade/#new-default-datetime-type
- Summary of change:
    - The default date type in Gremlin is now `java.time.OffsetDateTime` instead of `java.util.Date`. While `Date` is still accepted as input, server-side manipulations may yield `OffsetDateTime`.
- Occurrences in your code:
    - Lines 76-78 (`deploymentTimeline`):
      ```java
      .order().by("openedAt", Order.desc)
      .valueMap(true);
      ```
- Risk:
    - If your backend graph contains `Date` properties for `openedAt` and your application assumes `Date` objects are returned, the switch to `OffsetDateTime` in some manipulation contexts can surface type differences when data is consumed from `valueMap(true)`. You are not calling the date conversion steps (`asDate()`, `dateAdd()`, `dateDiff()`), so impact is typically minimal, but cross-version data or server-side transformations could still alter types.
- Recommendation:
    - Verify the runtime type of `openedAt` in integration tests against 3.8.0 and adjust DTOs/serialization if needed to accept `OffsetDateTime`.
- Confidence: Low to Medium

---