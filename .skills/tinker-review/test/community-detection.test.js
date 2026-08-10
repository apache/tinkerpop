/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Pure Louvain community detection over plain node/edge lists — no server needed.
// The gremlin I/O wrapper (communityDetection) is exercised end-to-end against a
// live graph separately; here we pin the algorithm + roll-up + post-pass behavior.

import { test } from "node:test";
import assert from "node:assert/strict";

import { detectCommunities } from "../scripts/patterns/community-detection.js";

const fn = (id, file) => ({ id, label: "Function", name: id, filePath: file });
const edge = (from, to, conf = "INFERRED") => ({ from, to, conf });

// Two triangles joined by a single weak bridge — the textbook two-community graph.
function barbell() {
  const nodes = [
    fn("a", "auth.java"), fn("b", "auth.java"), fn("c", "auth.java"),
    fn("x", "io.java"), fn("y", "io.java"), fn("z", "io.java"),
  ];
  const edges = [
    edge("a", "b", "EXTRACTED"), edge("b", "c", "EXTRACTED"), edge("a", "c", "EXTRACTED"),
    edge("x", "y", "EXTRACTED"), edge("y", "z", "EXTRACTED"), edge("x", "z", "EXTRACTED"),
    edge("c", "x", "AMBIGUOUS"), // weak bridge between the two dense groups
  ];
  return { nodes, edges };
}

test("splits a barbell into two communities and rolls them up to files", () => {
  const { nodes, edges } = barbell();
  const r = detectCommunities(nodes, edges, {});
  assert.equal(r.communityCount, 2, "two dense triangles => two communities");
  assert.ok(r.modularity > 0.3, `expected clear modular structure, got ${r.modularity}`);
  const files = r.communities.map((c) => c.dominantFile).sort();
  assert.deepEqual(files, ["auth.java", "io.java"], "each community rolls up to its file");
});

test("a single dense triangle is one community", () => {
  const nodes = [fn("a", "f.java"), fn("b", "f.java"), fn("c", "f.java")];
  const edges = [edge("a", "b"), edge("b", "c"), edge("a", "c")];
  const r = detectCommunities(nodes, edges, {});
  assert.equal(r.communityCount, 1);
  assert.equal(r.communities[0].size, 3);
});

test("vertices with no in-subgraph edge are counted as isolated, not communities", () => {
  const nodes = [
    fn("a", "f.java"), fn("b", "f.java"), // connected pair
    fn("orphan1", "x.java"), fn("orphan2", "y.java"), // no edges
  ];
  const edges = [edge("a", "b")];
  const r = detectCommunities(nodes, edges, {});
  assert.equal(r.communityCount, 1, "only the connected pair is a community");
  assert.equal(r.isolatedCount, 2, "the two edgeless vertices are isolated");
  assert.equal(r.communities[0].size, 2);
});

test("edges to a filtered-out vertex are dropped, not invented", () => {
  const nodes = [fn("a", "f.java"), fn("b", "f.java")];
  // 'ext' is not in the node set (e.g. an external stub excluded upstream)
  const edges = [edge("a", "b"), edge("a", "ext"), edge("b", "ext")];
  const r = detectCommunities(nodes, edges, {});
  assert.equal(r.totalNodes, 2, "the missing endpoint never becomes a node");
  assert.equal(r.totalEdges, 1, "only the fully-resolved edge survives");
});

test("the assignments map covers every node so write-back can stamp them all", () => {
  const { nodes, edges } = barbell();
  const r = detectCommunities(nodes, edges, {});
  assert.equal(r.assignments.size, nodes.length, "one community assignment per vertex");
});

const testNode = (id, file, changed = false) => ({ id, label: "Test", name: id, filePath: file, changed });

test("an all-test partition reads as test scaffolding, not new functional groupings", () => {
  const nodes = [
    testNode("t1", "AbstractIT.java"), testNode("t2", "AbstractIT.java"), testNode("t3", "AbstractIT.java"),
    testNode("u1", "AuditLogIT.java"), testNode("u2", "AuditLogIT.java"), testNode("u3", "AuditLogIT.java"),
  ];
  const edges = [
    edge("t1", "t2"), edge("t2", "t3"), edge("t1", "t3"),
    edge("u1", "u2"), edge("u2", "u3"), edge("u1", "u3"),
    edge("t3", "u1"),
  ];
  const r = detectCommunities(nodes, edges, {});
  assert.ok(r.communities.every((c) => c.role.startsWith("test scaffolding")), "each community typed as test scaffolding");
  assert.match(r.interpretation.headline, /modularity/);
  assert.ok(
    r.interpretation.reading.some((line) => /test infrastructure/i.test(line)),
    "reading calls out that dense structure is test infrastructure",
  );
});

test("the reading reports the changed-vs-context share when no churn is supplied", () => {
  const nodes = [
    fn("a", "f.java"), fn("b", "f.java"), fn("c", "f.java"), // changed:false via fn()
  ];
  const edges = [edge("a", "b"), edge("b", "c"), edge("a", "c")];
  const r = detectCommunities(nodes, edges, {});
  assert.ok(
    r.interpretation.reading.some((line) => /0% of clustered vertices are actually changed/.test(line)),
    "0% changed when no vertex is marked changed",
  );
  assert.equal(r.communities[0].changedCount, 0);
});

test("churn drives the role's change clause with real line counts", () => {
  const nodes = [fn("a", "AbstractIT.java"), fn("b", "AbstractIT.java"), fn("c", "AbstractIT.java")];
  const edges = [edge("a", "b"), edge("b", "c"), edge("a", "c")];
  const churn = { "AbstractIT.java": { added: 4, removed: 120, deleted: false } };
  const r = detectCommunities(nodes, edges, { churn });
  assert.equal(r.communities[0].churn.mode, "reduced", "removals dominate => reduced");
  assert.match(r.communities[0].role, /reduced \(−120\/\+4\)/, "role carries the churn counts");
});

test("net-deletion PRs get a deletion caveat and a net-deletion reading", () => {
  const nodes = [fn("a", "AbstractIT.java"), fn("b", "AbstractIT.java")];
  const edges = [edge("a", "b")];
  const churn = {
    "AbstractIT.java": { added: 2, removed: 60, deleted: false },
    "KerberosAuthenticator.java": { added: 0, removed: 400, deleted: true },
    "KerberosIT.java": { added: 0, removed: 210, deleted: true },
  };
  const r = detectCommunities(nodes, edges, { churn });
  assert.equal(r.churn.deletedFiles, 2, "two files deleted outright");
  assert.ok(r.interpretation.reading.some((l) => /net-deletion overall/.test(l)), "reads as net-deletion");
  assert.ok(
    r.interpretation.reading.some((l) => /Deleted code has no vertices/.test(l)),
    "warns that deleted code is absent from the communities",
  );
});
