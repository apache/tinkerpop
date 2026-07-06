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

import Graph from "graphology";
import louvain from "graphology-communities-louvain";

/**
 * Louvain modularity community detection over the PR's *code* subgraph.
 *
 * Complements `clusterAnalysis` (connected components), which stays as the coarse
 * "disjoint islands" guard. Connected components asks "is any changed file totally
 * unreachable from the rest?"; that signal saturates at one component in a
 * well-connected codebase. Community detection asks the finer question — "how many
 * densely-tied themes does this change actually contain?" — and keeps giving signal
 * even when everything is technically reachable.
 *
 * We run over Function/Type/File/Test vertices tied by code edges only. Discussion,
 * Step, Doc and GrammarRule vertices are excluded on purpose: the PR vertex's
 * `modifies` star and the `implements_step` hub would connect everything through a
 * single node and collapse the modularity structure we are trying to surface.
 *
 * Louvain can occasionally emit an internally-disconnected community; a
 * well-connectedness post-pass splits any such community back into its connected
 * pieces (the one guarantee Leiden would give us natively, recovered cheaply at
 * PR scale).
 *
 * Communities are detected at Function/Type grain and rolled up to the files they
 * touch for reporting. Each vertex is stamped with a `community` property so a
 * reviewer can query the partition directly (`g.V().has("community", n)`).
 *
 * @param {object} g - gremlin-js GraphTraversalSource (standard OLTP source)
 * @param {object} params
 * @param {number}  [params.resolution]      Louvain resolution (default: 1)
 * @param {boolean} [params.writeBack]       Stamp `community` onto vertices (default: true)
 * @param {number}  [params.minCommunitySize] Report communities at or above this size (default: 2)
 * @param {object}  [params.churn]           Per-file `{ [path]: { added, removed, deleted } }`
 *   (from the diff) so communities can be described by *how* they changed — reduction
 *   vs. expansion — and the report can note that deleted code is absent from the graph.
 * @returns {Promise<CommunityResult>}
 */

/** Confidence → edge weight. Stronger evidence pulls harder. */
const WEIGHT = { EXTRACTED: 3, INFERRED: 2, AMBIGUOUS: 1 };

const NODE_LABELS = ["Function", "Type", "File", "Test"];
const EDGE_LABELS = ["calls", "defines", "declares", "extends", "implements", "overrides", "tests"];

/**
 * @typedef {Object} Community
 * @property {number}   id            0-based community index (largest first)
 * @property {number}   size          member vertex count
 * @property {string[]} files         files this community touches (rolled up), largest share first
 * @property {string}   dominantFile  the file contributing the most members
 * @property {Object}   labelCounts   member count per vertex label (Function/Type/File/Test)
 * @property {number}   changedCount  members with changed:true
 * @property {string}   dominantLabel most common vertex label
 * @property {number}   testShare     fraction of members living in test files — drives the role
 * @property {?Object}  churn         `{ added, removed, mode }` for the community's files, when churn is supplied
 * @property {string}   role          human descriptor, e.g. "test scaffolding, reduced (−240/+12), spanning 3 files"
 *
 * @typedef {Object} CommunityResult
 * @property {number}      communityCount  communities with size >= minCommunitySize
 * @property {number}      isolatedCount   vertices with no in-subgraph code edge
 * @property {number}      modularity      Louvain modularity of the partition (0..1)
 * @property {number}      splitCount      communities split by the well-connectedness pass
 * @property {?Object}     churn           `{ added, removed, deletedFiles }` across all changed files, when supplied
 * @property {Community[]} communities     communities >= minCommunitySize, largest first
 * @property {number}      totalNodes      vertices fed to Louvain
 * @property {number}      totalEdges      undirected edges after weight-merging
 */
export async function communityDetection(g, params = {}) {
  const { nodes, edges } = await extractCodeSubgraph(g);
  const result = detectCommunities(nodes, edges, params);

  if (params.writeBack !== false && result.assignments) {
    await writeBackCommunities(g, result.assignments);
  }
  delete result.assignments;
  return result;
}

/**
 * Pull the code subgraph out as flat node/edge lists via `elementMap`. Edge
 * `elementMap` carries the incident vertices (IN/OUT) and the edge's `confidence`
 * in one step, so no `by()` projection is needed. A separate vertex query captures
 * nodes with no in-subgraph edge (meaningful singletons) that the edge query alone
 * would never surface.
 */
async function extractCodeSubgraph(g) {
  const vRaw = await g.V().hasLabel(...NODE_LABELS).hasNot("external").hasNot("parsed").elementMap().toList();
  const eRaw = await g.E().hasLabel(...EDGE_LABELS).elementMap().toList();

  const nodes = vRaw.map((m) => {
    const o = mapToObj(m);
    return { id: String(o.id), label: o.label, name: o.name, filePath: o.filePath || o.path, changed: o.changed === true };
  });

  const edges = eRaw.map((m) => {
    const o = mapToObj(m);
    return { from: String(o.OUT.id), to: String(o.IN.id), conf: o.confidence };
  });

  return { nodes, edges };
}

/**
 * Pure community detection over plain node/edge lists — no graph I/O, so it is
 * unit-testable without a server.
 *
 * @param {Array<{id,label,name,filePath}>} nodes
 * @param {Array<{from,to,conf}>} edges
 * @param {object} params - see communityDetection; `params.churn` is an optional
 *   per-file `{ [path]: { added, removed, deleted } }` map (all changed files, not
 *   just clustered ones) that lets each community be described by *how* it changed.
 * @returns {CommunityResult & { assignments: Map<string, number> }}
 */
export function detectCommunities(nodes, edges, params = {}) {
  const resolution = params.resolution || 1;
  const minSize = params.minCommunitySize || 2;
  const churn = params.churn || null;

  const nodeById = new Map(nodes.map((n) => [n.id, n]));
  const graph = new Graph({ type: "undirected", multi: false, allowSelfLoops: false });
  for (const n of nodes) graph.addNode(n.id);

  for (const e of edges) {
    if (e.from === e.to) continue;
    if (!graph.hasNode(e.from) || !graph.hasNode(e.to)) continue; // endpoint was filtered out
    const w = WEIGHT[e.conf] || 1;
    if (graph.hasEdge(e.from, e.to)) {
      graph.updateEdgeAttribute(e.from, e.to, "weight", (x) => (x || 0) + w);
    } else {
      graph.addEdge(e.from, e.to, { weight: w });
    }
  }

  let communities = {};
  let modularity = 0;
  if (graph.size > 0) {
    const detailed = louvain.detailed(graph, { getEdgeWeight: "weight", resolution });
    communities = detailed.communities;
    modularity = detailed.modularity;
  } else {
    // No edges: every node is its own community.
    graph.forEachNode((n, _attr) => { communities[n] = graph.nodes().indexOf(n); });
  }

  const { assignments, splitCount } = splitDisconnected(graph, communities);

  // Group members by final community id.
  const members = new Map();
  for (const [id, comm] of assignments) {
    if (!members.has(comm)) members.set(comm, []);
    members.get(comm).push(id);
  }

  const built = [...members.entries()]
    .map(([, ids]) => rollUp(ids, nodeById, churn))
    .sort((a, b) => b.size - a.size);

  const communityList = built.filter((c) => c.size >= minSize).map((c, i) => ({ ...c, id: i }));
  const isolatedCount = built.filter((c) => c.size < minSize).length;

  return {
    communityCount: communityList.length,
    isolatedCount,
    modularity: Number(modularity.toFixed(4)),
    splitCount,
    communities: communityList,
    churn: globalChurn(churn),
    interpretation: interpret(communityList, modularity, churn),
    totalNodes: graph.order,
    totalEdges: graph.size,
    assignments,
  };
}

/** Sum churn across all changed files, and count how many were deleted outright. */
function globalChurn(churn) {
  if (!churn) return null;
  let added = 0, removed = 0, deletedFiles = 0;
  for (const c of Object.values(churn)) {
    added += c.added || 0;
    removed += c.removed || 0;
    if (c.deleted) deletedFiles++;
  }
  return { added, removed, deletedFiles };
}

/**
 * Turn the raw partition into a reading a human can act on, deterministically.
 * The headline grades separation by modularity; the bullets call out what the
 * dense structure is *made of* (e.g. all test scaffolding — a real signal for a
 * removal PR, where deleted production code leaves nothing to cluster) and how
 * much of it the PR actually changed versus context pulled in for shape.
 */
function interpret(communities, modularity, churn) {
  if (communities.length === 0) {
    return { headline: "No thematic structure: the change has no densely-connected group of two or more code vertices.", reading: [] };
  }

  const headline = modularity < 0.3
    ? `Diffuse (modularity ${modularity.toFixed(2)}): the change reads as one loosely-connected mass rather than separable themes.`
    : modularity > 0.7
      ? `Sharply themed (modularity ${modularity.toFixed(2)}): ${communities.length} clearly separated communities.`
      : `Moderately themed (modularity ${modularity.toFixed(2)}): ${communities.length} distinguishable communities with some cross-talk.`;

  const reading = [];
  const testDom = communities.filter((c) => (c.testShare ?? 0) >= 0.8).length;
  if (testDom === communities.length) {
    reading.push("Every community is test infrastructure. The production code this change touches has too few internal call edges to cluster — typical when code is removed or thinned — so the densest remaining structure is the test scaffolding around it. Read these as the test areas the change disturbs, not new functional groupings.");
  } else if (testDom > communities.length / 2) {
    reading.push(`${testDom} of ${communities.length} communities are test infrastructure, so the change's dense structure sits more in tests than in production code.`);
  }

  const g = globalChurn(churn);
  if (g && (g.added > 0 || g.removed > 0)) {
    const net = g.added - g.removed;
    const direction = net < 0 ? "net-deletion" : net > 0 ? "net-addition" : "balanced";
    reading.push(`This change is ${direction} overall (−${g.removed}/+${g.added} lines). ${net < 0 ? "The communities describe surviving structure being trimmed, not new structure being built." : "The communities describe structure being grown or reworked."}`);
    if (g.deletedFiles > 0) {
      reading.push(`${g.deletedFiles} file(s) were deleted outright. Deleted code has no vertices, so it is absent from every community — these themes show only the surviving structure the removal reached into. See Removal References for what was actually removed.`);
    }
  } else {
    const total = communities.reduce((s, c) => s + c.size, 0);
    const changed = communities.reduce((s, c) => s + c.changedCount, 0);
    const share = total ? Math.round((changed / total) * 100) : 0;
    reading.push(`${share}% of clustered vertices are actually changed by this PR; the rest is surrounding context pulled in to give the change structural shape.`);
  }

  return { headline, reading };
}

/**
 * Describe what a community is: its kind (test scaffolding / call cluster / …),
 * how it changed, and its file span. When churn is known it drives the change
 * clause (real line counts beat the changed/unchanged vertex-share heuristic);
 * otherwise we fall back to that share.
 */
function describeRole(label, changedShare, fileCount, testShare, churnSummary) {
  const kind = testShare >= 0.6 ? "test scaffolding"
    : label === "Type" ? "type hierarchy"
      : label === "File" ? "file group"
        : "call cluster";
  const change = churnSummary
    ? `${churnSummary.mode} (−${churnSummary.removed}/+${churnSummary.added})`
    : changedShare >= 0.6 ? "mostly changed"
      : changedShare <= 0.2 ? "mostly context"
        : "mixed changed/context";
  const span = fileCount > 1 ? `spanning ${fileCount} files` : "within one file";
  return `${kind}, ${change}, ${span}`;
}

/**
 * Well-connectedness pass: within each Louvain community, find connected pieces
 * (using the code subgraph's own edges) and give each piece its own community id.
 * A community that is already internally connected passes through unchanged; a
 * disconnected one is split. This recovers Leiden's connectedness guarantee.
 */
function splitDisconnected(graph, communities) {
  const byComm = new Map();
  for (const [node, c] of Object.entries(communities)) {
    if (!byComm.has(c)) byComm.set(c, []);
    byComm.get(c).push(node);
  }

  const assignments = new Map();
  let nextId = 0;
  let splitCount = 0;

  for (const memberNodes of byComm.values()) {
    const memberSet = new Set(memberNodes);
    const seen = new Set();
    let pieces = 0;
    for (const start of memberNodes) {
      if (seen.has(start)) continue;
      pieces++;
      const id = nextId++;
      const queue = [start];
      seen.add(start);
      while (queue.length) {
        const u = queue.pop();
        assignments.set(u, id);
        graph.forEachNeighbor(u, (nb) => {
          if (memberSet.has(nb) && !seen.has(nb)) { seen.add(nb); queue.push(nb); }
        });
      }
    }
    if (pieces > 1) splitCount++;
  }

  return { assignments, splitCount };
}

/** Roll a community's member vertices up to files, label mix, churn, and a role descriptor. */
function rollUp(ids, nodeById, churn) {
  const fileCounts = new Map();
  const labelCounts = {};
  let changedCount = 0;
  for (const id of ids) {
    const n = nodeById.get(id);
    if (!n) continue;
    if (n.filePath) fileCounts.set(n.filePath, (fileCounts.get(n.filePath) || 0) + 1);
    labelCounts[n.label] = (labelCounts[n.label] || 0) + 1;
    if (n.changed) changedCount++;
  }
  let testCount = 0;
  for (const id of ids) {
    const n = nodeById.get(id);
    if (n && isTestFile(n.filePath)) testCount++;
  }
  const files = [...fileCounts.entries()].sort((a, b) => b[1] - a[1]).map(([f]) => f);
  const dominantLabel = Object.entries(labelCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || "Function";
  const testShare = ids.length ? testCount / ids.length : 0;

  // Roll churn up over the community's *distinct* files (a file counts once even
  // when several member functions live in it).
  let churnSummary = null;
  if (churn) {
    let added = 0, removed = 0, withChurn = 0;
    for (const f of files) {
      if (churn[f]) { added += churn[f].added || 0; removed += churn[f].removed || 0; withChurn++; }
    }
    if (withChurn > 0) churnSummary = { added, removed, mode: churnMode(added, removed) };
  }

  return {
    size: ids.length,
    files,
    dominantFile: files[0] || null,
    labelCounts,
    changedCount,
    dominantLabel,
    testShare,
    churn: churnSummary,
    role: describeRole(dominantLabel, ids.length ? changedCount / ids.length : 0, files.length, testShare, churnSummary),
  };
}

/** Classify a community's net churn: dominated by removals, additions, or balanced. */
function churnMode(added, removed) {
  if (added === 0 && removed > 0) return "purely reduced";
  if (removed > added * 2) return "reduced";
  if (added > removed * 2) return "expanded";
  return "reworked";
}

/**
 * Is this a test file? Test methods extract as Function vertices, so the vertex
 * label alone under-counts test-ness; the file path is the reliable tell.
 */
function isTestFile(p) {
  if (!p) return false;
  if (/(^|\/)(test|tests)\//i.test(p)) return true;
  const base = p.split("/").pop() || "";
  return /(Test|Tests|IT|ITCase|TestCase)\.\w+$/.test(base) || /(^test_|_test)\.\w+$/.test(base) || /\.test\.\w+$/.test(base);
}

/**
 * Stamp each vertex with its final `community` id. One traversal per community
 * (`V(...ids).property(...)`) keeps this to a handful of round trips.
 */
async function writeBackCommunities(g, assignments) {
  const byComm = new Map();
  for (const [id, comm] of assignments) {
    if (!byComm.has(comm)) byComm.set(comm, []);
    byComm.get(comm).push(coerceId(id));
  }
  for (const [comm, ids] of byComm) {
    await g.V(...ids).property("community", comm).iterate();
  }
}

/** TinkerGraph ids are numeric; ids arrive as strings from our node keys. */
function coerceId(id) {
  const n = Number(id);
  return Number.isNaN(n) ? id : n;
}

/**
 * Convert a gremlin `elementMap` Map into a plain object, keying by the enum's
 * element name so `t.id`/`t.label` become `id`/`label` and the edge adjacency
 * `Direction.IN`/`Direction.OUT` become `IN`/`OUT`. Nested vertex maps (on edges)
 * are converted recursively. Avoids depending on driver enum singleton identity.
 */
function mapToObj(m) {
  if (!(m instanceof Map)) return m;
  const o = {};
  for (const [k, v] of m) o[String(k)] = v instanceof Map ? mapToObj(v) : v;
  return o;
}
