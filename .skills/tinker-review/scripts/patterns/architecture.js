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

import gremlin from "gremlin";

const { process: { t } } = gremlin;

/**
 * Generate architecture SVG input from the graph's cluster analysis.
 *
 * Queries File vertices and depends_on edges from the populated graph,
 * groups files into clusters by directory path, and returns the node/edge
 * structure expected by the renderer's generateClusterSvg().
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {object} params
 * @param {object} [params.clusterResult] - Output from clusterAnalysis() (connectedComponent clusters)
 * @param {boolean} [params.changedOnly] - Only include changed files (default: false)
 * @param {number} [params.maxNodes] - Cap on nodes to render (default: 40)
 * @returns {Promise<{nodes: Array, edges: Array}>}
 */
export async function architecture(g, params = {}) {
  const { clusterResult, changedOnly = false, maxNodes = 40 } = params;

  let traversal = g.V().hasLabel("File");
  if (changedOnly) {
    traversal = traversal.has("changed", true);
  }

  const fileVertices = await traversal.elementMap().toList();

  if (fileVertices.length === 0) {
    return { nodes: [], edges: [] };
  }

  const clusterAssignment = buildClusterAssignment(fileVertices, clusterResult);

  let nodes = fileVertices.map((fileMap) => {
    const path = fileMap.get("path");
    const changed = fileMap.get("changed") || false;
    const id = path;
    const label = shortLabel(path);
    const cluster = clusterAssignment.get(path) || dirCluster(path);

    return { id, label, cluster, changed };
  });

  if (nodes.length > maxNodes) {
    nodes = prioritizeNodes(nodes, maxNodes);
  }

  const nodeIds = new Set(nodes.map((n) => n.id));

  // Find cross-file function calls: File A defines Function X which calls Function Y defined in File B
  const edgeResults = await g.V().hasLabel("Function")
    .as("caller")
    .out("calls")
    .as("callee")
    .select("caller", "callee")
    .by("filePath")
    .toList();

  const edgeSet = new Set();
  const edges = [];
  for (const row of edgeResults) {
    const from = row.get("caller");
    const to = row.get("callee");
    if (from && to && from !== to && nodeIds.has(from) && nodeIds.has(to)) {
      const key = `${from}|${to}`;
      if (!edgeSet.has(key)) {
        edgeSet.add(key);
        edges.push({ from, to });
      }
    }
  }

  return { nodes, edges };
}

function buildClusterAssignment(fileVertices, clusterResult) {
  const assignment = new Map();

  if (clusterResult && clusterResult.clusters) {
    for (const cluster of clusterResult.clusters) {
      const clusterLabel = deriveClusterLabel(cluster.files);
      for (const filePath of cluster.files) {
        assignment.set(filePath, clusterLabel);
      }
    }
  } else {
    for (const fileMap of fileVertices) {
      const path = fileMap.get("path");
      assignment.set(path, dirCluster(path));
    }
  }

  return assignment;
}

function deriveClusterLabel(files) {
  if (files.length === 0) return "other";
  if (files.length === 1) return dirCluster(files[0]);

  const segments = files.map((f) => f.split("/").slice(0, -1));
  const minLen = Math.min(...segments.map((s) => s.length));

  let common = 0;
  for (let i = 0; i < minLen; i++) {
    const seg = segments[0][i];
    if (segments.every((s) => s[i] === seg)) {
      common = i + 1;
    } else {
      break;
    }
  }

  if (common > 0) {
    return segments[0].slice(0, common).join("/");
  }

  return dirCluster(files[0]);
}

function dirCluster(filePath) {
  const parts = filePath.split("/");
  if (parts.length <= 1) return "/";
  if (parts.length <= 3) return parts.slice(0, -1).join("/");
  return parts.slice(0, 2).join("/");
}

function shortLabel(filePath) {
  const parts = filePath.split("/");
  return parts[parts.length - 1];
}

function prioritizeNodes(nodes, maxNodes) {
  const changed = nodes.filter((n) => n.changed);
  const unchanged = nodes.filter((n) => !n.changed);

  if (changed.length >= maxNodes) {
    return changed.slice(0, maxNodes);
  }

  return [...changed, ...unchanged.slice(0, maxNodes - changed.length)];
}
