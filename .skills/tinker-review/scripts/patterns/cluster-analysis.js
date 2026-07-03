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

/**
 * Determine whether a PR's changed files form one coherent change or multiple
 * disconnected clusters. Uses connectedComponent() via the OLAP traversal source.
 *
 * A single cluster means the PR is one logical change. Multiple clusters suggest
 * the PR bundles unrelated changes or has hidden dependencies the graph didn't capture.
 *
 * @param {object} a - gremlin-js GraphTraversalSource bound with withComputer() (the 'a' source)
 * @param {object} params
 * @param {boolean} [params.changedOnly] - Only analyze changed files (default: true)
 * @returns {Promise<ClusterResult>}
 */

/**
 * @typedef {Object} Cluster
 * @property {number}   id     1-based cluster index (largest first)
 * @property {string[]} files  file paths in this connected component
 * @property {number}   size   files.length
 *
 * @typedef {Object} ClusterResult
 * @property {number}    clusterCount  disconnected components among changed files
 * @property {boolean}   coherent      true when the change is one logical unit (<= 1 cluster)
 * @property {Cluster[]} clusters      components, largest first
 * @property {number}    totalFiles    changed files placed into clusters
 */
export async function clusterAnalysis(a, params = {}) {
  const changedOnly = params.changedOnly !== false;

  let traversal = a.V().hasLabel("File");
  if (changedOnly) {
    traversal = traversal.has("changed", true);
  }

  const results = await traversal
    .connectedComponent()
    .project("path", "component")
      .by("path")
      .by("gremlin.connectedComponentVertexProgram.component")
    .toList();

  const clusterMap = new Map();
  for (const r of results) {
    const path = r.get("path");
    const component = r.get("component");
    if (!clusterMap.has(component)) {
      clusterMap.set(component, []);
    }
    clusterMap.get(component).push(path);
  }

  const clusters = [...clusterMap.values()]
    .sort((a, b) => b.length - a.length)
    .map((files, i) => ({
      id: i + 1,
      files,
      size: files.length,
    }));

  return {
    clusterCount: clusters.length,
    coherent: clusters.length <= 1,
    clusters,
    totalFiles: results.length,
  };
}
