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

/**
 * Find orphan vertices — nodes that are missing expected relationships.
 * Useful for detecting undocumented functions, untested steps, or
 * disconnected code.
 *
 * @param {object} g - gremlin-js GraphTraversalSource
 * @param {object} params
 * @param {string} params.vertexLabel - Label to check (e.g., "Step", "Function")
 * @param {string} params.expectedEdge - Edge label that should exist
 * @param {string} [params.direction] - "in" or "out" (default: "in")
 * @param {boolean} [params.changedOnly] - Only check changed vertices (default: false)
 * @returns {Promise<OrphanResult>}
 */
export async function orphans(g, params) {
  const { vertexLabel, expectedEdge } = params;
  const direction = params.direction || "in";
  const changedOnly = params.changedOnly || false;

  let traversal = g.V().hasLabel(vertexLabel);
  if (changedOnly) {
    traversal = traversal.has("changed", true);
  }

  const vertices = await traversal.elementMap().toList();
  const orphaned = [];

  for (const vMap of vertices) {
    const vertexId = vMap.get(gremlin.process.t.id);

    let edgeTraversal;
    if (direction === "in") {
      edgeTraversal = g.V(vertexId).inE(expectedEdge);
    } else {
      edgeTraversal = g.V(vertexId).outE(expectedEdge);
    }

    const edges = await edgeTraversal.limit(1).toList();
    if (edges.length === 0) {
      orphaned.push({
        name: vMap.get("name") || vMap.get("path") || String(vertexId),
        label: vertexLabel,
        filePath: vMap.get("filePath") || vMap.get("path"),
        missingEdge: `${direction}:${expectedEdge}`,
      });
    }
  }

  return {
    orphaned,
    totalChecked: vertices.length,
    totalOrphaned: orphaned.length,
  };
}
