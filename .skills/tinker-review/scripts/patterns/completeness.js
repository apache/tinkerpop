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
 * Check that a vertex has all expected outgoing/incoming edge labels.
 *
 * @param {object} g - gremlin-js GraphTraversalSource
 * @param {object} params
 * @param {string} params.vertexLabel - Label of vertex to check (e.g., "Step")
 * @param {string} [params.vertexName] - Name property to filter by
 * @param {string[]} params.expectedEdges - Edge labels that should exist
 *   Prefix with "in:" or "out:" for direction (default: "out:")
 *   e.g., ["out:has_rule", "in:implements_step", "in:covers", "in:documents"]
 * @returns {Promise<CompletenessResult[]>}
 */

/**
 * @typedef {Object} CompletenessResult
 * @property {string}   node     the vertex checked (name or identifier)
 * @property {string[]} present  expected edge specs that exist (e.g. "out:defines")
 * @property {string[]} missing  expected edge specs that are absent — the gaps to weigh
 * @property {number}   score    present / (present + missing), 0..1
 */
export async function completeness(g, params) {
  const { vertexLabel, vertexName, expectedEdges } = params;

  let traversal = g.V().hasLabel(vertexLabel);
  if (vertexName) {
    traversal = traversal.has("name", vertexName);
  }

  const vertices = await traversal.elementMap().toList();

  const results = [];

  for (const vertexMap of vertices) {
    const vertexId = vertexMap.get(gremlin.process.t.id);
    const name = vertexMap.get("name") || String(vertexId);

    const present = [];
    const missing = [];

    for (const edgeSpec of expectedEdges) {
      const { direction, label } = parseEdgeSpec(edgeSpec);
      const exists = await edgeExists(g, vertexId, direction, label);

      if (exists) {
        present.push(edgeSpec);
      } else {
        missing.push(edgeSpec);
      }
    }

    const total = present.length + missing.length;
    const score = total > 0 ? present.length / total : 1;

    results.push({ node: name, present, missing, score });
  }

  return results;
}

function parseEdgeSpec(spec) {
  if (spec.startsWith("in:")) {
    return { direction: "in", label: spec.slice(3) };
  }
  if (spec.startsWith("out:")) {
    return { direction: "out", label: spec.slice(4) };
  }
  return { direction: "out", label: spec };
}

async function edgeExists(g, vertexId, direction, label) {
  let traversal;
  if (direction === "in") {
    traversal = g.V(vertexId).inE(label);
  } else {
    traversal = g.V(vertexId).outE(label);
  }

  const edges = await traversal.limit(1).toList();
  return edges.length > 0;
}
