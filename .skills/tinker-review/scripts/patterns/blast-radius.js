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

const INHERENTLY_CENTRAL = new Set([
  "equals", "hashCode", "toString", "clone", "close", "compareTo",
  "iterator", "hasNext", "next", "get", "set", "size", "isEmpty",
  "contains", "add", "remove", "clear", "values", "entrySet", "keySet",
  "finalize", "notify", "notifyAll", "wait",
]);

/**
 * Calculate blast radius — how many functions are reachable downstream
 * from changed functions via call edges. High blast radius means a change
 * here affects many callers.
 *
 * Methods that are inherently central (equals, toString, etc.) are filtered
 * out UNLESS they were modified in this PR.
 *
 * @param {object} g - gremlin-js GraphTraversalSource
 * @param {object} params
 * @param {number} [params.depth] - Max hops to traverse (default: 3)
 * @param {boolean} [params.changedOnly] - Start from changed functions only (default: true)
 * @returns {Promise<BlastRadiusResult>}
 */

/**
 * @typedef {Object} BlastRadiusFn
 * @property {string}  name
 * @property {string}  filePath
 * @property {string}  signature
 * @property {number}  linesStart
 * @property {number}  linesEnd
 * @property {boolean} changed         whether the PR modified this function
 * @property {number}  reachableCount  callers reachable within `depth` hops upstream; high = the
 *                                      change ripples widely (for driver/server this is expected)
 * @property {number}  depth           hop limit used for this row
 *
 * @typedef {Object} BlastRadiusResult
 * @property {BlastRadiusFn[]} functions        changed functions and how far each one's change reaches
 * @property {number}          maxReachable     largest reachableCount across changed functions
 * @property {number}          totalWithCallers changed functions that have any upstream callers
 * @property {number}          depth            hop limit applied
 */
export async function blastRadius(g, params = {}) {
  const depth = params.depth || 3;
  const changedOnly = params.changedOnly !== false;

  let traversal = g.V().hasLabel("Function");
  if (changedOnly) {
    traversal = traversal.has("changed", true);
  }

  const functions = await traversal.elementMap().toList();
  const results = [];

  for (const fnMap of functions) {
    const vertexId = fnMap.get(gremlin.process.t.id);
    const name = fnMap.get("name");
    const changed = fnMap.get("changed");

    if (INHERENTLY_CENTRAL.has(name) && !changed) continue;

    const reachable = await g.V(vertexId)
      .repeat(gremlin.process.statics.in_("calls"))
      .times(depth)
      .emit()
      .dedup()
      .count()
      .next();

    const count = reachable.value;
    if (count > 0) {
      results.push({
        name,
        filePath: fnMap.get("filePath"),
        signature: fnMap.get("signature"),
        linesStart: fnMap.get("lines_start"),
        linesEnd: fnMap.get("lines_end"),
        changed,
        reachableCount: count,
        depth,
      });
    }
  }

  results.sort((a, b) => b.reachableCount - a.reachableCount);

  return {
    functions: results,
    maxReachable: results.length > 0 ? results[0].reachableCount : 0,
    totalWithCallers: results.length,
    depth,
  };
}
