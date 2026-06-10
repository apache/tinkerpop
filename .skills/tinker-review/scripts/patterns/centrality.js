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
 * Identify high-centrality functions — those with many incoming and outgoing
 * call edges. These are structural hotspots where changes propagate widely.
 *
 * Methods that are inherently central by nature (equals, toString, hashCode, etc.)
 * are filtered out UNLESS they were modified in this PR — in which case they're
 * highly relevant because many callers depend on their behavior.
 *
 * @param {object} g - gremlin-js GraphTraversalSource
 * @param {object} params
 * @param {boolean} [params.changedOnly] - Only check changed functions (default: true)
 * @param {number} [params.topN] - Return top N results (default: 10)
 * @param {number} [params.minDegree] - Minimum combined in+out degree to include (default: 3)
 * @returns {Promise<CentralityResult>}
 */
export async function highCentrality(g, params = {}) {
  const changedOnly = params.changedOnly !== false;
  const topN = params.topN || 10;
  const minDegree = params.minDegree || 3;

  let traversal = g.V().hasLabel("Function");
  if (changedOnly) {
    traversal = traversal.has("changed", true);
  }

  const functions = await traversal.elementMap().toList();
  const results = [];
  const filtered = [];

  for (const fnMap of functions) {
    const vertexId = fnMap.get(gremlin.process.t.id);
    const name = fnMap.get("name");
    const changed = fnMap.get("changed");

    const inDegree = await g.V(vertexId).inE("calls").count().next();
    const outDegree = await g.V(vertexId).outE("calls").count().next();

    const inCount = inDegree.value;
    const outCount = outDegree.value;
    const totalDegree = inCount + outCount;

    if (totalDegree < minDegree) continue;

    const entry = {
      name,
      filePath: fnMap.get("filePath"),
      signature: fnMap.get("signature"),
      linesStart: fnMap.get("lines_start"),
      linesEnd: fnMap.get("lines_end"),
      changed,
      inDegree: inCount,
      outDegree: outCount,
      totalDegree,
      inherentlyCentral: INHERENTLY_CENTRAL.has(name),
    };

    if (INHERENTLY_CENTRAL.has(name) && !changed) {
      filtered.push(entry);
    } else {
      results.push(entry);
    }
  }

  results.sort((a, b) => b.totalDegree - a.totalDegree);

  return {
    hotspots: results.slice(0, topN),
    totalAnalyzed: functions.length,
    aboveThreshold: results.length,
    filteredAsBoilerplate: filtered.length,
  };
}
