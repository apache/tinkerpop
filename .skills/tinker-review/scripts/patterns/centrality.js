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
import { changedMeaningful, atLeastP, atLeast } from "../graph/change-levels.js";

const { process: { statics: __ } } = gremlin;

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
 * @param {boolean} [params.changedOnly] - Only check changed functions (default: true).
 *   "Changed" defaults to the meaningful tiers (BEHAVIORAL, STRUCTURAL); NONE and
 *   FORMATTING are excluded.
 * @param {string} [params.minChangeLevel] - Narrow the changed filter to this
 *   level and above (e.g. "STRUCTURAL" for signature churn only)
 * @param {number} [params.topN] - Return top N results (default: 10)
 * @param {number} [params.minDegree] - Minimum combined in+out degree to include (default: 3)
 * @param {boolean} [params.excludeLibrary] - Drop calls to library-origin external
 *   stubs from out-degree so JDK/accessor noise doesn't inflate hotspots (default: true).
 *   Only takes effect once classifyExternals has tagged `origin`.
 * @returns {Promise<CentralityResult>}
 */

/**
 * @typedef {Object} Hotspot
 * @property {string}  name
 * @property {string}  filePath
 * @property {string}  signature
 * @property {number}  linesStart
 * @property {number}  linesEnd
 * @property {string}  changeLevel        how the PR moved this function (NONE | FORMATTING | BEHAVIORAL | STRUCTURAL)
 * @property {number}  inDegree           incoming call edges (how many functions call it)
 * @property {number}  outDegree          outgoing call edges, excluding origin:library targets
 * @property {number}  totalDegree        inDegree + outDegree — the centrality score
 * @property {boolean} inherentlyCentral  a boilerplate method (equals/toString/…) central by
 *                                         nature; surfaced only when the PR modified it
 *
 * @typedef {Object} CentralityResult
 * @property {Hotspot[]} hotspots               top functions by totalDegree (>= minDegree)
 * @property {number}    totalAnalyzed          functions considered
 * @property {number}    aboveThreshold         how many cleared minDegree
 * @property {number}    filteredAsBoilerplate  inherently-central, unchanged functions dropped
 */
export async function highCentrality(g, params = {}) {
  const changedOnly = params.changedOnly !== false;
  const topN = params.topN || 10;
  const minDegree = params.minDegree || 3;
  const excludeLibrary = params.excludeLibrary !== false;
  // Default risk lens: BEHAVIORAL + STRUCTURAL. `minChangeLevel` can narrow to
  // STRUCTURAL-only (signature churn) when a reviewer wants just the API surface.
  const changePredicate = params.minChangeLevel
    ? atLeastP(params.minChangeLevel)
    : changedMeaningful();

  let traversal = g.V().hasLabel("Function");
  if (changedOnly) {
    traversal = traversal.has("changeLevel", changePredicate);
  }

  const functions = await traversal.elementMap().toList();
  const results = [];
  const filtered = [];

  for (const fnMap of functions) {
    const vertexId = fnMap.get(gremlin.process.t.id);
    const name = fnMap.get("name");
    const changeLevel = fnMap.get("changeLevel");
    // A boilerplate method (equals/toString/…) is surfaced only when the PR
    // changed it meaningfully — a formatting-only touch is not enough.
    const meaningfullyChanged = atLeast(changeLevel || "NONE", "BEHAVIORAL");

    const inDegree = await g.V(vertexId).inE("calls").count().next();
    // Out-degree optionally skips calls to library-origin externals (getName,
    // toString, …) so ubiquitous JDK/accessor calls don't inflate the hotspot.
    let outTraversal = g.V(vertexId).outE("calls");
    if (excludeLibrary) {
      outTraversal = outTraversal.where(__.inV().not(__.has("origin", "library")));
    }
    const outDegree = await outTraversal.count().next();

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
      changeLevel,
      inDegree: inCount,
      outDegree: outCount,
      totalDegree,
      inherentlyCentral: INHERENTLY_CENTRAL.has(name),
    };

    if (INHERENTLY_CENTRAL.has(name) && !meaningfullyChanged) {
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
