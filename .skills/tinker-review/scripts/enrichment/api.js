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

import { readFile } from "node:fs/promises";
import { join } from "node:path";
import gremlin from "gremlin";
import { CONFIDENCE, normalizeConfidence, isValidConfidence } from "../graph/confidence.js";

const { process: { statics: __ } } = gremlin;

let cachedSteps = null;

// === Read operations ===

export async function listFunctions(g, filter = {}) {
  let t = g.V().hasLabel("Function");
  if (filter.changed !== undefined) t = t.has("changed", filter.changed);
  if (filter.visibility) t = t.has("visibility", filter.visibility);
  if (filter.filePath) t = t.has("filePath", filter.filePath);
  const results = await t.elementMap().toList();
  return results.map(m => ({
    name: m.get("name"),
    signature: m.get("signature"),
    filePath: m.get("filePath"),
    visibility: m.get("visibility"),
    changed: m.get("changed"),
    linesStart: m.get("lines_start"),
    linesEnd: m.get("lines_end"),
  }));
}

export async function listTypes(g, filter = {}) {
  let t = g.V().hasLabel("Type");
  if (filter.kind) t = t.has("kind", filter.kind);
  if (filter.filePath) t = t.has("filePath", filter.filePath);
  const results = await t.elementMap().toList();
  return results.map(m => ({
    name: m.get("name"),
    kind: m.get("kind"),
    visibility: m.get("visibility"),
    filePath: m.get("filePath"),
  }));
}

export async function getCallsFrom(g, functionName, filePath) {
  const results = await g.V().hasLabel("Function")
    .has("name", functionName)
    .has("filePath", filePath)
    .out("calls")
    .elementMap()
    .toList();
  return results.map(m => ({
    calleeName: m.get("name"),
    filePath: m.get("filePath"),
  }));
}

export async function getCanonicalSteps(repoPath) {
  if (cachedSteps) return cachedSteps;
  const g4Path = join(repoPath, "gremlin-language/src/main/antlr4/Gremlin.g4");
  const g4 = await readFile(g4Path, "utf-8");
  cachedSteps = [...g4.matchAll(/traversalMethod_(\w+)/g)].map(m => m[1]);
  cachedSteps = [...new Set(cachedSteps)].sort();
  return cachedSteps;
}

// The likely symbol name a source file defined — its basename without extension
// (Krb5Authenticator.java -> Krb5Authenticator). Used to turn deleted-file paths
// into names the agent can grep the rest of the repo for.
function symbolFromPath(path) {
  const base = path.split("/").pop() || path;
  const dot = base.indexOf(".");
  return dot > 0 ? base.slice(0, dot) : base;
}

function extOf(path) {
  return path.includes(".") ? path.split(".").pop() : "";
}

// === Removal-impact reads ===

/**
 * Files the PR deleted (stub Files marked deleted). Each entry pairs the path
 * with the symbol name it likely defined, so the agent can grep the surviving
 * tree for lingering references to removed code.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @returns {Promise<{path: string, symbol: string}[]>}
 */
export async function listDeleted(g) {
  const paths = await g.V().hasLabel("File").has("deleted", true).values("path").toList();
  return paths.map((path) => ({ path, symbol: symbolFromPath(path) }));
}

/**
 * Unresolved external callees (external Function stubs) — names the changed code
 * calls that weren't defined in the changed set. Includes each stub's `origin`
 * (library/project/unresolved, once classifyExternals has run) and flags any
 * whose name matches a deleted file's symbol: a changed file still calling a
 * just-removed name is a dangling reference visible in the graph alone.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @returns {Promise<{name: string, origin: string, matchesDeletedSymbol: boolean}[]>}
 */
export async function listExternalRefs(g) {
  const rows = await g.V().hasLabel("Function").has("external", true)
    .project("name", "origin")
    .by("name")
    .by(__.coalesce(__.values("origin"), __.constant("unclassified")))
    .toList();
  const deletedPaths = await g.V().hasLabel("File").has("deleted", true).values("path").toList();
  const deletedSymbols = new Set(deletedPaths.map(symbolFromPath));

  const rank = { project: 0, unresolved: 1, unclassified: 2, library: 3 };
  return rows
    .map((r) => ({
      name: r.get("name"),
      origin: r.get("origin"),
      matchesDeletedSymbol: deletedSymbols.has(r.get("name")),
    }))
    .sort((a, b) => {
      if (a.matchesDeletedSymbol !== b.matchesDeletedSymbol) return a.matchesDeletedSymbol ? -1 : 1;
      return (rank[a.origin] ?? 9) - (rank[b.origin] ?? 9);
    });
}

// === Write operations ===

export async function mapStep(g, functionName, filePath, canonicalStepName, confidence) {
  const conf = normalizeConfidence(confidence, CONFIDENCE.INFERRED);
  const stepExists = await g.V().hasLabel("Step").has("name", canonicalStepName).hasNext();
  if (!stepExists) {
    await g.addV("Step")
      .property("name", canonicalStepName)
      .property("canonical_name", canonicalStepName)
      .next();
  }

  await g.V().hasLabel("Function")
    .has("name", functionName)
    .has("filePath", filePath)
    .addE("implements_step")
    .property("confidence", conf)
    .to(__.V().hasLabel("Step").has("name", canonicalStepName))
    .next();

  return { mapped: `${functionName} -> ${canonicalStepName}`, confidence: conf };
}

/**
 * Re-grade the confidence of existing edge(s) after the agent verifies them
 * against source. Identifies edges by their source vertex (name, optionally
 * pinned to a file) and relation, optionally narrowed to a named target —
 * covering the edges agents actually verify (`calls`, `implements_step`).
 * Promote a confirmed edge to EXTRACTED, or downgrade a wrong resolution to
 * AMBIGUOUS so it surfaces in the report's review list.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {object} params
 * @param {string} params.relation - Edge label (e.g. calls, implements_step)
 * @param {string} params.fromName - Source vertex name
 * @param {string} [params.fromFile] - Pin the source to this filePath
 * @param {string} [params.toName] - Narrow to edges whose target has this name
 * @param {string} [params.fromLabel] - Source vertex label (default Function)
 * @param {string} params.confidence - EXTRACTED | INFERRED | AMBIGUOUS
 * @returns {Promise<object>}
 */
export async function setEdgeConfidence(g, params = {}) {
  const { relation, fromName, fromFile, toName, fromLabel = "Function", confidence } = params;
  if (!isValidConfidence(confidence)) {
    return { error: `invalid confidence "${confidence}" — use EXTRACTED, INFERRED, or AMBIGUOUS` };
  }
  if (!relation || !fromName) {
    return { error: "relation and fromName are required" };
  }

  let t = g.V().hasLabel(fromLabel).has("name", fromName);
  if (fromFile) t = t.has("filePath", fromFile);
  t = t.outE(relation);
  if (toName) t = t.where(__.inV().has("name", toName));

  const updated = await t.property("confidence", confidence).toList();
  return {
    relation,
    from: fromName,
    to: toName || "*",
    confidence,
    updated: updated.length,
  };
}

/**
 * Record a lingering reference to removed code: a surviving file (fromPath) that
 * still mentions a symbol defined by a deleted file (toPath). Creates a
 * `references` edge File -> File(deleted), carrying the matched symbol and
 * location. The source file is often outside the changed set and has no vertex
 * yet, so it's find-or-created as an unparsed marker. This is the payoff of a
 * removal review — "the PR deleted X, but these places still use it."
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {object} params
 * @param {string} params.fromPath - Surviving file that references the removed symbol
 * @param {string} params.toPath - Deleted file path (must be an existing deleted File)
 * @param {string} [params.symbol] - The removed symbol found in fromPath
 * @param {string} [params.location] - Where (e.g. "L42" or a line snippet)
 * @param {string} [params.confidence] - default INFERRED (a textual grep match)
 * @returns {Promise<object>}
 */
export async function addReference(g, params = {}) {
  const { fromPath, toPath, symbol, location, confidence } = params;
  const conf = normalizeConfidence(confidence, CONFIDENCE.INFERRED);
  if (!fromPath || !toPath) {
    return { error: "fromPath and toPath are required" };
  }

  const targetExists = await g.V().hasLabel("File").has("path", toPath).hasNext();
  if (!targetExists) {
    return { error: `no deleted File vertex for toPath "${toPath}" (use listDeleted for valid targets)` };
  }

  const srcExists = await g.V().hasLabel("File").has("path", fromPath).hasNext();
  if (!srcExists) {
    await g.addV("File")
      .property("path", fromPath)
      .property("language", extOf(fromPath))
      .property("changed", false)
      .property("parsed", false)
      .property("deleted", false)
      .next();
  }

  await g.V().hasLabel("File").has("path", fromPath)
    .addE("references")
    .property("confidence", conf)
    .property("symbol", symbol || "")
    .property("location", location || "")
    .to(__.V().hasLabel("File").has("path", toPath))
    .next();

  return { referenced: `${fromPath} -> ${toPath}`, symbol: symbol || "", confidence: conf };
}

export async function linkDiscussion(g, url, source, title, body, confidence) {
  const conf = normalizeConfidence(confidence, CONFIDENCE.INFERRED);
  await g.addV("Discussion")
    .property("url", url)
    .property("source", source)
    .property("title", title)
    .property("body", body || "")
    .next();

  const prDiscussion = await g.V().hasLabel("Discussion").has("source", "pr").hasNext();
  if (prDiscussion) {
    await g.V().hasLabel("Discussion").has("source", "pr")
      .addE("addresses")
      .property("confidence", conf)
      .to(__.V().hasLabel("Discussion").has("url", url))
      .next();
  }

  return { linked: `${source}: ${title}`, confidence: conf };
}

export async function annotate(g, label, name, key, value) {
  await g.V().hasLabel(label).has("name", name)
    .property(key, value)
    .next();

  return { annotated: `${label}:${name}.${key} = ${value}` };
}

export async function linkDoc(g, entityLabel, entityName, docPath, section, confidence) {
  const conf = normalizeConfidence(confidence, CONFIDENCE.INFERRED);
  const docExists = await g.V().hasLabel("Doc").has("path", docPath).hasNext();
  if (!docExists) {
    await g.addV("Doc")
      .property("path", docPath)
      .property("section", section || "")
      .next();
  }

  await g.V().hasLabel("Doc").has("path", docPath)
    .addE("documents")
    .property("confidence", conf)
    .to(__.V().hasLabel(entityLabel).has("name", entityName))
    .next();

  return { linked: `${docPath} documents ${entityLabel}:${entityName}`, confidence: conf };
}

export async function addGrammarRule(g, name, production) {
  await g.addV("GrammarRule")
    .property("name", name)
    .property("production", production || "")
    .next();

  return { added: `GrammarRule: ${name}` };
}

export async function createPrDiscussion(g, pr, title, body) {
  const exists = await g.V().hasLabel("Discussion").has("source", "pr").hasNext();
  if (exists) return { created: false };

  await g.addV("Discussion")
    .property("url", `https://github.com/apache/tinkerpop/pull/${pr}`)
    .property("source", "pr")
    .property("title", title || `PR #${pr}`)
    .property("body", body || "")
    .next();

  return { created: true, pr };
}
