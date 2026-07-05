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
import { symbolFromPath, createReferenceEdge } from "../graph/references.js";

const { process: { statics: __ } } = gremlin;

let cachedSteps = null;

// === Read operations ===

/**
 * List Function vertices, optionally filtered. The go-to orientation read:
 * `--changed true` shows just the functions this PR touched; `--visibility
 * public` narrows to the API surface. Returns signature and line span so you can
 * jump to source in the worktree.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {object} [filter] - { changed?: boolean, visibility?: string, filePath?: string }
 * @returns {Promise<{name, signature, filePath, visibility, changed, linesStart, linesEnd}[]>}
 */
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

/**
 * List Type vertices (classes, interfaces, enums), optionally by `kind` or
 * `filePath`. Use it to see the types the PR defines or touches before drilling
 * into their functions.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {object} [filter] - { kind?: string, filePath?: string }
 * @returns {Promise<{name, kind, visibility, filePath}[]>}
 */
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

/**
 * The direct callees of one function — its outgoing `calls` edges. Use it to
 * trace what a changed function depends on (e.g. does it still call a symbol the
 * PR removed?). The function is keyed by name AND file because names repeat
 * across the codebase.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {string} functionName - Caller's name
 * @param {string} filePath - Caller's file (disambiguates same-named functions)
 * @returns {Promise<{calleeName, filePath}[]>}
 */
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

/**
 * The canonical Gremlin traversal-step vocabulary, parsed from `Gremlin.g4`
 * (every `traversalMethod_<name>` rule). This is the authoritative set of step
 * names you map GLV/host-language methods onto with `mapStep`. Reads the grammar
 * file, not the graph, so it needs the repo path rather than `g`; result is
 * cached for the process.
 *
 * @param {string} repoPath - Path to the checked-out worktree
 * @returns {Promise<string[]>} sorted, de-duplicated canonical step names
 */
export async function getCanonicalSteps(repoPath) {
  if (cachedSteps) return cachedSteps;
  const g4Path = join(repoPath, "gremlin-language/src/main/antlr4/Gremlin.g4");
  const g4 = await readFile(g4Path, "utf-8");
  cachedSteps = [...g4.matchAll(/traversalMethod_(\w+)/g)].map(m => m[1]);
  cachedSteps = [...new Set(cachedSteps)].sort();
  return cachedSteps;
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

/**
 * Record that a GLV/host-language function implements a canonical Gremlin step,
 * as an `implements_step` edge (creating the Step vertex if it's the first time
 * that step is seen). The core enrichment move of the GLV playbook: map only
 * real traversal-step methods, not language boilerplate. Validate the step name
 * against `getCanonicalSteps` first.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {string} functionName - The implementing function's name
 * @param {string} filePath - The implementing function's file
 * @param {string} canonicalStepName - A name from getCanonicalSteps
 * @param {string} [confidence] - default INFERRED (a name-based mapping)
 * @returns {Promise<object>}
 */
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
 * Manual escape hatch for recording a lingering reference to removed code that
 * the Phase-1 removal-refs pass didn't catch — e.g. a config-string or
 * non-code-symbol reference, which that pass deliberately skips. The automatic
 * pass (patterns/removal-refs.js) handles code symbols; use this for the cases
 * that need a human/agent to spot. Creates the same `references` edge.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {object} params
 * @param {string} params.fromPath - Surviving file that references the removed symbol
 * @param {string} params.toPath - Deleted file path (must be an existing deleted File)
 * @param {string} [params.symbol] - The removed symbol found in fromPath
 * @param {string} [params.location] - Where (e.g. "L42" or a line snippet)
 * @param {string} [params.confidence] - default INFERRED (a textual match)
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

  await createReferenceEdge(g, { fromPath, toPath, symbol, location, confidence: conf });
  return { referenced: `${fromPath} -> ${toPath}`, symbol: symbol || "", confidence: conf };
}

/**
 * Attach an external discussion you found (JIRA issue, dev-list thread, proposal
 * doc) to the graph as a Discussion vertex, linked from the PR's own discussion
 * via an `addresses` edge. Use it when enrichment turns up prior context the
 * Phase-1 discovery pass missed, so the report can cite where the change was
 * debated.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {string} url - Canonical link to the discussion
 * @param {string} source - jira | devlist | proposal
 * @param {string} title - Human-readable title
 * @param {string} [body] - Optional excerpt/summary
 * @param {string} [confidence] - default INFERRED
 * @returns {Promise<object>}
 */
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

/**
 * Set an arbitrary property on a vertex identified by label + name. The
 * general-purpose escape hatch for recording a fact the schema has no dedicated
 * edge for (e.g. tagging a Function with a review note). Prefer a typed command
 * when one fits; reach for this only when none does.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {string} label - Vertex label (e.g. Function, Step)
 * @param {string} name - Vertex name
 * @param {string} key - Property key to set
 * @param {string} value - Property value
 * @returns {Promise<object>}
 */
export async function annotate(g, label, name, key, value) {
  await g.V().hasLabel(label).has("name", name)
    .property(key, value)
    .next();

  return { annotated: `${label}:${name}.${key} = ${value}` };
}

/**
 * Record that a documentation file documents a graph entity, as a `documents`
 * edge from a Doc vertex (created on first use) to the named entity. Use it to
 * connect a step/feature to the reference docs or recipe that covers it, so the
 * report can flag a change whose docs weren't updated.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {string} entityLabel - Label of the documented entity (e.g. Step)
 * @param {string} entityName - Name of the documented entity
 * @param {string} docPath - Path to the doc file
 * @param {string} [section] - Optional section/anchor within the doc
 * @param {string} [confidence] - default INFERRED
 * @returns {Promise<object>}
 */
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

/**
 * Add a GrammarRule vertex for a rule the PR introduces to the grammar. Use it
 * in the grammar playbook when a `*.g4` change adds a production the graph
 * should track so later steps can link to it.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {string} name - Rule name
 * @param {string} [production] - Optional production body
 * @returns {Promise<object>}
 */
export async function addGrammarRule(g, name, production) {
  await g.addV("GrammarRule")
    .property("name", name)
    .property("production", production || "")
    .next();

  return { added: `GrammarRule: ${name}` };
}

/**
 * Link a Gremlin step to the grammar production that defines it, as a `has_rule`
 * edge (Step -> GrammarRule). Closes the loop `addGrammarRule` opens: the rule
 * vertex exists, this connects the step concept to it. The grammar playbook's
 * `checks.completeness` on `has_rule` reports which steps still lack this link.
 * Both vertices must already exist (`mapStep` creates the Step, `addGrammarRule`
 * the rule).
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {string} stepName - Canonical step name (an existing Step vertex)
 * @param {string} ruleName - Grammar rule name (an existing GrammarRule vertex)
 * @param {string} [confidence] - default INFERRED
 * @returns {Promise<object>}
 */
export async function linkRule(g, stepName, ruleName, confidence) {
  const conf = normalizeConfidence(confidence, CONFIDENCE.INFERRED);
  const stepExists = await g.V().hasLabel("Step").has("name", stepName).hasNext();
  if (!stepExists) {
    return { error: `no Step vertex named "${stepName}" (map it with mapStep first)` };
  }
  const ruleExists = await g.V().hasLabel("GrammarRule").has("name", ruleName).hasNext();
  if (!ruleExists) {
    return { error: `no GrammarRule vertex named "${ruleName}" (add it with addGrammarRule first)` };
  }

  await g.V().hasLabel("Step").has("name", stepName)
    .addE("has_rule")
    .property("confidence", conf)
    .to(__.V().hasLabel("GrammarRule").has("name", ruleName))
    .next();

  return { linked: `${stepName} has_rule ${ruleName}`, confidence: conf };
}

/**
 * Record that a test covers a Gremlin step's behavior, as a `covers` edge
 * (Test -> Step). The `new-step` playbook treats a step with no `covers` as a
 * coverage gap; this is how you record the coverage you find during enrichment.
 * The test is keyed by name AND file because test names repeat across suites;
 * the Step vertex must already exist (created via `mapStep`).
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {string} testName - The covering test's name
 * @param {string} filePath - The test's file (disambiguates same-named tests)
 * @param {string} stepName - Canonical step name (an existing Step vertex)
 * @param {string} [confidence] - default INFERRED
 * @returns {Promise<object>}
 */
export async function mapCoverage(g, testName, filePath, stepName, confidence) {
  const conf = normalizeConfidence(confidence, CONFIDENCE.INFERRED);
  const stepExists = await g.V().hasLabel("Step").has("name", stepName).hasNext();
  if (!stepExists) {
    return { error: `no Step vertex named "${stepName}" (map it with mapStep first)` };
  }
  const testExists = await g.V().hasLabel("Test")
    .has("name", testName).has("filePath", filePath).hasNext();
  if (!testExists) {
    return { error: `no Test vertex "${testName}" in ${filePath}` };
  }

  await g.V().hasLabel("Test").has("name", testName).has("filePath", filePath)
    .addE("covers")
    .property("confidence", conf)
    .to(__.V().hasLabel("Step").has("name", stepName))
    .next();

  return { covered: `${testName} covers ${stepName}`, confidence: conf };
}

/**
 * INTERNAL (Phase 1). Create the root PR Discussion vertex that every other
 * discussion links back to via `addresses`. review.js calls this once while
 * building the graph; it's idempotent (a second call is a no-op). Exposed on the
 * CLI only for manual re-runs — reviewers don't call it during enrichment.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {number|string} pr - PR number (forms the GitHub URL)
 * @param {string} [title] - Defaults to "PR #<pr>"
 * @param {string} [body] - Optional description
 * @returns {Promise<{created: boolean, pr?}>}
 */
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
