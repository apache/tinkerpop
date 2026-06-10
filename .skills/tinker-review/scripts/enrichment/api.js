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

// === Write operations ===

export async function mapStep(g, functionName, filePath, canonicalStepName) {
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
    .to(__.V().hasLabel("Step").has("name", canonicalStepName))
    .next();

  return { mapped: `${functionName} -> ${canonicalStepName}` };
}

export async function linkDiscussion(g, url, source, title, body) {
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
      .to(__.V().hasLabel("Discussion").has("url", url))
      .next();
  }

  return { linked: `${source}: ${title}` };
}

export async function annotate(g, label, name, key, value) {
  await g.V().hasLabel(label).has("name", name)
    .property(key, value)
    .next();

  return { annotated: `${label}:${name}.${key} = ${value}` };
}

export async function linkDoc(g, entityLabel, entityName, docPath, section) {
  const docExists = await g.V().hasLabel("Doc").has("path", docPath).hasNext();
  if (!docExists) {
    await g.addV("Doc")
      .property("path", docPath)
      .property("section", section || "")
      .next();
  }

  await g.V().hasLabel("Doc").has("path", docPath)
    .addE("documents")
    .to(__.V().hasLabel(entityLabel).has("name", entityName))
    .next();

  return { linked: `${docPath} documents ${entityLabel}:${entityName}` };
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
