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

// Shared helpers for symbols and `references` edges. Used by both the Phase-1
// removal-refs pass (patterns/removal-refs.js) and the manual addReference
// escape hatch (enrichment/api.js), so the edge shape lives in one place.

import gremlin from "gremlin";
import { CONFIDENCE } from "./confidence.js";

const { process: { statics: __ } } = gremlin;

const CODE_EXTENSIONS = new Set([
  "java", "py", "js", "ts", "jsx", "tsx", "mjs", "go", "cs", "groovy",
  "kt", "scala", "rb", "rs", "c", "cpp", "h", "hpp",
]);

// The likely symbol a source file defined — its basename without extension
// (Krb5Authenticator.java -> Krb5Authenticator).
export function symbolFromPath(path) {
  const base = path.split("/").pop() || path;
  const dot = base.indexOf(".");
  return dot > 0 ? base.slice(0, dot) : base;
}

export function extOf(path) {
  return path.includes(".") ? path.split(".").pop() : "";
}

export function isCodeFile(path) {
  return CODE_EXTENSIONS.has(extOf(path));
}

/**
 * Create a `references` edge: a surviving file (fromPath) still mentions a symbol
 * from a deleted file (toPath). The source file is often outside the changed set
 * and has no vertex yet, so it is find-or-created as an unparsed marker. Assumes
 * the target (deleted) File vertex already exists.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {object} params
 * @param {string} params.fromPath
 * @param {string} params.toPath
 * @param {string} [params.symbol]
 * @param {string} [params.location]
 * @param {string} [params.confidence] - default INFERRED
 */
export async function createReferenceEdge(g, params) {
  const { fromPath, toPath, symbol, location, confidence = CONFIDENCE.INFERRED } = params;

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
    .property("confidence", confidence)
    .property("symbol", symbol || "")
    .property("location", location || "")
    .to(__.V().hasLabel("File").has("path", toPath))
    .next();
}
