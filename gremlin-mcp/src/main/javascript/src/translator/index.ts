/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

/**
 * @fileoverview Orchestrates the normalize-and-translate pipeline.
 *
 * Pipeline:
 *   1. Mechanical pre-processing (gremlingo. strip, trailing _, PascalCase→camelCase)
 *   2. LLM normalization via MCP sampling (handles remaining dialect-specific constructs)
 *   3. GremlinTranslator.translate() to the target language
 */

import type { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { GremlinTranslator } from 'gremlin-language/language';
import { applyMechanicalNormalization } from './normalizers/shared.js';
import { normalizeWithLlm } from './llm.js';

export interface NormalizeAndTranslateResult {
  readonly original: string;
  readonly normalized: string;
  readonly translated: string;
  readonly llmNormalizationSkipped: boolean;
}

/**
 * Normalizes a Gremlin query from any dialect to canonical format, then translates
 * it to the specified target language.
 *
 * @param query - Input Gremlin query in any dialect
 * @param target - Translator key (e.g. 'JAVASCRIPT', 'PYTHON')
 * @param traversalSource - Traversal source variable name (default: 'g')
 * @param server - MCP server instance for LLM sampling
 * @returns Original, normalized, and translated query strings
 */
export async function normalizeAndTranslate(
  query: string,
  target: string,
  traversalSource: string,
  server: Server
): Promise<NormalizeAndTranslateResult> {
  const mechanical = applyMechanicalNormalization(query);
  const [normalized, llmNormalizationSkipped] = await normalizeWithLlm(mechanical, server)
    .then((result): [string, boolean] => [result, false])
    .catch((): [string, boolean] => [mechanical, true]);
  const result = GremlinTranslator.translate(
    normalized,
    traversalSource,
    target as Parameters<typeof GremlinTranslator.translate>[2]
  );
  return {
    original: query,
    normalized,
    translated: result.getTranslated(),
    llmNormalizationSkipped,
  };
}
