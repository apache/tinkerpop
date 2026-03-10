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
 * @fileoverview LLM-based normalization of Gremlin queries to canonical format.
 *
 * Uses MCP sampling to request normalization from the host LLM client, requiring
 * no API key and no dependency on any specific LLM provider.
 */

import type { Server } from '@modelcontextprotocol/sdk/server/index.js';

const SYSTEM_PROMPT = `You are a Gremlin query normalizer. Convert the given Gremlin query to canonical gremlin-language ANTLR grammar format.

Canonical format rules:
- Step names are camelCase: out(), hasLabel(), values() — not PascalCase
- Boolean literals are lowercase: true, false — not True, False
- Null literal is: null — not None
- No language-specific prefixes (gremlingo. has already been stripped)
- Enum values use canonical casing: T.label, T.id, T.key, T.value; Direction.OUT, Direction.IN, Direction.BOTH; Order.asc, Order.desc, Order.shuffle; Scope.local, Scope.global; Pop.first, Pop.last, Pop.all, Pop.mixed; Merge.onCreate, Merge.onMatch, Merge.outV, Merge.inV; Cardinality.single, Cardinality.list, Cardinality.set
- No type casts: use null instead of (Map) null, (Object) null, (String) null, etc.
- Collection literals: [a, b, c] for lists, {a, b, c} for sets, ["key": value] for maps
- Date literals: datetime("2023-01-01T00:00:00Z") — not OffsetDateTime.parse(...)
- UUID literals: uuid("...") or uuid() — not UUID.fromString(...) or UUID.randomUUID()
- No Java collection constructors (new ArrayList<>(), new HashSet<>(), new LinkedHashMap<>()) — use list/set/map literals

Return ONLY the normalized Gremlin query. No explanation, no markdown, no surrounding text.`;

/**
 * Normalizes a Gremlin query to canonical format using MCP sampling.
 * Delegates to the host LLM client — no API key or vendor dependency required.
 *
 * @param query - Pre-processed Gremlin query (after mechanical normalization)
 * @param server - MCP server instance used to issue the sampling request
 * @returns Normalized canonical Gremlin query string
 * @throws If the client does not support sampling or the response is not text
 */
export async function normalizeWithLlm(query: string, server: Server): Promise<string> {
  const result = await server.createMessage({
    messages: [{ role: 'user', content: { type: 'text', text: query } }],
    systemPrompt: SYSTEM_PROMPT,
    maxTokens: 1024,
  });

  if (result.content.type !== 'text') {
    throw new Error('Unexpected response type from LLM normalization');
  }

  return result.content.text.trim();
}
