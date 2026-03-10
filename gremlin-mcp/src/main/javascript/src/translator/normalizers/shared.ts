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
 * @fileoverview Mechanical pre-normalization transforms applied before LLM normalization.
 *
 * These transforms are cheap, deterministic, and language-agnostic. They reduce noise
 * before the LLM sees the query:
 *   1. Strip gremlingo. prefix (Go dialect)
 *   2. Strip trailing _ from known step names (reserved word workarounds)
 *   3. Convert PascalCase step names to camelCase (Go/.NET dialects)
 */

import { GREMLIN_STEP_NAMES, GREMLIN_STEP_NAMES_PASCAL } from '../stepNames.js';

/**
 * Applies mechanical pre-normalization to a Gremlin query string.
 * Safe to apply to any dialect including canonical — transforms are idempotent.
 */
export function applyMechanicalNormalization(query: string): string {
  let result = query;

  // 1. Strip gremlingo. prefix
  result = result.replace(/gremlingo\./g, '');

  // 2. Strip trailing _ from known step names before (
  //    e.g. in_( → in(, from_( → from(
  //    Only applies to names in GREMLIN_STEP_NAMES to avoid corrupting user identifiers.
  result = result.replace(/\b(\w+)_\s*\(/g, (_match, name: string) => {
    if (GREMLIN_STEP_NAMES.has(name)) {
      return `${name}(`;
    }
    return _match;
  });

  // 3. Convert PascalCase step names → camelCase
  //    e.g. HasLabel( → hasLabel(, Out( → out(
  //    Only converts tokens where (a) the PascalCase form is in GREMLIN_STEP_NAMES_PASCAL and
  //    (b) the resulting camelCase form is in GREMLIN_STEP_NAMES. The second check handles
  //    steps like V and E whose canonical form is already uppercase — converting them to
  //    v( or e( would be wrong.
  result = result.replace(/\b([A-Z][a-zA-Z0-9]*)\s*\(/g, (_match, name: string) => {
    if (GREMLIN_STEP_NAMES_PASCAL.has(name)) {
      const camel = `${name.charAt(0).toLowerCase()}${name.slice(1)}`;
      if (GREMLIN_STEP_NAMES.has(camel)) {
        return `${camel}(`;
      }
    }
    return _match;
  });

  return result;
}
