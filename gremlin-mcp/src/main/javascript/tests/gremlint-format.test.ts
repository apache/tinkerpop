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

import { describe, it, expect, beforeAll } from '@jest/globals';

let formatQuery: (q: string, opts?: any) => string;

beforeAll(async () => {
  // Import the compiled gremlint artifact directly from the repository (lib/index.js)
  const mod = await import('../../../../../gremlint/lib/index.js');
  // Resolve across possible interop shapes
  formatQuery =
    (mod && (mod.formatQuery as any)) ||
    (mod && mod.default && (mod.default.formatQuery as any)) ||
    (mod && (mod.default as any))?.formatQuery ||
    (mod as any);

  if (!formatQuery || typeof formatQuery !== 'function') {
    throw new Error(
      'Could not resolve formatQuery from gremlint module. Please build gremlint first.'
    );
  }
});

describe('gremlint formatQuery', () => {
  const sample = `g.V().hasLabel('person').has('name','marko').out('knows').values('name')`;

  it('formats and returns a different string that contains newlines', () => {
    const formatted = formatQuery(sample, {
      indentation: 2,
      maxLineLength: 60,
      shouldPlaceDotsAfterLineBreaks: true,
    });
    expect(typeof formatted).toBe('string');
    expect(formatted).not.toBe(sample);
    expect(formatted.includes('\n')).toBe(true);
  });

  it('produces different output for different indentation options', () => {
    const f2 = formatQuery(sample, { indentation: 2 });
    const f4 = formatQuery(sample, { indentation: 4 });
    expect(f2).not.toBe(f4);

    const secondLine2 = f2.split('\n')[1] || '';
    const secondLine4 = f4.split('\n')[1] || '';

    const leadingSpaces = (s: string) => s.match(/^ */)?.[0].length || 0;
    expect(leadingSpaces(secondLine2)).toBeLessThanOrEqual(leadingSpaces(secondLine4));
  });
});
