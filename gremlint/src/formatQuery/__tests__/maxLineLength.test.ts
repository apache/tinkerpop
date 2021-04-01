/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { formatQuery } from '..';

test('No line in the query should exceed the maximum line length', () => {
  // When the maximum line length is equal to the length of the query, no line wrapping should occur
  expect(
    formatQuery("g.V().hasLabel('person').where(outE('created').count().is(P.gte(2))).count()", {
      indentation: 0,
      maxLineLength: 76,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe("g.V().hasLabel('person').where(outE('created').count().is(P.gte(2))).count()");

  // A query of length 77 should be wrapped when the maximum line length is set to 76
  expect(
    formatQuery("g.V().hasLabel('person').where(outE('created').count().is(P.gte(2))).count()", {
      indentation: 0,
      maxLineLength: 75,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(`g.V().
  hasLabel('person').
  where(outE('created').count().is(P.gte(2))).
  count()`);

  // When wrapping occurs, the parentheses, punctuations or commas after the wrapped tokens should be included when
  // considering whether to further wrap the query. This doesn't currently work, as the following test shows
  // https://issues.apache.org/jira/browse/TINKERPOP-2539
  /*expect(
    formatQuery("g.V().hasLabel('person').where(outE('created').count().is(P.gte(2))).count()", {
      indentation: 0,
      maxLineLength: 45,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(`g.V().
  hasLabel('person').
  where(
    outE('created').count().is(P.gte(2))).
  count()`);*/

  // Test that if the query is wrapped before exceeding the max line length, even if it does not start at the beginning
  // of the line
  expect(
    formatQuery("List<Vertex> people = g.V().hasLabel('person').toList();", {
      indentation: 0,
      maxLineLength: 40,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(`List<Vertex> people = g.V().
  hasLabel('person').
  toList();`);
});
