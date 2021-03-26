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

test('It should be possible to call curly bracket closures which are not wrapped in parentheses', () => {
  // Test calling closure whose curly brackets are wrapped in parentheses
  expect(
    formatQuery(
      "g.V().branch({ it.get().value('name') }).option('marko', values('age')).option(none, values('name'))",
      {
        indentation: 0,
        maxLineLength: 40,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  branch({ it.get().value('name') }).
    option('marko', values('age')).
    option(none, values('name'))`);

  // Test calling closure which is not the last step
  expect(
    formatQuery("g.V().branch{ it.get().value('name') }.option('marko', values('age')).option(none, values('name'))", {
      indentation: 0,
      maxLineLength: 35,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(`g.V().
  branch{ it.get().value('name') }.
    option('marko', values('age')).
    option(none, values('name'))`);
  expect(
    formatQuery("g.V().hasLabel('person').outE('created').count().map{ it.get() * 10 }.path()", {
      indentation: 0,
      maxLineLength: 25,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(`g.V().
  hasLabel('person').
  outE('created').
  count().
  map{ it.get() * 10 }.
  path()`);

  // Test calling closure which is the last step
  expect(
    formatQuery("g.V(4).out().values('name').inject('daniel').map{ it.get().length() }", {
      indentation: 0,
      maxLineLength: 30,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(`g.V(4).
  out().
  values('name').
  inject('daniel').
  map{ it.get().length() }`);

  // Test calling closure whose subsequent step should not wrap
  expect(
    formatQuery("g.V(4).out().values('name').inject('daniel').map{ it.get().length() }.path()", {
      indentation: 0,
      maxLineLength: 80,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(`g.V(4).out().values('name').inject('daniel').map{ it.get().length() }.path()`);

  // Test calling consecutive closures
  expect(
    formatQuery(
      "g.V().filter{ it.get().value('name') == 'marko' }.flatMap{ it.get().vertices(OUT, 'created') }.map{ it.get().value('name') }",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  filter{ it.get().value('name') == 'marko' }.
  flatMap{ it.get().vertices(OUT, 'created') }.
  map{ it.get().value('name') }`);

  // Test calling closure by()-modulator
  expect(
    formatQuery("g.V().group().by{ it.value('name')[1] }.by('name').next()", {
      indentation: 0,
      maxLineLength: 30,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(`g.V().
  group().
    by{ it.value('name')[1] }.
    by('name').
  next()`);
});
