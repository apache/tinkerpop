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

test('When determining if multiline curly bracket closures should cause wrapping, look only at the longest line of the code block', () => {
  // Test that when moving multiline code blocks, we move all the lines of the code block, not just the first
  expect(
    formatQuery(
      `g.V(1).out().map{ it.get()
                    .value('name') }`,
      {
        indentation: 0,
        maxLineLength: 25,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V(1).
  out().
  map{ it.get()
         .value('name') }`);

  expect(
    formatQuery(
      `g.V().filter{ it.get()
                .label() == 'person' }`,
      {
        indentation: 0,
        maxLineLength: 35,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  filter{ it.get()
            .label() == 'person' }`);

  expect(
    formatQuery(
      `g.V().
branch{ it.get()
          .value('name') }.
option('marko', values('age')).
option(none, values('name'))`,
      {
        indentation: 0,
        maxLineLength: 35,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  branch{ it.get()
            .value('name') }.
    option('marko', values('age')).
    option(none, values('name'))`);

  expect(
    formatQuery(
      `g.V(4).
out().
values('name').
inject('daniel').
map{ it.get()
       .length() }.
path()`,
      {
        indentation: 0,
        maxLineLength: 25,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V(4).
  out().
  values('name').
  inject('daniel').
  map{ it.get()
         .length() }.
  path()`);

  expect(
    formatQuery(
      `g.V().
filter{ it.get()
          .value('name') == 'marko' }.
flatMap{ it.get()
           .vertices(OUT,'created') }.
map{ it.get()
       .value('name') }`,
      {
        indentation: 0,
        maxLineLength: 40,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  filter{ it.get()
            .value('name') == 'marko' }.
  flatMap{ it.get()
             .vertices(OUT,'created') }.
  map{ it.get()
         .value('name') }`);

  expect(
    formatQuery(
      `g.V().
out().
out().
path().
by{ it.value('name') }.
by{ it.value('name') }.
by{ g.V(it).
      in('created').
      values('name').
      fold().next() }`,
      {
        indentation: 0,
        maxLineLength: 30,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  out().
  out().
  path().
    by{ it.value('name') }.
    by{ it.value('name') }.
    by{ g.V(it).
          in('created').
          values('name').
          fold().next() }`);

  expect(
    formatQuery(
      `g.V(ids).
has('factor_a').
has('factor_b').
project('Factor A', 'Factor B', 'Product').
by(values('factor_a')).
by(values('factor_b')).
by(map{ it.get().value('factor_a') * 
        it.get().value('factor_b') })`,
      {
        indentation: 0,
        maxLineLength: 45,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V(ids).
  has('factor_a').
  has('factor_b').
  project('Factor A', 'Factor B', 'Product').
    by(values('factor_a')).
    by(values('factor_b')).
    by(map{ it.get().value('factor_a') * 
            it.get().value('factor_b') })`);
});
