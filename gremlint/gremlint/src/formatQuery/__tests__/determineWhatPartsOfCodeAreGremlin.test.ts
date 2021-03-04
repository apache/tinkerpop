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

test('Extract the parts of the code that can be parsed as Gremlin, format those separately, and leave the rest of the code alone', () => {
  expect(
    formatQuery(
      `contains = {
  value -> it.get().contains(value)
}

g.V().filter(values('name').filter(contains('Gremlint')))`,
      {
        indentation: 0,
        maxLineLength: 35,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`contains = {
  value -> it.get().contains(value)
}

g.V().
  filter(
    values('name').
    filter(contains('Gremlint')))`);

  expect(
    formatQuery(
      `      g.V(ids).
     has('factor_a').
    has('factor_b').
   project('Factor A', 'Factor B', 'Product').
  by(values('factor_a')).
 by(values('factor_b')).
by(map({ it.get().value('factor_a') *
         it.get().value('factor_b') }))`,
      {
        indentation: 0,
        maxLineLength: 72,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`      g.V(ids).
  has('factor_a').
  has('factor_b').
  project('Factor A', 'Factor B', 'Product').
    by(values('factor_a')).
    by(values('factor_b')).
    by(map({ it.get().value('factor_a') *
             it.get().value('factor_b') }))`);

  expect(
    formatQuery(
      `a = 4.5;
b = 4.5;

g.V(ids).
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
  ).toBe(`a = 4.5;
b = 4.5;

g.V(ids).
  has('factor_a').
  has('factor_b').
  project('Factor A', 'Factor B', 'Product').
    by(values('factor_a')).
    by(values('factor_b')).
    by(map{ it.get().value('factor_a') *
            it.get().value('factor_b') })`);

  expect(
    formatQuery(
      `g.V(ids).
has('factor_a').
has('factor_b').
project('Factor A', 'Factor B', 'Product').
by(values('factor_a')).
by(values('factor_b')).
by(map{ it.get().value('factor_a') *
        it.get().value('factor_b') });`,
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
            it.get().value('factor_b') });`);
});
