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

test('When specifying indentation for a query, the relative indentation within closures should be preserved', () => {
  // Test that relative indentation is preserved between all the lines within a closure when indentation is 0
  expect(
    formatQuery(
      `g.V().
has('sell_price').
has('buy_price').
project('product', 'profit').
by('name').
by{ it.get().value('sell_price') -
    it.get().value('buy_price') };`,
      {
        indentation: 0,
        maxLineLength: 70,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  has('sell_price').
  has('buy_price').
  project('product', 'profit').
    by('name').
    by{ it.get().value('sell_price') -
        it.get().value('buy_price') };`);

  // Test that relative indentation is preserved between all the lines within a closure when indentation is 20
  expect(
    formatQuery(
      `g.V().
has('sell_price').
has('buy_price').
project('product', 'profit').
by('name').
by{ it.get().value('sell_price') -
    it.get().value('buy_price') };`,
      {
        indentation: 20,
        maxLineLength: 70,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`                    g.V().
                      has('sell_price').
                      has('buy_price').
                      project('product', 'profit').
                        by('name').
                        by{ it.get().value('sell_price') -
                            it.get().value('buy_price') };`);

  // Test that relative indentation is preserved in closures which are nested
  expect(
    formatQuery(
      `g.V().filter(out('Sells').
             map{ it.get('sell_price') -
                  it.get('buy_price') }.
             where(gt(50)))`,
      { indentation: 0, maxLineLength: 45, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(
    `g.V().
  filter(
    out('Sells').
    map{ it.get('sell_price') -
         it.get('buy_price') }.
    where(gt(50)))`,
  );

  expect(
    formatQuery(
      `g.V().filter(map{ one   = 1
                  two   = 2
                  three = 3 }))`,
      { indentation: 0, maxLineLength: 35, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V().filter(map{ one   = 1
                  two   = 2
                  three = 3 }))`);
  expect(
    formatQuery(
      `g.V().filter(map{ one   = 1
                  two   = 2
                  three = 3 }))`,
      { indentation: 0, maxLineLength: 28, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V().
  filter(map{ one   = 1
              two   = 2
              three = 3 }))`);
  expect(
    formatQuery(
      `g.V().filter(map{ one   = 1
                  two   = 2
                  three = 3 }))`,
      { indentation: 0, maxLineLength: 22, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V().
  filter(
    map{ one   = 1
         two   = 2
         three = 3 }))`);

  expect(
    formatQuery(
      `g.V().where(map{ buyPrice  = it.get().value('buy_price');
                 sellPrice = it.get().value('sell_price');
                 sellPrice - buyPrice; }.is(gt(50)))`,
      { indentation: 0, maxLineLength: 60, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V().where(map{ buyPrice  = it.get().value('buy_price');
                 sellPrice = it.get().value('sell_price');
                 sellPrice - buyPrice; }.is(gt(50)))`);
  expect(
    formatQuery(
      `g.V().where(map{ buyPrice  = it.get().value('buy_price');
                 sellPrice = it.get().value('sell_price');
                 sellPrice - buyPrice; }.is(gt(50)))`,
      { indentation: 0, maxLineLength: 50, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V().
  where(map{ buyPrice  = it.get().value('buy_price');
             sellPrice = it.get().value('sell_price');
             sellPrice - buyPrice; }.is(gt(50)))`);
  expect(
    formatQuery(
      `g.V().where(map{ buyPrice  = it.get().value('buy_price');
                 sellPrice = it.get().value('sell_price');
                 sellPrice - buyPrice; }.is(gt(50)))`,
      { indentation: 0, maxLineLength: 45, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V().
  where(
    map{ buyPrice  = it.get().value('buy_price');
         sellPrice = it.get().value('sell_price');
         sellPrice - buyPrice; }.is(gt(50)))`);

  expect(
    formatQuery(
      `g.V().where(out().map{ buyPrice  = it.get().value('buy_price');
                       sellPrice = it.get().value('sell_price');
                       sellPrice - buyPrice; }.is(gt(50)))`,
      { indentation: 0, maxLineLength: 60, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V().where(out().map{ buyPrice  = it.get().value('buy_price');
                       sellPrice = it.get().value('sell_price');
                       sellPrice - buyPrice; }.is(gt(50)))`);
  expect(
    formatQuery(
      `g.V().where(out().map{ buyPrice  = it.get().value('buy_price');
                       sellPrice = it.get().value('sell_price');
                       sellPrice - buyPrice; }.is(gt(50)))`,
      { indentation: 0, maxLineLength: 55, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V().
  where(out().map{ buyPrice  = it.get().value('buy_price');
                   sellPrice = it.get().value('sell_price');
                   sellPrice - buyPrice; }.is(gt(50)))`);
  expect(
    formatQuery(
      `g.V().where(out().map{ buyPrice  = it.get().value('buy_price');
                       sellPrice = it.get().value('sell_price');
                       sellPrice - buyPrice; }.is(gt(50)))`,
      { indentation: 0, maxLineLength: 50, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V().
  where(
    out().map{ buyPrice  = it.get().value('buy_price');
               sellPrice = it.get().value('sell_price');
               sellPrice - buyPrice; }.is(gt(50)))`);
  expect(
    formatQuery(
      `g.V().where(out().map{ buyPrice  = it.get().value('buy_price');
                       sellPrice = it.get().value('sell_price');
                       sellPrice - buyPrice; }.is(gt(50)))`,
      { indentation: 0, maxLineLength: 45, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V().
  where(
    out().
    map{ buyPrice  = it.get().value('buy_price');
         sellPrice = it.get().value('sell_price');
         sellPrice - buyPrice; }.
    is(gt(50)))`);

  // Test that relative indentation is preserved between all the lines within a closure when not all tokens in a stepGroup are methods (for instance, g in g.V() adds to the width of the stepGroup even if it is not a method)
  expect(
    formatQuery(
      `g.V().map({ it.get('sell_price') -
            it.get('buy_price') }))`,
      {
        indentation: 0,
        maxLineLength: 35,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().map({ it.get('sell_price') -
            it.get('buy_price') }))`);

  // Test that relative indentation is preserved between all the lines within a closure when the first line is indented because the query doesn't start at the beginning of the line
  expect(
    formatQuery(
      `profit = g.V().map({ it.get('sell_price') -
                     it.get('buy_price') }))`,
      {
        indentation: 0,
        maxLineLength: 45,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`profit = g.V().map({ it.get('sell_price') -
                     it.get('buy_price') }))`);

  // Test that relative indentation is preserved between all lines within a closure when the method to which the closure is an argument is wrapped
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
      { indentation: 0, maxLineLength: 40, shouldPlaceDotsAfterLineBreaks: false },
    ),
  ).toBe(`g.V(ids).
  has('factor_a').
  has('factor_b').
  project(
    'Factor A',
    'Factor B',
    'Product').
    by(values('factor_a')).
    by(values('factor_b')).
    by(
      map{ it.get().value('factor_a') *
           it.get().value('factor_b') })`);

  // Test that relative indentation is preserved between all lines within a closure when dots are placed after line breaks
  // When the whole query is long enough to wrap
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
      { indentation: 0, maxLineLength: 45, shouldPlaceDotsAfterLineBreaks: true },
    ),
  ).toBe(`g.V(ids)
  .has('factor_a')
  .has('factor_b')
  .project('Factor A', 'Factor B', 'Product')
    .by(values('factor_a'))
    .by(values('factor_b'))
    .by(map{ it.get().value('factor_a') *
             it.get().value('factor_b') })`);

  // When the query is long enough to wrap, but the traversal containing the closure is not the first step in its step group and not long enough to wrap
  expect(
    formatQuery(
      `g.V().where(out().map{ buyPrice  = it.get().value('buy_price');
                       sellPrice = it.get().value('sell_price');
                       sellPrice - buyPrice; })`,
      { indentation: 0, maxLineLength: 50, shouldPlaceDotsAfterLineBreaks: true },
    ),
  ).toBe(`g.V().where(out().map{ buyPrice  = it.get().value('buy_price');
                       sellPrice = it.get().value('sell_price');
                       sellPrice - buyPrice; })`);

  // When the query is long enough to wrap, but the traversal containing the closure is the first step in its step group and not long enough to wrap
  expect(
    formatQuery(
      `g.V().where(out().map{ buyPrice  = it.get().value('buy_price');
                       sellPrice = it.get().value('sell_price');
                       sellPrice - buyPrice; }.is(gt(50)))`,
      { indentation: 0, maxLineLength: 45, shouldPlaceDotsAfterLineBreaks: true },
    ),
  ).toBe(`g.V()
  .where(
    out()
    .map{ buyPrice  = it.get().value('buy_price');
          sellPrice = it.get().value('sell_price');
          sellPrice - buyPrice; }
    .is(gt(50)))`);

  // When the query is long enough to wrap, but the traversal containing the closure is the first step in its traversal and not long enough to wrap
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
      { indentation: 0, maxLineLength: 40, shouldPlaceDotsAfterLineBreaks: true },
    ),
  ).toBe(`g.V(ids)
  .has('factor_a')
  .has('factor_b')
  .project(
    'Factor A',
    'Factor B',
    'Product')
    .by(values('factor_a'))
    .by(values('factor_b'))
    .by(
      map{ it.get().value('factor_a') *
           it.get().value('factor_b') })`);

  // When the whole query is short enough to not wrap
  expect(
    formatQuery(
      `g.V().map({ it.get('sell_price') -
            it.get('buy_price') }))`,
      {
        indentation: 0,
        maxLineLength: 35,
        shouldPlaceDotsAfterLineBreaks: true,
      },
    ),
  ).toBe(`g.V().map({ it.get('sell_price') -
            it.get('buy_price') }))`);
});
