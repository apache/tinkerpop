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

// TODO: These tests seem related to those in closureIndentation.test.ts, so it might be worth checking if they can be merged.
test('Both top-level and inlined non-Gremlin code should be indented together with the Gremlin query', () => {
  // Test that top-level and inlined non-Gremlin code are not indented when indentation is 0
  expect(
    formatQuery(
      `hasField = { field -> __.has(field) }

profitQuery = g.V().
filter(hasField('sell_price')).
filter(hasField('buy_price')).
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
  ).toBe(`hasField = { field -> __.has(field) }

profitQuery = g.V().
  filter(hasField('sell_price')).
  filter(hasField('buy_price')).
  project('product', 'profit').
    by('name').
    by{ it.get().value('sell_price') -
        it.get().value('buy_price') };`);

  // Test that top-level and inlined non-Gremlin code are not indented when indentation is 20
  expect(
    formatQuery(
      `hasField = { field -> __.has(field) }
      
profitQuery = g.V().
filter(hasField('sell_price')).
filter(hasField('buy_price')).
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
  ).toBe(`                    hasField = { field -> __.has(field) }

                    profitQuery = g.V().
                      filter(hasField('sell_price')).
                      filter(hasField('buy_price')).
                      project('product', 'profit').
                        by('name').
                        by{ it.get().value('sell_price') -
                            it.get().value('buy_price') };`);
});
