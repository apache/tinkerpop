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

test('Each of multiple queries formatted at once should be formatted as if they were formatted individually', () => {
  // Test that linebreaks don't happen too soon because the formatter fails to distinguish between lines from the end
  // of one query and the start of the next
  expect(
    formatQuery(
      `g.V(1).out().values('name')

g.V(1).out().map{ it.get().value('name') }

g.V(1).out().map(values('name'))`,
      {
        indentation: 0,
        maxLineLength: 45,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V(1).out().values('name')

g.V(1).out().map{ it.get().value('name') }

g.V(1).out().map(values('name'))`);

  expect(
    formatQuery(
      `g.V().branch{ it.get().value('name') }.option('marko', values('age')).option(none, values('name'))

g.V().branch(values('name')).option('marko', values('age')).option(none, values('name'))

g.V().choose(has('name','marko'),values('age'), values('name'))`,
      {
        indentation: 0,
        maxLineLength: 70,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  branch{ it.get().value('name') }.
    option('marko', values('age')).
    option(none, values('name'))

g.V().
  branch(values('name')).
    option('marko', values('age')).
    option(none, values('name'))

g.V().choose(has('name', 'marko'), values('age'), values('name'))`);
});
