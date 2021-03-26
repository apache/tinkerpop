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

test('It should be possible ot use formatQuery with a config, with a partial config and no config', () => {
  // Test using formatQuery with a default config
  expect(
    formatQuery(
      "g.V().has('person', 'name', 'marko').shortestPath().with(ShortestPath.target, __.has('name', 'josh')).with(ShortestPath.distance, 'weight')",
      {
        indentation: 0,
        maxLineLength: 80,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with(ShortestPath.target, __.has('name', 'josh')).
    with(ShortestPath.distance, 'weight')`);

  // Test using formatQuery with a non-default confi
  expect(
    formatQuery(
      "g.V().has('person', 'name', 'marko').shortestPath().with(ShortestPath.target, __.has('name', 'josh')).with(ShortestPath.distance, 'weight')",
      {
        indentation: 8,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: true,
      },
    ),
  ).toBe(`        g.V()
          .has('person', 'name', 'marko')
          .shortestPath()
            .with(
              ShortestPath.target,
              __.has('name', 'josh'))
            .with(ShortestPath.distance, 'weight')`);

  // Test using formatQuery with an empty config
  expect(
    formatQuery(
      "g.V().has('person', 'name', 'marko').shortestPath().with(ShortestPath.target, __.has('name', 'josh')).with(ShortestPath.distance, 'weight')",
      {},
    ),
  ).toBe(`g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with(ShortestPath.target, __.has('name', 'josh')).
    with(ShortestPath.distance, 'weight')`);

  // Test using formatQuery without a config
  expect(
    formatQuery(
      "g.V().has('person', 'name', 'marko').shortestPath().with(ShortestPath.target, __.has('name', 'josh')).with(ShortestPath.distance, 'weight')",
    ),
  ).toBe(`g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with(ShortestPath.target, __.has('name', 'josh')).
    with(ShortestPath.distance, 'weight')`);
});
