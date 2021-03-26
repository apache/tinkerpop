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

// If modulators have to be wrapped, they should be indented with two additional spaces, but consecutive steps should
// not be indented with two additional spaces. Check that as-steps are indented as modulators.
test('Wrapped modulators should be indented with two spaces', () => {
  // Test as()-modulator indentation
  expect(
    formatQuery(
      "g.V().has('name', within('marko', 'vadas', 'josh')).as('person').V().has('name', within('lop', 'ripple')).addE('uses').from('person')",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  has('name', within('marko', 'vadas', 'josh')).
    as('person').
  V().
  has('name', within('lop', 'ripple')).
  addE('uses').from('person')`);

  // Test as_()-modulator indentation
  expect(
    formatQuery(
      "g.V().has('name', within('marko', 'vadas', 'josh')).as_('person').V().has('name', within('lop', 'ripple')).addE('uses').from('person')",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  has('name', within('marko', 'vadas', 'josh')).
    as_('person').
  V().
  has('name', within('lop', 'ripple')).
  addE('uses').from('person')`);

  // Test by()-modulator indentation
  expect(
    formatQuery(
      "g.V().hasLabel('person').group().by(values('name', 'age').fold()).unfold().filter(select(values).count(local).is(gt(1)))",
      {
        indentation: 0,
        maxLineLength: 40,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  hasLabel('person').
  group().
    by(values('name', 'age').fold()).
  unfold().
  filter(
    select(values).
    count(local).
    is(gt(1)))`);
  expect(
    formatQuery(
      "g.V().hasLabel('person').groupCount().by(values('age').choose(is(lt(28)),constant('young'),choose(is(lt(30)), constant('old'), constant('very old'))))",
      {
        indentation: 0,
        maxLineLength: 80,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(`g.V().
  hasLabel('person').
  groupCount().
    by(
      values('age').
      choose(
        is(lt(28)),
        constant('young'),
        choose(is(lt(30)), constant('old'), constant('very old'))))`);

  // Test emit()-modulator indentation
  expect(
    formatQuery("g.V(1).repeat(bothE('created').dedup().otherV()).emit().path()", {
      indentation: 0,
      maxLineLength: 45,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V(1).
  repeat(bothE('created').dedup().otherV()).
    emit().
  path()`,
  );
  expect(
    formatQuery('g.V().repeat(both()).times(1000000).emit().range(6,10)', {
      indentation: 0,
      maxLineLength: 35,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V().
  repeat(both()).
    times(1000000).
    emit().
  range(6, 10)`,
  );
  expect(
    formatQuery("g.V(1).repeat(out()).times(2).emit().path().by('name')", {
      indentation: 0,
      maxLineLength: 30,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V(1).
  repeat(out()).
    times(2).
    emit().
  path().by('name')`,
  );
  expect(
    formatQuery("g.withSack(1).V(1).repeat(sack(sum).by(constant(1))).times(10).emit().sack().math('sin _')", {
      indentation: 0,
      maxLineLength: 40,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.withSack(1).
  V(1).
  repeat(sack(sum).by(constant(1))).
    times(10).
    emit().
  sack().
  math('sin _')`,
  );

  // Test from()-modulator indentation
  expect(
    formatQuery(
      "g.V().has('person','name','vadas').as('e').in('knows').as('m').out('knows').where(neq('e')).path().from('m').by('name')",
      {
        indentation: 0,
        maxLineLength: 20,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  has(
    'person',
    'name',
    'vadas').
    as('e').
  in('knows').
    as('m').
  out('knows').
  where(neq('e')).
  path().
    from('m').
    by('name')`,
  );

  // Test from()-modulator indentation
  expect(
    formatQuery(
      "g.V().has('person','name','vadas').as_('e').in('knows').as_('m').out('knows').where(neq('e')).path().from_('m').by('name')",
      {
        indentation: 0,
        maxLineLength: 20,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  has(
    'person',
    'name',
    'vadas').
    as_('e').
  in('knows').
    as_('m').
  out('knows').
  where(neq('e')).
  path().
    from_('m').
    by('name')`,
  );

  // Test option()-modulator indentation
  expect(
    formatQuery(
      "g.V().hasLabel('person').choose(values('name')).option('marko', values('age')).option('josh', values('name')).option('vadas', elementMap()).option('peter', label())",
      {
        indentation: 0,
        maxLineLength: 80,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  choose(values('name')).
    option('marko', values('age')).
    option('josh', values('name')).
    option('vadas', elementMap()).
    option('peter', label())`,
  );

  // Test read()-modulator indentation
  expect(
    formatQuery('g.io(someInputFile).read().iterate()', {
      indentation: 0,
      maxLineLength: 20,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.io(someInputFile).
    read().
  iterate()`,
  );

  // Test times()-modulator indentation
  expect(
    formatQuery("g.V().repeat(both()).times(3).values('age').max()", {
      indentation: 0,
      maxLineLength: 20,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V().
  repeat(both()).
    times(3).
  values('age').
  max()`,
  );

  // Test to()-modulator indentation
  expect(
    formatQuery("g.V(v1).addE('knows').to(v2).property('weight',0.75).iterate()", {
      indentation: 0,
      maxLineLength: 20,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V(v1).
  addE('knows').
    to(v2).
  property(
    'weight',
    0.75).
  iterate()`,
  );

  // Test until()-modulator indentation
  expect(
    formatQuery(
      "g.V(6).repeat('a', both('created').simplePath()).emit(repeat('b', both('knows')).until(loops('b').as('b').where(loops('a').as('b'))).hasId(2)).dedup()",
      {
        indentation: 0,
        maxLineLength: 45,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V(6).
  repeat('a', both('created').simplePath()).
    emit(
      repeat('b', both('knows')).
        until(
          loops('b').as('b').
          where(loops('a').as('b'))).
      hasId(2)).
  dedup()`,
  );

  // Test with()-modulator indentation
  expect(
    formatQuery(
      "g.V().connectedComponent().with(ConnectedComponent.propertyName, 'component').project('name','component').by('name').by('component')",
      {
        indentation: 0,
        maxLineLength: 55,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  connectedComponent().
    with(ConnectedComponent.propertyName, 'component').
  project('name', 'component').
    by('name').
    by('component')`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('person').connectedComponent().with(ConnectedComponent.propertyName, 'component').with(ConnectedComponent.edges, outE('knows')).project('name','component').by('name').by('component')",
      {
        indentation: 0,
        maxLineLength: 55,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  connectedComponent().
    with(ConnectedComponent.propertyName, 'component').
    with(ConnectedComponent.edges, outE('knows')).
  project('name', 'component').
    by('name').
    by('component')`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('software').values('name').fold().order(Scope.local).index().with(WithOptions.indexer, WithOptions.list).unfold().order().by(__.tail(Scope.local, 1))",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('software').
  values('name').
  fold().
  order(Scope.local).
  index().
    with(WithOptions.indexer, WithOptions.list).
  unfold().
  order().by(__.tail(Scope.local, 1))`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('person').values('name').fold().order(Scope.local).index().with(WithOptions.indexer, WithOptions.map)",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  values('name').
  fold().
  order(Scope.local).
  index().
    with(WithOptions.indexer, WithOptions.map)`,
  );
  expect(
    formatQuery('g.io(someInputFile).with(IO.reader, IO.graphson).read().iterate()', {
      indentation: 0,
      maxLineLength: 35,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.io(someInputFile).
    with(IO.reader, IO.graphson).
    read().
  iterate()`,
  );
  expect(
    formatQuery('g.io(someOutputFile).with(IO.writer,IO.graphml).write().iterate()', {
      indentation: 0,
      maxLineLength: 35,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.io(someOutputFile).
    with(IO.writer, IO.graphml).
    write().
  iterate()`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('person').pageRank().with(PageRank.edges, __.outE('knows')).with(PageRank.propertyName, 'friendRank').order().by('friendRank',desc).elementMap('name','friendRank')",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  pageRank().
    with(PageRank.edges, __.outE('knows')).
    with(PageRank.propertyName, 'friendRank').
  order().by('friendRank', desc).
  elementMap('name', 'friendRank')`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('person').peerPressure().with(PeerPressure.propertyName, 'cluster').group().by('cluster').by('name')",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  peerPressure().
    with(PeerPressure.propertyName, 'cluster').
  group().by('cluster').by('name')`,
  );
  expect(
    formatQuery("g.V().shortestPath().with(ShortestPath.target, __.has('name','peter'))", {
      indentation: 0,
      maxLineLength: 55,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V().
  shortestPath().
    with(ShortestPath.target, __.has('name', 'peter'))`,
  );
  expect(
    formatQuery(
      "g.V().shortestPath().with(ShortestPath.edges, Direction.IN).with(ShortestPath.target, __.has('name','josh'))",
      {
        indentation: 0,
        maxLineLength: 55,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  shortestPath().
    with(ShortestPath.edges, Direction.IN).
    with(ShortestPath.target, __.has('name', 'josh'))`,
  );
  expect(
    formatQuery("g.V().has('person','name','marko').shortestPath().with(ShortestPath.target,__.has('name','josh'))", {
      indentation: 0,
      maxLineLength: 55,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with(ShortestPath.target, __.has('name', 'josh'))`,
  );
  expect(
    formatQuery(
      "g.V().has('person','name','marko').shortestPath().with(ShortestPath.target, __.has('name','josh')).with(ShortestPath.distance, 'weight')",
      {
        indentation: 0,
        maxLineLength: 55,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with(ShortestPath.target, __.has('name', 'josh')).
    with(ShortestPath.distance, 'weight')`,
  );
  expect(
    formatQuery(
      "g.V().has('person','name','marko').shortestPath().with(ShortestPath.target, __.has('name','josh')).with(ShortestPath.includeEdges, true)",
      {
        indentation: 0,
        maxLineLength: 55,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with(ShortestPath.target, __.has('name', 'josh')).
    with(ShortestPath.includeEdges, true)`,
  );
  expect(
    formatQuery(
      "g.inject(g.withComputer().V().shortestPath().with(ShortestPath.distance, 'weight').with(ShortestPath.includeEdges, true).with(ShortestPath.maxDistance, 1).toList().toArray()).map(unfold().values('name','weight').fold())",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.inject(
  g.withComputer().
    V().
    shortestPath().
      with(ShortestPath.distance, 'weight').
      with(ShortestPath.includeEdges, true).
      with(ShortestPath.maxDistance, 1).
    toList().
    toArray()).
  map(unfold().values('name', 'weight').fold())`,
  );
  expect(
    formatQuery("g.V().hasLabel('person').valueMap().with(WithOptions.tokens)", {
      indentation: 0,
      maxLineLength: 35,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V().
  hasLabel('person').
  valueMap().
    with(WithOptions.tokens)`,
  );
  expect(
    formatQuery("g.V().hasLabel('person').valueMap('name').with(WithOptions.tokens,WithOptions.labels)", {
      indentation: 0,
      maxLineLength: 35,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V().
  hasLabel('person').
  valueMap('name').
    with(
      WithOptions.tokens,
      WithOptions.labels)`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('person').properties('location').valueMap().with(WithOptions.tokens, WithOptions.values)",
      {
        indentation: 0,
        maxLineLength: 35,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  properties('location').
  valueMap().
    with(
      WithOptions.tokens,
      WithOptions.values)`,
  );

  // Test with_()-modulator indentation
  expect(
    formatQuery(
      "g.V().connectedComponent().with_(ConnectedComponent.propertyName, 'component').project('name','component').by('name').by('component')",
      {
        indentation: 0,
        maxLineLength: 55,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  connectedComponent().
    with_(ConnectedComponent.propertyName, 'component').
  project('name', 'component').
    by('name').
    by('component')`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('person').connectedComponent().with_(ConnectedComponent.propertyName, 'component').with_(ConnectedComponent.edges, outE('knows')).project('name','component').by('name').by('component')",
      {
        indentation: 0,
        maxLineLength: 55,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  connectedComponent().
    with_(ConnectedComponent.propertyName, 'component').
    with_(ConnectedComponent.edges, outE('knows')).
  project('name', 'component').
    by('name').
    by('component')`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('software').values('name').fold().order(Scope.local).index().with_(WithOptions.indexer, WithOptions.list).unfold().order().by(__.tail(Scope.local, 1))",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('software').
  values('name').
  fold().
  order(Scope.local).
  index().
    with_(WithOptions.indexer, WithOptions.list).
  unfold().
  order().by(__.tail(Scope.local, 1))`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('person').values('name').fold().order(Scope.local).index().with_(WithOptions.indexer, WithOptions.map)",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  values('name').
  fold().
  order(Scope.local).
  index().
    with_(WithOptions.indexer, WithOptions.map)`,
  );
  expect(
    formatQuery('g.io(someInputFile).with_(IO.reader, IO.graphson).read().iterate()', {
      indentation: 0,
      maxLineLength: 35,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.io(someInputFile).
    with_(IO.reader, IO.graphson).
    read().
  iterate()`,
  );
  expect(
    formatQuery('g.io(someOutputFile).with_(IO.writer,IO.graphml).write().iterate()', {
      indentation: 0,
      maxLineLength: 35,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.io(someOutputFile).
    with_(IO.writer, IO.graphml).
    write().
  iterate()`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('person').pageRank().with_(PageRank.edges, __.outE('knows')).with_(PageRank.propertyName, 'friendRank').order().by('friendRank',desc).elementMap('name','friendRank')",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  pageRank().
    with_(PageRank.edges, __.outE('knows')).
    with_(PageRank.propertyName, 'friendRank').
  order().by('friendRank', desc).
  elementMap('name', 'friendRank')`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('person').peerPressure().with_(PeerPressure.propertyName, 'cluster').group().by('cluster').by('name')",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  peerPressure().
    with_(PeerPressure.propertyName, 'cluster').
  group().by('cluster').by('name')`,
  );
  expect(
    formatQuery("g.V().shortestPath().with_(ShortestPath.target, __.has('name','peter'))", {
      indentation: 0,
      maxLineLength: 55,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V().
  shortestPath().
    with_(ShortestPath.target, __.has('name', 'peter'))`,
  );
  expect(
    formatQuery(
      "g.V().shortestPath().with_(ShortestPath.edges, Direction.IN).with_(ShortestPath.target, __.has('name','josh'))",
      {
        indentation: 0,
        maxLineLength: 55,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  shortestPath().
    with_(ShortestPath.edges, Direction.IN).
    with_(ShortestPath.target, __.has('name', 'josh'))`,
  );
  expect(
    formatQuery("g.V().has('person','name','marko').shortestPath().with_(ShortestPath.target,__.has('name','josh'))", {
      indentation: 0,
      maxLineLength: 55,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with_(ShortestPath.target, __.has('name', 'josh'))`,
  );
  expect(
    formatQuery(
      "g.V().has('person','name','marko').shortestPath().with_(ShortestPath.target, __.has('name','josh')).with_(ShortestPath.distance, 'weight')",
      {
        indentation: 0,
        maxLineLength: 55,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with_(ShortestPath.target, __.has('name', 'josh')).
    with_(ShortestPath.distance, 'weight')`,
  );
  expect(
    formatQuery(
      "g.V().has('person','name','marko').shortestPath().with_(ShortestPath.target, __.has('name','josh')).with_(ShortestPath.includeEdges, true)",
      {
        indentation: 0,
        maxLineLength: 55,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with_(ShortestPath.target, __.has('name', 'josh')).
    with_(ShortestPath.includeEdges, true)`,
  );
  expect(
    formatQuery(
      "g.inject(g.withComputer().V().shortestPath().with_(ShortestPath.distance, 'weight').with_(ShortestPath.includeEdges, true).with_(ShortestPath.maxDistance, 1).toList().toArray()).map(unfold().values('name','weight').fold())",
      {
        indentation: 0,
        maxLineLength: 50,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.inject(
  g.withComputer().
    V().
    shortestPath().
      with_(ShortestPath.distance, 'weight').
      with_(ShortestPath.includeEdges, true).
      with_(ShortestPath.maxDistance, 1).
    toList().
    toArray()).
  map(unfold().values('name', 'weight').fold())`,
  );
  expect(
    formatQuery("g.V().hasLabel('person').valueMap().with_(WithOptions.tokens)", {
      indentation: 0,
      maxLineLength: 35,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V().
  hasLabel('person').
  valueMap().
    with_(WithOptions.tokens)`,
  );
  expect(
    formatQuery("g.V().hasLabel('person').valueMap('name').with_(WithOptions.tokens,WithOptions.labels)", {
      indentation: 0,
      maxLineLength: 35,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.V().
  hasLabel('person').
  valueMap('name').
    with_(
      WithOptions.tokens,
      WithOptions.labels)`,
  );
  expect(
    formatQuery(
      "g.V().hasLabel('person').properties('location').valueMap().with_(WithOptions.tokens, WithOptions.values)",
      {
        indentation: 0,
        maxLineLength: 35,
        shouldPlaceDotsAfterLineBreaks: false,
      },
    ),
  ).toBe(
    `g.V().
  hasLabel('person').
  properties('location').
  valueMap().
    with_(
      WithOptions.tokens,
      WithOptions.values)`,
  );

  // Test write()-modulator indentation
  expect(
    formatQuery('g.io(someOutputFile).write().iterate()', {
      indentation: 0,
      maxLineLength: 25,
      shouldPlaceDotsAfterLineBreaks: false,
    }),
  ).toBe(
    `g.io(someOutputFile).
    write().
  iterate()`,
  );
});
