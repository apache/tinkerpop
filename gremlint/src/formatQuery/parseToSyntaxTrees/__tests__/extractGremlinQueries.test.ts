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

import { extractGremlinQueries } from '../extractGremlinQueries';

test('Extract the parts of the code that can be parsed as Gremlin', () => {
  expect(
    extractGremlinQueries(
      `graph = TinkerFactory.createModern()
g = graph.traversal()
g.V().has('name','marko').out('knows').values('name')`,
    ),
  ).toStrictEqual([`g.V().has('name','marko').out('knows').values('name')`]);

  expect(
    extractGremlinQueries(
      `g.V().has('name','marko').next()
g.V(marko).out('knows')
g.V(marko).out('knows').values('name')`,
    ),
  ).toStrictEqual([
    `g.V().has('name','marko').next()`,
    `g.V(marko).out('knows')`,
    `g.V(marko).out('knows').values('name')`,
  ]);

  expect(
    extractGremlinQueries(
      `g = graph.traversal();
List<Vertex> vertices = g.V().toList()`,
    ),
  ).toStrictEqual([`g.V().toList()`]);

  expect(
    extractGremlinQueries(
      `v1 = g.addV('person').property('name','marko').next()
v2 = g.addV('person').property('name','stephen').next()
g.V(v1).addE('knows').to(v2).property('weight',0.75).iterate()`,
    ),
  ).toStrictEqual([
    `g.addV('person').property('name','marko').next()`,
    `g.addV('person').property('name','stephen').next()`,
    `g.V(v1).addE('knows').to(v2).property('weight',0.75).iterate()`,
  ]);

  expect(
    extractGremlinQueries(
      `marko = g.V().has('person','name','marko').next()
peopleMarkoKnows = g.V().has('person','name','marko').out('knows').toList()`,
    ),
  ).toStrictEqual([
    `g.V().has('person','name','marko').next()`,
    `g.V().has('person','name','marko').out('knows').toList()`,
  ]);

  expect(
    extractGremlinQueries(
      `graph = TinkerGraph.open()
g = graph.traversal()
v = g.addV().property('name','marko').property('name','marko a. rodriguez').next()
g.V(v).properties('name').count()
v.property(list, 'name', 'm. a. rodriguez')
g.V(v).properties('name').count()
g.V(v).properties()
g.V(v).properties('name')
g.V(v).properties('name').hasValue('marko')
g.V(v).properties('name').hasValue('marko').property('acl','private')
g.V(v).properties('name').hasValue('marko a. rodriguez')
g.V(v).properties('name').hasValue('marko a. rodriguez').property('acl','public')
g.V(v).properties('name').has('acl','public').value()
g.V(v).properties('name').has('acl','public').drop()
g.V(v).properties('name').has('acl','public').value()
g.V(v).properties('name').has('acl','private').value()
g.V(v).properties()
g.V(v).properties().properties()
g.V(v).properties().property('date',2014)
g.V(v).properties().property('creator','stephen')
g.V(v).properties().properties()
g.V(v).properties('name').valueMap()
g.V(v).property('name','okram')
g.V(v).properties('name')
g.V(v).values('name')`,
    ),
  ).toStrictEqual([
    `g.addV().property('name','marko').property('name','marko a. rodriguez').next()`,
    `g.V(v).properties('name').count()`,
    `g.V(v).properties('name').count()`,
    `g.V(v).properties()`,
    `g.V(v).properties('name')`,
    `g.V(v).properties('name').hasValue('marko')`,
    `g.V(v).properties('name').hasValue('marko').property('acl','private')`,
    `g.V(v).properties('name').hasValue('marko a. rodriguez')`,
    `g.V(v).properties('name').hasValue('marko a. rodriguez').property('acl','public')`,
    `g.V(v).properties('name').has('acl','public').value()`,
    `g.V(v).properties('name').has('acl','public').drop()`,
    `g.V(v).properties('name').has('acl','public').value()`,
    `g.V(v).properties('name').has('acl','private').value()`,
    `g.V(v).properties()`,
    `g.V(v).properties().properties()`,
    `g.V(v).properties().property('date',2014)`,
    `g.V(v).properties().property('creator','stephen')`,
    `g.V(v).properties().properties()`,
    `g.V(v).properties('name').valueMap()`,
    `g.V(v).property('name','okram')`,
    `g.V(v).properties('name')`,
    `g.V(v).values('name')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().as('a').
      properties('location').as('b').
      hasNot('endTime').as('c').
      select('a','b','c').by('name').by(value).by('startTime') // determine the current location of each person
g.V().has('name','gremlin').inE('uses').
      order().by('skill',asc).as('a').
      outV().as('b').
      select('a','b').by('skill').by('name') // rank the users of gremlin by their skill level`,
    ),
  ).toStrictEqual([
    `g.V().as('a').
      properties('location').as('b').
      hasNot('endTime').as('c').
      select('a','b','c').by('name').by(value).by('startTime')`,
    `g.V().has('name','gremlin').inE('uses').
      order().by('skill',asc).as('a').
      outV().as('b').
      select('a','b').by('skill').by('name')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V(1).out().values('name')
g.V(1).out().map {it.get().value('name')}
g.V(1).out().map(values('name'))`,
    ),
  ).toStrictEqual([
    `g.V(1).out().values('name')`,
    `g.V(1).out().map {it.get().value('name')}`,
    `g.V(1).out().map(values('name'))`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().filter {it.get().label() == 'person'}
g.V().filter(label().is('person'))
g.V().hasLabel('person')`,
    ),
  ).toStrictEqual([
    `g.V().filter {it.get().label() == 'person'}`,
    `g.V().filter(label().is('person'))`,
    `g.V().hasLabel('person')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().hasLabel('person').sideEffect(System.out.&println)
g.V().sideEffect(outE().count().store("o")).
      sideEffect(inE().count().store("i")).cap("o","i")`,
    ),
  ).toStrictEqual([
    `g.V().hasLabel('person').sideEffect(System.out.&println)`,
    `g.V().sideEffect(outE().count().store("o")).
      sideEffect(inE().count().store("i")).cap("o","i")`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().branch {it.get().value('name')}.
      option('marko', values('age')).
      option(none, values('name'))
g.V().branch(values('name')).
      option('marko', values('age')).
      option(none, values('name'))
g.V().choose(has('name','marko'),
             values('age'),
             values('name'))`,
    ),
  ).toStrictEqual([
    `g.V().branch {it.get().value('name')}.
      option('marko', values('age')).
      option(none, values('name'))`,
    `g.V().branch(values('name')).
      option('marko', values('age')).
      option(none, values('name'))`,
    `g.V().choose(has('name','marko'),
             values('age'),
             values('name'))`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().out('created').hasNext()
g.V().out('created').next()
g.V().out('created').next(2)
g.V().out('nothing').tryNext()
g.V().out('created').toList()
g.V().out('created').toSet()
g.V().out('created').toBulkSet()
results = ['blah',3]
g.V().out('created').fill(results)
g.addV('person').iterate()`,
    ),
  ).toStrictEqual([
    `g.V().out('created').hasNext()`,
    `g.V().out('created').next()`,
    `g.V().out('created').next(2)`,
    `g.V().out('nothing').tryNext()`,
    `g.V().out('created').toList()`,
    `g.V().out('created').toSet()`,
    `g.V().out('created').toBulkSet()`,
    `g.V().out('created').fill(results)`,
    `g.addV('person').iterate()`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V(1).as('a').out('created').in('created').where(neq('a')).
  addE('co-developer').from('a').property('year',2009)
g.V(3,4,5).aggregate('x').has('name','josh').as('a').
  select('x').unfold().hasLabel('software').addE('createdBy').to('a')
g.V().as('a').out('created').addE('createdBy').to('a').property('acl','public')
g.V(1).as('a').out('knows').
  addE('livesNear').from('a').property('year',2009).
  inV().inE('livesNear').values('year')
g.V().match(
        __.as('a').out('knows').as('b'),
        __.as('a').out('created').as('c'),
        __.as('b').out('created').as('c')).
      addE('friendlyCollaborator').from('a').to('b').
        property(id,23).property('project',select('c').values('name'))
g.E(23).valueMap()
marko = g.V().has('name','marko').next()
peter = g.V().has('name','peter').next()
g.V(marko).addE('knows').to(peter)
g.addE('knows').from(marko).to(peter)`,
    ),
  ).toStrictEqual([
    `g.V(1).as('a').out('created').in('created').where(neq('a')).
  addE('co-developer').from('a').property('year',2009)`,
    `g.V(3,4,5).aggregate('x').has('name','josh').as('a').
  select('x').unfold().hasLabel('software').addE('createdBy').to('a')`,
    `g.V().as('a').out('created').addE('createdBy').to('a').property('acl','public')`,
    `g.V(1).as('a').out('knows').
  addE('livesNear').from('a').property('year',2009).
  inV().inE('livesNear').values('year')`,
    `g.V().match(
        __.as('a').out('knows').as('b'),
        __.as('a').out('created').as('c'),
        __.as('b').out('created').as('c')).
      addE('friendlyCollaborator').from('a').to('b').
        property(id,23).property('project',select('c').values('name'))`,
    `g.E(23).valueMap()`,
    `g.V().has('name','marko').next()`,
    `g.V().has('name','peter').next()`,
    `g.V(marko).addE('knows').to(peter)`,
    `g.addE('knows').from(marko).to(peter)`,
  ]);

  expect(
    extractGremlinQueries(
      `g.addV('person').property('name','stephen')
g.V().values('name')
g.V().outE('knows').addV().property('name','nothing')
g.V().has('name','nothing')
g.V().has('name','nothing').bothE()`,
    ),
  ).toStrictEqual([
    `g.addV('person').property('name','stephen')`,
    `g.V().values('name')`,
    `g.V().outE('knows').addV().property('name','nothing')`,
    `g.V().has('name','nothing')`,
    `g.V().has('name','nothing').bothE()`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V(1).property('country','usa')
g.V(1).property('city','santa fe').property('state','new mexico').valueMap()
g.V(1).property(list,'age',35)
g.V(1).valueMap()
g.V(1).property('friendWeight',outE('knows').values('weight').sum(),'acl','private')
g.V(1).properties('friendWeight').valueMap()`,
    ),
  ).toStrictEqual([
    `g.V(1).property('country','usa')`,
    `g.V(1).property('city','santa fe').property('state','new mexico').valueMap()`,
    `g.V(1).property(list,'age',35)`,
    `g.V(1).valueMap()`,
    `g.V(1).property('friendWeight',outE('knows').values('weight').sum(),'acl','private')`,
    `g.V(1).properties('friendWeight').valueMap()`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V(1).out('created')
g.V(1).out('created').aggregate('x')
g.V(1).out('created').aggregate(global, 'x')
g.V(1).out('created').aggregate('x').in('created')
g.V(1).out('created').aggregate('x').in('created').out('created')
g.V(1).out('created').aggregate('x').in('created').out('created').
       where(without('x')).values('name')`,
    ),
  ).toStrictEqual([
    `g.V(1).out('created')`,
    `g.V(1).out('created').aggregate('x')`,
    `g.V(1).out('created').aggregate(global, 'x')`,
    `g.V(1).out('created').aggregate('x').in('created')`,
    `g.V(1).out('created').aggregate('x').in('created').out('created')`,
    `g.V(1).out('created').aggregate('x').in('created').out('created').
       where(without('x')).values('name')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().out('knows').aggregate('x').cap('x')
g.V().out('knows').aggregate('x').by('name').cap('x')`,
    ),
  ).toStrictEqual([
    `g.V().out('knows').aggregate('x').cap('x')`,
    `g.V().out('knows').aggregate('x').by('name').cap('x')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().aggregate(global, 'x').limit(1).cap('x')
g.V().aggregate(local, 'x').limit(1).cap('x')
g.withoutStrategies(EarlyLimitStrategy).V().aggregate(local,'x').limit(1).cap('x')`,
    ),
  ).toStrictEqual([
    `g.V().aggregate(global, 'x').limit(1).cap('x')`,
    `g.V().aggregate(local, 'x').limit(1).cap('x')`,
    `g.withoutStrategies(EarlyLimitStrategy).V().aggregate(local,'x').limit(1).cap('x')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().as('a').out('created').as('b').select('a','b')
g.V().as('a').out('created').as('b').select('a','b').by('name')`,
    ),
  ).toStrictEqual([
    `g.V().as('a').out('created').as('b').select('a','b')`,
    `g.V().as('a').out('created').as('b').select('a','b').by('name')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().sideEffect{println "first: \${it}"}.sideEffect{println "second: \${it}"}.iterate()
g.V().sideEffect{println "first: \${it}"}.barrier().sideEffect{println "second: \${it}"}.iterate()`,
    ),
  ).toStrictEqual([
    `g.V().sideEffect{println "first: \${it}"}.sideEffect{println "second: \${it}"}.iterate()`,
    `g.V().sideEffect{println "first: \${it}"}.barrier().sideEffect{println "second: \${it}"}.iterate()`,
  ]);

  expect(
    extractGremlinQueries(
      `graph = TinkerGraph.open()
g = graph.traversal()
g.io('data/grateful-dead.xml').read().iterate()
g = graph.traversal().withoutStrategies(LazyBarrierStrategy)
clockWithResult(1){g.V().both().both().both().count().next()}
clockWithResult(1){g.V().repeat(both()).times(3).count().next()}
clockWithResult(1){g.V().both().barrier().both().barrier().both().barrier().count().next()}`,
    ),
  ).toStrictEqual([`g.io('data/grateful-dead.xml').read().iterate()`]);

  expect(
    extractGremlinQueries(
      `graph = TinkerGraph.open()
g = graph.traversal()
g.io('data/grateful-dead.xml').read().iterate()
clockWithResult(1){g.V().both().both().both().count().next()}
g.V().both().both().both().count().iterate().toString()`,
    ),
  ).toStrictEqual([
    `g.io('data/grateful-dead.xml').read().iterate()`,
    `g.V().both().both().both().count().iterate().toString()`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().group().by(bothE().count())
g.V().group().by(bothE().count()).by('name')
g.V().group().by(bothE().count()).by(count())`,
    ),
  ).toStrictEqual([
    `g.V().group().by(bothE().count())`,
    `g.V().group().by(bothE().count()).by('name')`,
    `g.V().group().by(bothE().count()).by(count())`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().groupCount('a').by(label).cap('a')
g.V().groupCount('a').by(label).groupCount('b').by(outE().count()).cap('a','b')`,
    ),
  ).toStrictEqual([
    `g.V().groupCount('a').by(label).cap('a')`,
    `g.V().groupCount('a').by(label).groupCount('b').by(outE().count()).cap('a','b')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().hasLabel('person').
      choose(values('age').is(lte(30)),
        __.in(),
        __.out()).values('name')
g.V().hasLabel('person').
      choose(values('age')).
        option(27, __.in()).
        option(32, __.out()).values('name')`,
    ),
  ).toStrictEqual([
    `g.V().hasLabel('person').
      choose(values('age').is(lte(30)),
        __.in(),
        __.out()).values('name')`,
    `g.V().hasLabel('person').
      choose(values('age')).
        option(27, __.in()).
        option(32, __.out()).values('name')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().choose(hasLabel('person'), out('created')).values('name')
g.V().choose(hasLabel('person'), out('created'), identity()).values('name')`,
    ),
  ).toStrictEqual([
    `g.V().choose(hasLabel('person'), out('created')).values('name')`,
    `g.V().choose(hasLabel('person'), out('created'), identity()).values('name')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V(1).coalesce(outE('knows'), outE('created')).inV().path().by('name').by(label)
g.V(1).coalesce(outE('created'), outE('knows')).inV().path().by('name').by(label)
g.V(1).property('nickname', 'okram')
g.V().hasLabel('person').coalesce(values('nickname'), values('name'))`,
    ),
  ).toStrictEqual([
    `g.V(1).coalesce(outE('knows'), outE('created')).inV().path().by('name').by(label)`,
    `g.V(1).coalesce(outE('created'), outE('knows')).inV().path().by('name').by(label)`,
    `g.V(1).property('nickname', 'okram')`,
    `g.V().hasLabel('person').coalesce(values('nickname'), values('name'))`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().coin(0.5)
g.V().coin(0.0)
g.V().coin(1.0)`,
    ),
  ).toStrictEqual([`g.V().coin(0.5)`, `g.V().coin(0.0)`, `g.V().coin(1.0)`]);

  expect(
    extractGremlinQueries(
      `g.V().
  connectedComponent().
    with(ConnectedComponent.propertyName, 'component').
  project('name','component').
    by('name').
    by('component')
g.V().hasLabel('person').
  connectedComponent().
    with(ConnectedComponent.propertyName, 'component').
    with(ConnectedComponent.edges, outE('knows')).
  project('name','component').
    by('name').
    by('component')`,
    ),
  ).toStrictEqual([
    `g.V().
  connectedComponent().
    with(ConnectedComponent.propertyName, 'component').
  project('name','component').
    by('name').
    by('component')`,
    `g.V().hasLabel('person').
  connectedComponent().
    with(ConnectedComponent.propertyName, 'component').
    with(ConnectedComponent.edges, outE('knows')).
  project('name','component').
    by('name').
    by('component')`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().choose(hasLabel('person'),
    values('name'),
    constant('inhuman'))
g.V().coalesce(
    hasLabel('person').values('name'),
    constant('inhuman'))`,
    ),
  ).toStrictEqual([
    `g.V().choose(hasLabel('person'),
    values('name'),
    constant('inhuman'))`,
    `g.V().coalesce(
    hasLabel('person').values('name'),
    constant('inhuman'))`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V().count()
g.V().hasLabel('person').count()
g.V().hasLabel('person').outE('created').count().path()
g.V().hasLabel('person').outE('created').count().map {it.get() * 10}.path()`,
    ),
  ).toStrictEqual([
    `g.V().count()`,
    `g.V().hasLabel('person').count()`,
    `g.V().hasLabel('person').outE('created').count().path()`,
    `g.V().hasLabel('person').outE('created').count().map {it.get() * 10}.path()`,
  ]);

  expect(
    extractGremlinQueries(
      `g.V(1).both().both()
g.V(1).both().both().cyclicPath()
g.V(1).both().both().cyclicPath().path()
g.V(1).as('a').out('created').as('b').
  in('created').as('c').
  cyclicPath().
  path()
g.V(1).as('a').out('created').as('b').
  in('created').as('c').
  cyclicPath().from('a').to('b').
  path()`,
    ),
  ).toStrictEqual([
    `g.V(1).both().both()`,
    `g.V(1).both().both().cyclicPath()`,
    `g.V(1).both().both().cyclicPath().path()`,
    `g.V(1).as('a').out('created').as('b').
  in('created').as('c').
  cyclicPath().
  path()`,
    `g.V(1).as('a').out('created').as('b').
  in('created').as('c').
  cyclicPath().from('a').to('b').
  path()`,
  ]);
});
