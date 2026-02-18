/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import assert from 'assert';
import { GraphTraversalSource, statics as __, CardinalityValue } from '../../lib/process/graph-traversal.js';
import { P, TextP, t as T, order as Order, scope as Scope, column as Column,
         operator as Operator, pop as Pop, cardinality as Cardinality,
         withOptions as WithOptions, direction } from '../../lib/process/traversal.js';
import { ReadOnlyStrategy, SubgraphStrategy, OptionsStrategy,
         PartitionStrategy, SeedStrategy } from '../../lib/process/traversal-strategy.js';
import { Graph, Vertex } from '../../lib/structure/graph.js';
import { TraversalStrategies } from '../../lib/process/traversal-strategy.js';
import { Long } from '../../lib/utils.js';
import GremlinLang from '../../lib/process/gremlin-lang.js';

const g = new GraphTraversalSource(new Graph(), new TraversalStrategies());

describe('GremlinLang', function () {
  describe('End-to-end tests', function () {
    const tests = [
      // #0
      [g.V(), 'g.V()'],
      // #1
      [g.V('1','2','3','4'), "g.V('1','2','3','4')"],
      // #2
      [g.V('3').valueMap(true), "g.V('3').valueMap(true)"],
      // #3
      [g.V().constant(5), 'g.V().constant(5)'],
      // #4
      [g.V().constant(1.5), 'g.V().constant(1.5)'],
      // #5
      [g.V().constant('Hello'), "g.V().constant('Hello')"],
      // #6
      [g.V().hasLabel('airport').limit(5), "g.V().hasLabel('airport').limit(5)"],
      // #7
      [g.V().hasLabel(P.within(['a','b','c'])), "g.V().hasLabel(within(['a','b','c']))"],
      // #8
      [g.V().hasLabel('airport','continent').out().limit(5), "g.V().hasLabel('airport','continent').out().limit(5)"],
      // #9
      [g.V().hasLabel('airport').out().values('code').limit(5), "g.V().hasLabel('airport').out().values('code').limit(5)"],
      // #10
      [g.V('3').as('a').out('route').limit(10).where(P.eq('a')).by('region'), "g.V('3').as('a').out('route').limit(10).where(eq('a')).by('region')"],
      // #11
      [g.V('3').repeat(__.out('route').simplePath()).times(2).path().by('code'), "g.V('3').repeat(__.out('route').simplePath()).times(2).path().by('code')"],
      // #12
      [g.V().has('airport','region','US-TX').order().by('code'), "g.V().has('airport','region','US-TX').order().by('code')"],
      // #13
      [g.V('3').union(__.values('code'),__.values('city')), "g.V('3').union(__.values('code'),__.values('city'))"],
      // #14
      [g.V('3').as('a').out('route','routes').path().by('code'), "g.V('3').as('a').out('route','routes').path().by('code')"],
      // #15
      [g.V().where(__.values('runways').is(5)), "g.V().where(__.values('runways').is(5))"],
      // #16
      [g.V('3').repeat(__.out()).until(__.has('code','AGR')).path().by('code').limit(5), "g.V('3').repeat(__.out()).until(__.has('code','AGR')).path().by('code').limit(5)"],
      // #17
      [g.V().order().by(__.id()), 'g.V().order().by(__.id())'],
      // #18
      [g.V().order().by(T.id), 'g.V().order().by(T.id)'],
      // #19
      [g.V().order().by(__.id(),Order.desc), 'g.V().order().by(__.id(),Order.desc)'],
      // #20
      [g.V().order().by('code',Order.desc), "g.V().order().by('code',Order.desc)"],
      // #21
      [g.V().hasLabel('airport').dedup().local(__.values('code','city').fold()), "g.V().hasLabel('airport').dedup().local(__.values('code','city').fold())"],
      // #22
      [g.V().count(Scope.local), 'g.V().count(Scope.local)'],
      // #23
      [g.E().count(), 'g.E().count()'],
      // #24
      [g.V('3').outE('route').inV().path().by('code').by('dist'), "g.V('3').outE('route').inV().path().by('code').by('dist')"],
      // #25
      [g.V('3').propertyMap().select(Column.keys), "g.V('3').propertyMap().select(Column.keys)"],
      // #26
      [g.V('3').propertyMap().select(Column.values), "g.V('3').propertyMap().select(Column.values)"],
      // #27
      [g.V().has('airport','code','AUS').values('runways').math('_ + 1'), "g.V().has('airport','code','AUS').values('runways').math('_ + 1')"],
      // #28
      [g.V('3').emit().repeat(__.out()).times(3).limit(5).path().by('code'), "g.V('3').emit().repeat(__.out()).times(3).limit(5).path().by('code')"],
      // #29
      [g.V().match(__.as('a').has('code','AUS'),__.as('a').out('route').as('b')).select('b').by('code'), "g.V().match(__.as('a').has('code','AUS'),__.as('a').out('route').as('b')).select('b').by('code')"],
      // #30
      [g.V().has('test-using-keyword-as-property','repeat'), "g.V().has('test-using-keyword-as-property','repeat')"],
      // #31
      [g.addE('test').to(__.V('4')), "g.addE('test').to(__.V('4'))"],
      // #32
      [g.V().values('runways').max(), 'g.V().values(\'runways\').max()'],
      // #33
      [g.V().values('runways').min(), 'g.V().values(\'runways\').min()'],
      // #34
      [g.V().values('runways').sum(), 'g.V().values(\'runways\').sum()'],
      // #35
      [g.V().values('runways').mean(), 'g.V().values(\'runways\').mean()'],
      // #36
      [g.withSack(0).V('3','5').sack(Operator.sum).by('runways').sack(), "g.withSack(0).V('3','5').sack(Operator.sum).by('runways').sack()"],
      // #37
      [g.inject(3,4,5), 'g.inject(3,4,5)'],
      // #38
      [g.inject([3,4,5]), 'g.inject([3,4,5])'],
      // #39
      [g.inject(3,4,5).count(), 'g.inject(3,4,5).count()'],
      // #40
      [g.V().has('runways',P.gt(5)).count(), "g.V().has('runways',gt(5)).count()"],
      // #41
      [g.V().has('runways',P.lte(5.3)).count(), "g.V().has('runways',lte(5.3)).count()"],
      // #42
      [g.V().has('code',P.within([123,124])), "g.V().has('code',within([123,124]))"],
      // #43
      [g.V().has('code',P.within([123,'abc'])), "g.V().has('code',within([123,'abc']))"],
      // #44
      [g.V().has('code',P.within(['abc',123])), "g.V().has('code',within(['abc',123]))"],
      // #45
      [g.V().has('code',P.within(['abc','xyz'])), "g.V().has('code',within(['abc','xyz']))"],
      // #46
      [g.V().has('region',P.within(['US-TX','US-GA'])), "g.V().has('region',within(['US-TX','US-GA']))"],
      // #47
      [g.V().and(__.has('runways',P.gt(5)),__.has('region','US-TX')), "g.V().and(__.has('runways',gt(5)),__.has('region','US-TX'))"],
      // #48
      [g.V().union(__.has('runways',P.gt(5)),__.has('region','US-TX')), "g.V().union(__.has('runways',gt(5)),__.has('region','US-TX'))"],
      // #49
      [g.V('3').choose(__.values('runways').is(3),__.constant('three'),__.constant('not three')), "g.V('3').choose(__.values('runways').is(3),__.constant('three'),__.constant('not three'))"],
      // #50
      [g.V('3').choose(__.out().count()).option(0,__.constant('none')).option(1,__.constant('one')).option(2,__.constant('two')), "g.V('3').choose(__.out().count()).option(0,__.constant('none')).option(1,__.constant('one')).option(2,__.constant('two'))"],
      // #51
      [g.V('3').choose(__.out().count()).option(1.5,__.constant('one and a half')), "g.V('3').choose(__.out().count()).option(1.5,__.constant('one and a half'))"],
      // #52
      [g.V().repeat(__.out()).until(__.or(__.loops().is(3),__.has('code','AGR'))).count(), "g.V().repeat(__.out()).until(__.or(__.loops().is(3),__.has('code','AGR'))).count()"],
      // #53
      [g.V().group().by('continent').by(__.count()).order(Scope.local).by(Column.values), "g.V().group().by('continent').by(__.count()).order(Scope.local).by(Column.values)"],
      // #54
      [g.V('3').as('a').out('route').as('a').select(Pop.all,'a'), "g.V('3').as('a').out('route').as('a').select(Pop.all,'a')"],
      // #55
      [g.addV('test').property(Cardinality.set,'p1',10), "g.addV('test').property(Cardinality.set,'p1',10)"],
      // #56
      [g.addV('test').property(Cardinality.list,'p1',10), "g.addV('test').property(Cardinality.list,'p1',10)"],
      // #57
      [g.addV('test').property(Cardinality.single,'p1',10), "g.addV('test').property(Cardinality.single,'p1',10)"],
      // #58
      [g.V().order().by(T.label), 'g.V().order().by(T.label)'],
      // #59
      [g.V().range(1,5), 'g.V().range(1,5)'],
      // #60
      [g.addV('test').property('p1',123), "g.addV('test').property('p1',123)"],
      // #61
      [g.addV('test').property('date',new Date('2021-02-01T09:30:00.000Z')), `g.addV('test').property('date',datetime("2021-02-01T09:30:00.000Z"))`],
      // #62
      [g.addV('test').property('date',new Date('2021-02-01T00:00:00.000Z')), `g.addV('test').property('date',datetime("2021-02-01T00:00:00.000Z"))`],
      // #63
      [g.addE('route').from_(__.V('1')).to(__.V('2')), "g.addE('route').from(__.V('1')).to(__.V('2'))"],
      // #64
      [g.withSideEffect('a',[1,2]).V().values('runways').where(P.within('a')), "g.withSideEffect('a',[1,2]).V().values('runways').where(within(['a']))"],
      // #65
      [g.withSideEffect('a',1).V().values('runways').where(P.within('a')), "g.withSideEffect('a',1).V().values('runways').where(within(['a']))"],
      // #66
      [g.withSideEffect('a','abc').V().values('runways').where(P.within('a')), "g.withSideEffect('a','abc').V().values('runways').where(within(['a']))"],
      // #67
      [g.V().hasLabel('airport').limit(5).values('code').fold().index(), "g.V().hasLabel('airport').limit(5).values('code').fold().index()"],
      // #68
      [g.V().hasLabel('airport').timeLimit(1000).count(), "g.V().hasLabel('airport').timeLimit(1000).count()"],
      // #69
      [g.V().where(__.values('elev').is(P.gt(14000))), "g.V().where(__.values('elev').is(gt(14000)))"],
      // #70
      [g.V().where(__.out().count().is(P.gt(200))).values('code'), "g.V().where(__.out().count().is(gt(200))).values('code')"],
      // #71
      [g.V().filter(__.has('region','US-TX')).count(), "g.V().filter(__.has('region','US-TX')).count()"],
      // #72
      [g.withSack(0).V('3').repeat(__.outE('route').sack(Operator.sum).by('dist').inV()).until(__.or(__.loops().is(2),__.sack().is(P.gt(500)))).local(__.union(__.path().by('code').by('dist'),__.sack()).fold()).limit(10), "g.withSack(0).V('3').repeat(__.outE('route').sack(Operator.sum).by('dist').inV()).until(__.or(__.loops().is(2),__.sack().is(gt(500)))).local(__.union(__.path().by('code').by('dist'),__.sack()).fold()).limit(10)"],
      // #73
      [g.addV().as('a').addV().as('b').addE('knows').from_('a').to('b'), "g.addV().as('a').addV().as('b').addE('knows').from('a').to('b')"],
      // #74
      [g.addV('Person').as('a').addV('Person').as('b').addE('knows').from_('a').to('b'), "g.addV('Person').as('a').addV('Person').as('b').addE('knows').from('a').to('b')"],
      // #75
      [g.V('3').project('Out','In').by(__.out().count()).by(__.in_().count()), "g.V('3').project('Out','In').by(__.out().count()).by(__.in().count())"],
      // #76
      [g.V('3').out('route').aggregate('x').where(P.within('x')).path().by('code'), "g.V('3').out('route').aggregate('x').where(within(['x'])).path().by('code')"],
      // #77
      [g.V().has('date',new Date('2021-02-22T00:00:00.000Z')), `g.V().has('date',datetime("2021-02-22T00:00:00.000Z"))`],
      // #78
      [g.V().has('date',P.within([new Date('2021-01-01T00:00:00.000Z'),new Date('2021-02-22T00:00:00.000Z')])), `g.V().has('date',within([datetime("2021-01-01T00:00:00.000Z"),datetime("2021-02-22T00:00:00.000Z")]))`],
      // #79
      [g.V().has('date',P.between(new Date('2021-01-01T00:00:00.000Z'),new Date('2021-02-22T00:00:00.000Z'))), `g.V().has('date',between(datetime("2021-01-01T00:00:00.000Z"),datetime("2021-02-22T00:00:00.000Z")))`],
      // #80
      [g.V().has('date',P.inside(new Date('2021-01-01T00:00:00.000Z'),new Date('2021-02-22T00:00:00.000Z'))), `g.V().has('date',inside(datetime("2021-01-01T00:00:00.000Z"),datetime("2021-02-22T00:00:00.000Z")))`],
      // #81
      [g.V().has('date',P.gt(new Date('2021-01-01T00:00:00.000Z'))), `g.V().has('date',gt(datetime("2021-01-01T00:00:00.000Z")))`],
      // #82
      [g.V().has('runways',P.between(3,5)), "g.V().has('runways',between(3,5))"],
      // #83
      [g.V().has('runways',P.inside(3,5)), "g.V().has('runways',inside(3,5))"],
      // #84
      [g.V('3').outE().elementMap(), "g.V('3').outE().elementMap()"],
      // #85
      [g.V('3').valueMap().by(__.unfold()), "g.V('3').valueMap().by(__.unfold())"],
      // #86
      [g.V('44').valueMap().with_(WithOptions.tokens,WithOptions.labels), "g.V('44').valueMap().with('~tinkerpop.valueMap.tokens',2)"],
      // #87
      [g.V('44').valueMap().with_(WithOptions.tokens), "g.V('44').valueMap().with('~tinkerpop.valueMap.tokens')"],
      // #88
      [g.withStrategies(new ReadOnlyStrategy()).addV('test'), "g.withStrategies(ReadOnlyStrategy).addV('test')"],
      // #89
      [g.withStrategies(new SubgraphStrategy({vertices: __.has('region','US-TX'), edges: __.hasLabel('route')})).V().count(), "g.withStrategies(new SubgraphStrategy(vertices:__.has('region','US-TX'),edges:__.hasLabel('route'))).V().count()"],
      // #90
      [g.withStrategies(new SubgraphStrategy({vertexProperties: __.hasNot('runways')})).V().count(), "g.withStrategies(new SubgraphStrategy(vertexProperties:__.hasNot('runways'))).V().count()"],
      // #91
      [g.withStrategies(new SubgraphStrategy({vertices: __.has('region','US-TX'), vertexProperties: __.hasNot('runways')})).V().count(), "g.withStrategies(new SubgraphStrategy(vertices:__.has('region','US-TX'),vertexProperties:__.hasNot('runways'))).V().count()"],
      // #92
      [g.withStrategies(new ReadOnlyStrategy(), new SubgraphStrategy({vertices: __.has('region','US-TX'), edges: __.hasLabel('route')})).V().count(), "g.withStrategies(ReadOnlyStrategy,new SubgraphStrategy(vertices:__.has('region','US-TX'),edges:__.hasLabel('route'))).V().count()"],
      // #93
      [g.withStrategies(new ReadOnlyStrategy(), new SubgraphStrategy({vertices: __.has('region','US-TX')})).V().count(), "g.withStrategies(ReadOnlyStrategy,new SubgraphStrategy(vertices:__.has('region','US-TX'))).V().count()"],
      // #94 - OptionsStrategy extracted, not serialized
      [g.with_('evaluationTimeout',500).V().count(), 'g.V().count()'],
      // #95 - OptionsStrategy extracted, not serialized
      [g.withStrategies(new OptionsStrategy({evaluationTimeout: 500})).V().count(), 'g.V().count()'],
      // #96
      [g.withStrategies(new PartitionStrategy({partitionKey: '_partition', writePartition: 'a', readPartitions: ['a','b']})).addV('test'), "g.withStrategies(new PartitionStrategy(partitionKey:'_partition',writePartition:'a',readPartitions:['a','b'])).addV('test')"],
      // #97
      [g.withComputer().V().shortestPath().with_('~tinkerpop.shortestPath.target',__.has('name','peter')), "g.withStrategies(VertexProgramStrategy).V().shortestPath().with('~tinkerpop.shortestPath.target',__.has('name','peter'))"],
      // #98
      [g.V().coalesce(__.E(),__.addE('person')), "g.V().coalesce(__.E(),__.addE('person'))"],
      // #99
      [g.inject(1).E(), 'g.inject(1).E()'],
      // #100
      [g.V().has('name',TextP.startingWith('foo')), "g.V().has('name',startingWith('foo'))"],
      // #101
      [g.V().has('name',TextP.endingWith('foo')), "g.V().has('name',endingWith('foo'))"],
      // #102
      [g.V().has('p1',null), "g.V().has('p1',null)"],
      // #103
      [g.V(new Vertex(0,'person')), 'g.V(0)'],
      // #104 - Map with enum key
      [g.inject(new Map([[T.id, 0], [T.label, 'person'], ['name', 'stephen'], ['age', 32]])), "g.inject([(T.id):0,(T.label):'person','name':'stephen','age':32])"],
      // #105 - chained or predicates
      [g.V().has('age',P.gt(5).or(P.lt(2)).or(P.eq(3))), "g.V().has('age',gt(5).or(lt(2)).or(eq(3)))"],
      // #106 - complex nested and/or predicates
      [g.V().has('age',P.gt(5).and(P.lt(10)).or(P.eq(15))), "g.V().has('age',gt(5).and(lt(10)).or(eq(15)))"],
      // #107
      [g.inject(new Set(['a'])), "g.inject({'a'})"],
      // #108
      [g.mergeV(null), 'g.mergeV(null)'],
      // #109
      [g.mergeE(null), 'g.mergeE(null)'],
      // #110 - double quote in string
      [g.addV('"test"'), "g.addV('\\\"test\\\"')"],
      // #111 - mixed quotes
      [g.addV("t'\"est"), "g.addV('t\\'\\\"est')"],
      // #112 - newline in string
      [g.addV('hello\nworld'), "g.addV('hello\\nworld')"],
      // #113 - tab in string
      [g.addV('hello\tworld'), "g.addV('hello\\tworld')"],
      // #114 - carriage return in string
      [g.inject('a\rb'), "g.inject('a\\rb')"],
      // #115 - backspace in string
      [g.inject('a\bb'), "g.inject('a\\bb')"],
      // #116 - form feed in string
      [g.inject('a\fb'), "g.inject('a\\fb')"],
      // #117 - null char in string
      [g.inject('a\x00b'), "g.inject('a\\u0000b')"],
      // #118 - datetime with non-zero milliseconds
      [g.inject(new Date('2018-03-21T08:35:44.741Z')), 'g.inject(datetime("2018-03-21T08:35:44.741Z"))'],
      // #119 - withoutStrategies
      [g.withoutStrategies(ReadOnlyStrategy).V(), 'g.withoutStrategies(ReadOnlyStrategy).V()'],
      // #120 - SeedStrategy
      [g.withStrategies(new SeedStrategy({seed: 999999})).V(), 'g.withStrategies(new SeedStrategy(seed:999999)).V()'],
      // #121 - P.within with empty list
      [g.V().has('code',P.within([])), "g.V().has('code',within([]))"],
      // #122 - P.not
      [g.V().has('age',P.not(P.eq(5))), "g.V().has('age',not(eq(5)))"],
      // #123 - Direction aliases from_ and to mapped to OUT and IN
      [g.V().to(direction.from_, 'knows').to(direction.in, 'created'),
       "g.V().to(Direction.OUT,'knows').to(Direction.IN,'created')"],
      // #124 - from_/to with Vertex ID extraction
      [g.addE('knows').from_(new Vertex(1, 'person')).to(new Vertex(2, 'person')),
       "g.addE('knows').from(1).to(2)"],
      // #125 - mid-traversal V() with Vertex ID extraction
      [g.inject('foo').V(1, new Vertex(2, 'person')),
       "g.inject('foo').V(1,2)"],
      // #126 - mergeE with Vertex ID extraction in Map (source step)
      [g.mergeE(new Map([['label', 'knows'], [direction.out, new Vertex(1, 'person')], [direction.in, new Vertex(2, 'person')]])),
       "g.mergeE(['label':'knows',(Direction.OUT):1,(Direction.IN):2])"],
      // #127 - mergeE with Vertex ID extraction in Map (mid-traversal)
      [g.inject('foo').mergeE(new Map([['label', 'knows'], [direction.out, new Vertex(1, 'person')], [direction.in, new Vertex(2, 'person')]])),
       "g.inject('foo').mergeE(['label':'knows',(Direction.OUT):1,(Direction.IN):2])"],
    ];

    tests.forEach(([traversal, expected], index) => {
      it(`test #${index}: ${expected}`, function () {
        assert.strictEqual(traversal.getGremlinLang().getGremlin(), expected);
      });
    });
  });

  describe('JS-specific tests', function () {
    it('should handle Long values', function () {
      assert.strictEqual(g.V(new Long('9007199254740993')).getGremlinLang().getGremlin(), 'g.V(9007199254740993L)');
    });

    it('should handle NaN', function () {
      assert.strictEqual(g.inject(NaN).getGremlinLang().getGremlin(), 'g.inject(NaN)');
    });

    it('should handle Infinity', function () {
      assert.strictEqual(g.inject(Infinity).getGremlinLang().getGremlin(), 'g.inject(+Infinity)');
    });

    it('should handle -Infinity', function () {
      assert.strictEqual(g.inject(-Infinity).getGremlinLang().getGremlin(), 'g.inject(-Infinity)');
    });

    it('source operations should not mutate original g', function () {
      const g2 = g.withSack(0);
      assert.strictEqual(g.V().getGremlinLang().getGremlin(), 'g.V()');
      assert.strictEqual(g2.V().getGremlinLang().getGremlin(), 'g.withSack(0).V()');
    });

    it('should handle clone isolation', function () {
      const t1 = g.V().has('name','josh');
      const t2 = t1.clone().out('created');
      assert.strictEqual(t1.getGremlinLang().getGremlin(), "g.V().has('name','josh')");
      assert.strictEqual(t2.getGremlinLang().getGremlin(), "g.V().has('name','josh').out('created')");
    });

    it('should handle anonymous traversal prefix', function () {
      const t = __.out('knows');
      assert.strictEqual(t.getGremlinLang().getGremlin('__'), "__.out('knows')");
    });

    it('should handle OptionsStrategy extraction', function () {
      const t = g.with_('evaluationTimeout', 500).V().count();
      assert.strictEqual(t.getGremlinLang().getGremlin(), 'g.V().count()');
      assert.strictEqual(t.getGremlinLang().getOptionsStrategies().length, 1);
    });

    it('should handle empty Set', function () {
      assert.strictEqual(g.inject(new Set()).getGremlinLang().getGremlin(), 'g.inject({})');
    });

    it('should handle empty Map', function () {
      assert.strictEqual(g.inject(new Map()).getGremlinLang().getGremlin(), 'g.inject([:])'); 
    });

    it('should handle inject with array', function () {
      assert.strictEqual(g.inject([3,4,5]).getGremlinLang().getGremlin(), 'g.inject([3,4,5])');
    });

    it('should handle iterate/discard', function () {
      const t = g.V().count();
      t.iterate();
      assert.strictEqual(t.getGremlinLang().getGremlin(), 'g.V().count().discard()');
    });

    it('should handle CardinalityValue.list', function () {
      assert.strictEqual(
        g.inject(new Map([['age', CardinalityValue.list(33)]])).getGremlinLang().getGremlin(),
        "g.inject(['age':Cardinality.list(33)])"
      );
    });

    it('should handle CardinalityValue.set', function () {
      assert.strictEqual(
        g.inject(new Map([['age', CardinalityValue.set(33)]])).getGremlinLang().getGremlin(),
        "g.inject(['age':Cardinality.set(33)])"
      );
    });

    it('should handle CardinalityValue.single', function () {
      assert.strictEqual(
        g.inject(new Map([['age', CardinalityValue.single(33)]])).getGremlinLang().getGremlin(),
        "g.inject(['age':Cardinality.single(33)])"
      );
    });
  });
});