# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""
Unit tests for the Translator Class.
"""
__author__ = 'Kelvin R. Lawrence (gfxman)'

from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.traversal import *
from gremlin_python.process.strategies import *
from gremlin_python.process.translator import *
from datetime import datetime


class TestTraversalStrategies(object):

    def test_translations(self):
        g = traversal().withGraph(Graph())

        tests = list()
        # 0
        tests.append([g.V(),
                     "g.V()"])
        # 1
        tests.append([g.V('1', '2', '3', '4'),
                     "g.V('1','2','3','4')"])
        # 2
        tests.append([g.V('3').valueMap(True),
                     "g.V('3').valueMap(True)"])
        # 3
        tests.append([g.V().constant(5),
                     "g.V().constant(5)"])
        # 4
        tests.append([g.V().constant(1.5),
                     "g.V().constant(1.5)"])
        # 5
        tests.append([g.V().constant('Hello'),
                     "g.V().constant('Hello')"])
        # 6
        tests.append([g.V().hasLabel('airport').limit(5),
                     "g.V().hasLabel('airport').limit(5)"])
        # 7
        tests.append([g.V().hasLabel(within('a', 'b', 'c')),
                     "g.V().hasLabel(within(['a','b','c']))"])
        # 8
        tests.append([g.V().hasLabel('airport', 'continent').out().limit(5),
                     "g.V().hasLabel('airport','continent').out().limit(5)"])
        # 9
        tests.append([g.V().hasLabel('airport').out().values('code').limit(5),
                     "g.V().hasLabel('airport').out().values('code').limit(5)"])
        # 10
        tests.append([g.V('3').as_('a').out('route').limit(10).where(eq('a')).by('region'),
                     "g.V('3').as('a').out('route').limit(10).where(eq('a')).by('region')"])
        # 11
        tests.append([g.V('3').repeat(__.out('route').simplePath()).times(2).path().by('code'),
                     "g.V('3').repeat(__.out('route').simplePath()).times(2).path().by('code')"])
        # 12
        tests.append([g.V().hasLabel('airport').out().has('region', 'US-TX').values('code').limit(5),
                     "g.V().hasLabel('airport').out().has('region','US-TX').values('code').limit(5)"])
        # 13
        tests.append([g.V().hasLabel('airport').union(__.values('city'), __.values('region')).limit(5),
                     "g.V().hasLabel('airport').union(__.values('city'),__.values('region')).limit(5)"])
        # 14
        tests.append([g.V('3').as_('a').out('route', 'routes'),
                     "g.V('3').as('a').out('route','routes')"])
        # 15
        tests.append([g.V().where(__.values('runways').is_(5)),
                    "g.V().where(__.values('runways').is(5))"])
        # 16
        tests.append([g.V('3').repeat(__.out().simplePath()).until(__.has('code', 'AGR')).path().by('code').limit(5),
                     "g.V('3').repeat(__.out().simplePath()).until(__.has('code','AGR')).path().by('code').limit(5)"])
        # 17
        tests.append([g.V().hasLabel('airport').order().by(__.id()),
                     "g.V().hasLabel('airport').order().by(__.id())"])
        # 18
        tests.append([g.V().hasLabel('airport').order().by(T.id),
                     "g.V().hasLabel('airport').order().by(T.id)"])
        # 19
        tests.append([g.V().hasLabel('airport').order().by(__.id(),Order.desc),
                     "g.V().hasLabel('airport').order().by(__.id(),Order.desc)"])
        # 20
        tests.append([g.V().hasLabel('airport').order().by('code',Order.desc),
                     "g.V().hasLabel('airport').order().by('code',Order.desc)"])
        # 21
        tests.append([g.V('1', '2', '3').local(__.out().out().dedup().fold()),
                     "g.V('1','2','3').local(__.out().out().dedup().fold())"])
        # 22
        tests.append([g.V('3').out().path().count(Scope.local),
                     "g.V('3').out().path().count(Scope.local)"])
        # 23
        tests.append([g.E().count(),
                     "g.E().count()"])
        # 24
        tests.append([g.V('5').outE('route').inV().path().limit(10),
                     "g.V('5').outE('route').inV().path().limit(10)"])
        # 25
        tests.append([g.V('5').propertyMap().select(Column.keys),
                     "g.V('5').propertyMap().select(Column.keys)"])
        # 26
        tests.append([g.V('5').propertyMap().select(Column.values),
                     "g.V('5').propertyMap().select(Column.values)"])
        # 27
        tests.append([g.V('3').values('runways').math('_ + 1'),
                     "g.V('3').values('runways').math('_ + 1')"])
        # 28
        tests.append([g.V('3').emit().repeat(__.out().simplePath()).times(3).limit(5).path(),
                     "g.V('3').emit().repeat(__.out().simplePath()).times(3).limit(5).path()"])
        # 29
        tests.append([g.V().match(__.as_('a').has('code', 'LHR').as_('b')).select('b').by('code'),
                     "g.V().match(__.as('a').has('code','LHR').as('b')).select('b').by('code')"])
        # 30
        tests.append([g.V().has('test-using-keyword-as-property','repeat'),
                     "g.V().has('test-using-keyword-as-property','repeat')"])
        # 31
        tests.append([g.V('1').addE('test').to(__.V('4')),
                     "g.V('1').addE('test').to(__.V('4'))"])
        # 32
        tests.append([g.V().values('runways').max(),
                     "g.V().values('runways').max()"])
        # 33
        tests.append([g.V().values('runways').min(),
                     "g.V().values('runways').min()"])
        # 34
        tests.append([g.V().values('runways').sum(),
                     "g.V().values('runways').sum()"])
        # 35
        tests.append([g.V().values('runways').mean(),
                     "g.V().values('runways').mean()"])
        # 36
        tests.append([g.withSack(0).V('3', '5').sack(Operator.sum).by('runways').sack(),
                     "g.withSack(0).V('3','5').sack(Operator.sum).by('runways').sack()"])
        # 37
        tests.append([g.V('3').values('runways').store('x').V('4').values('runways').store('x').by(__.constant(1)).V('6').store('x').by(__.constant(1)).select('x').unfold().sum(),
                     "g.V('3').values('runways').store('x').V('4').values('runways').store('x').by(__.constant(1)).V('6').store('x').by(__.constant(1)).select('x').unfold().sum()"])
        # 38
        tests.append([g.inject(3, 4, 5),
                     "g.inject(3,4,5)"])
        # 39
        tests.append([g.inject([3, 4, 5]),
                     "g.inject([3, 4, 5])"])
        # 40
        tests.append([g.inject(3, 4, 5).count(),
                     "g.inject(3,4,5).count()"])
        # 41
        tests.append([g.V().has('runways', gt(5)).count(),
                     "g.V().has('runways',gt(5)).count()"])
        # 42
        tests.append([g.V().has('runways', lte(5.3)).count(),
                     "g.V().has('runways',lte(5.3)).count()"])
        # 43
        tests.append([g.V().has('code', within(123,124)),
                     "g.V().has('code',within([123,124]))"])
        # 44
        tests.append([g.V().has('code', within(123, 'abc')),
                     "g.V().has('code',within([123,'abc']))"])
        # 45
        tests.append([g.V().has('code', within('abc', 123)),
                     "g.V().has('code',within(['abc',123]))"])
        # 46
        tests.append([g.V().has('code', within('abc', 'xyz')),
                     "g.V().has('code',within(['abc','xyz']))"])
        # 47
        tests.append([g.V('1', '2').has('region', P.within('US-TX','US-GA')),
                     "g.V('1','2').has('region',within(['US-TX','US-GA']))"])
        # 48
        tests.append([g.V().and_(__.has('runways', P.gt(5)), __.has('region','US-TX')),
                     "g.V().and(__.has('runways',gt(5)),__.has('region','US-TX'))"])
        # 49
        tests.append([g.V().union(__.has('runways', gt(5)), __.has('region','US-TX')),
                     "g.V().union(__.has('runways',gt(5)),__.has('region','US-TX'))"])
        # 50
        tests.append([g.V('3').choose(__.values('runways').is_(3),__.constant('three'),__.constant('not three')),
                     "g.V('3').choose(__.values('runways').is(3),__.constant('three'),__.constant('not three'))"])
        # 51
        tests.append([g.V('3').choose(__.values('runways')).option(1,__.constant('three')).option(2,__.constant('not three')),
                     "g.V('3').choose(__.values('runways')).option(1,__.constant('three')).option(2,__.constant('not three'))"])
        # 52
        tests.append([g.V('3').choose(__.values('runways')).option(1.5,__.constant('one and a half')).option(2,__.constant('not three')),
                     "g.V('3').choose(__.values('runways')).option(1.5,__.constant('one and a half')).option(2,__.constant('not three'))"])
        # 53
        tests.append([g.V('3').repeat(__.out().simplePath()).until(__.loops().is_(1)).count(),
                     "g.V('3').repeat(__.out().simplePath()).until(__.loops().is(1)).count()"])
        # 54
        tests.append([g.V().hasLabel('airport').limit(20).group().by('region').by('code').order(Scope.local).by(Column.keys),
                     "g.V().hasLabel('airport').limit(20).group().by('region').by('code').order(Scope.local).by(Column.keys)"])
        # 55
        tests.append([g.V('1').as_('a').V('2').as_('a').select(Pop.all_, 'a'),
                     "g.V('1').as('a').V('2').as('a').select(Pop.all,'a')"])
        # 56
        tests.append([g.addV('test').property(Cardinality.set_, 'p1', 10),
                     "g.addV('test').property(Cardinality.set,'p1',10)"])
        # 57
        tests.append([g.addV('test').property(Cardinality.list_, 'p1', 10),
                     "g.addV('test').property(Cardinality.list,'p1',10)"])

        # 58
        tests.append([g.addV('test').property(Cardinality.single, 'p1', 10),
                     "g.addV('test').property(Cardinality.single,'p1',10)"])
        # 59
        tests.append([g.V().limit(5).order().by(T.label),
                     "g.V().limit(5).order().by(T.label)"])

        # 60
        tests.append([g.V().range(1, 5),
                     "g.V().range(1,5)"])

        # 61
        tests.append([g.addV('test').property('p1', 123),
                     "g.addV('test').property('p1',123)"])

        # 62
        tests.append([g.addV('test').property('date',datetime(2021, 2, 1, 9, 30)),
                     "g.addV('test').property('date',new Date(121,2,1,9,30,0))"])
        # 63
        tests.append([g.addV('test').property('date',datetime(2021, 2, 1)),
                     "g.addV('test').property('date',new Date(121,2,1,0,0,0))"])
        # 64
        tests.append([g.addE('route').from_(__.V('1')).to(__.V('2')),
                     "g.addE('route').from(__.V('1')).to(__.V('2'))"])
        # 65
        tests.append([g.withSideEffect('a', [1, 2]).V('3').select('a'),
                     "g.withSideEffect('a',[1, 2]).V('3').select('a')"])
        # 66
        tests.append([g.withSideEffect('a', 1).V('3').select('a'),
                     "g.withSideEffect('a',1).V('3').select('a')"])
        # 67
        tests.append([g.withSideEffect('a', 'abc').V('3').select('a'),
                     "g.withSideEffect('a','abc').V('3').select('a')"])
        # 68
        tests.append([g.V().has('airport', 'region', 'US-NM').limit(3).values('elev').fold().index(),
                     "g.V().has('airport','region','US-NM').limit(3).values('elev').fold().index()"])
        # 69
        tests.append([g.V('3').repeat(__.timeLimit(1000).out().simplePath()).until(__.has('code', 'AGR')).path(),
                     "g.V('3').repeat(__.timeLimit(1000).out().simplePath()).until(__.has('code','AGR')).path()"])

        # 70
        tests.append([g.V().hasLabel('airport').where(__.values('elev').is_(gt(14000))),
                     "g.V().hasLabel('airport').where(__.values('elev').is(gt(14000)))"])

        # 71
        tests.append([g.V().hasLabel('airport').where(__.out().count().is_(gt(250))).values('code'),
                     "g.V().hasLabel('airport').where(__.out().count().is(gt(250))).values('code')"])

        # 72
        tests.append([g.V().hasLabel('airport').filter(__.out().count().is_(gt(250))).values('code'),
                     "g.V().hasLabel('airport').filter(__.out().count().is(gt(250))).values('code')"])
        # 73
        tests.append([g.withSack(0).
                        V('3').
                        repeat(__.outE('route').sack(Operator.sum).by('dist').inV()).
                        until(__.has('code', 'AGR').or_().loops().is_(4)).
                        has('code', 'AGR').
                        local(__.union(__.path().by('code').by('dist'),__.sack()).fold()).
                        limit(10),
                     "g.withSack(0).V('3').repeat(__.outE('route').sack(Operator.sum).by('dist').inV()).until(__.has('code','AGR').or().loops().is(4)).has('code','AGR').local(__.union(__.path().by('code').by('dist'),__.sack()).fold()).limit(10)"])

        # 74
        tests.append([g.addV().as_('a').addV().as_('b').addE('knows').from_('a').to('b'),
                     "g.addV().as('a').addV().as('b').addE('knows').from('a').to('b')"])

        # 75
        tests.append([g.addV('Person').as_('a').addV('Person').as_('b').addE('knows').from_('a').to('b'),
                     "g.addV('Person').as('a').addV('Person').as('b').addE('knows').from('a').to('b')"])
        # 76
        tests.append([g.V('3').project('Out','In').by(__.out().count()).by(__.in_().count()),
                     "g.V('3').project('Out','In').by(__.out().count()).by(__.in().count())"])
        # 77
        tests.append([g.V('44').out().aggregate('a').out().where(within('a')).path(),
                     "g.V('44').out().aggregate('a').out().where(within(['a'])).path()"])
        # 78
        tests.append([g.V().has('date', datetime(2021, 2, 22)),
                     "g.V().has('date',new Date(121,2,22,0,0,0))"])
        # 79
        tests.append([g.V().has('date', within(datetime(2021, 2, 22), datetime(2021, 1, 1))),
                      "g.V().has('date',within([new Date(121,2,22,0,0,0),new Date(121,1,1,0,0,0)]))"])
        # 80
        tests.append([g.V().has('date', between(datetime(2021, 1, 1), datetime(2021, 2, 22))),
                                "g.V().has('date',between(new Date(121,1,1,0,0,0),new Date(121,2,22,0,0,0)))"])
        # 81
        tests.append([g.V().has('date', inside(datetime(2021, 1, 1),datetime(2021, 2, 22))),
                                "g.V().has('date',inside(new Date(121,1,1,0,0,0),new Date(121,2,22,0,0,0)))"])
        # 82
        tests.append([g.V().has('date', P.gt(datetime(2021, 1, 1, 9, 30))),
                     "g.V().has('date',gt(new Date(121,1,1,9,30,0)))"])
        # 83
        tests.append([g.V().has('runways', between(3,5)),
                     "g.V().has('runways',between(3,5))"])
        # 84
        tests.append([g.V().has('runways', inside(3,5)),
                     "g.V().has('runways',inside(3,5))"])
        # 85
        tests.append([g.V('44').outE().elementMap(),
                     "g.V('44').outE().elementMap()"])
        # 86
        tests.append([g.V('44').valueMap().by(__.unfold()),
                     "g.V('44').valueMap().by(__.unfold())"])
        # 87
        tests.append([g.V('44').valueMap().with_(WithOptions.tokens,WithOptions.labels),
                     "g.V('44').valueMap().with(WithOptions.tokens,WithOptions.labels)"])
        # 88
        tests.append([g.V('44').valueMap().with_(WithOptions.tokens),
                     "g.V('44').valueMap().with(WithOptions.tokens)"])
        # 89
        tests.append([g.withStrategies(ReadOnlyStrategy()).addV('test'),
                      "g.withStrategies(new ReadOnlyStrategy()).addV('test')"])
        # 90
        strategy = SubgraphStrategy(vertices=__.has('region', 'US-TX'), edges=__.hasLabel('route'))
        tests.append([g.withStrategies(strategy).V().count(),
                    "g.withStrategies(new SubgraphStrategy(vertices:__.has('region','US-TX'),edges:__.hasLabel('route'))).V().count()"])
        # 91
        strategy = SubgraphStrategy(vertex_properties=__.hasNot('runways'))
        tests.append([g.withStrategies(strategy).V().count(),
                      "g.withStrategies(new SubgraphStrategy(vertexProperties:__.hasNot('runways'))).V().count()"])
        # 92
        strategy = SubgraphStrategy(vertices=__.has('region', 'US-TX'),vertex_properties=__.hasNot('runways'))
        tests.append([g.withStrategies(strategy).V().count(),
                      "g.withStrategies(new SubgraphStrategy(vertices:__.has('region','US-TX'),vertexProperties:__.hasNot('runways'))).V().count()"])
        # 93
        strategy = SubgraphStrategy(vertices=__.has('region', 'US-TX'), edges=__.hasLabel('route'))
        tests.append([g.withStrategies(ReadOnlyStrategy(),strategy).V().count(),
                      "g.withStrategies(new ReadOnlyStrategy(),new SubgraphStrategy(vertices:__.has('region','US-TX'),edges:__.hasLabel('route'))).V().count()"])
        # 94
        strategy = SubgraphStrategy(vertices=__.has('region', 'US-TX'))
        tests.append([g.withStrategies(ReadOnlyStrategy(), strategy).V().count(),
                      "g.withStrategies(new ReadOnlyStrategy(),new SubgraphStrategy(vertices:__.has('region','US-TX'))).V().count()"])
        # 95
        tests.append([g.with_('evaluationTimeout', 500).V().count(),
                      "g.withStrategies(new OptionsStrategy(evaluationTimeout:500)).V().count()"])
        # 96
        tests.append([g.withStrategies(OptionsStrategy({'evaluationTimeout': 500})).V().count(),
                     "g.withStrategies(new OptionsStrategy(evaluationTimeout:500)).V().count()"])
        # 97
        tests.append([g.withStrategies(PartitionStrategy(partition_key="partition", write_partition="a", read_partitions=["a"])).addV('test'),
                     "g.withStrategies(new PartitionStrategy(partitionKey:'partition',writePartition:'a',readPartitions:['a'])).addV('test')"])
        # 98
        tests.append([g.withComputer().V().shortestPath().with_(ShortestPath.target, __.has('name','peter')),
                     "g.withStrategies(new VertexProgramStrategy()).V().shortestPath().with('~tinkerpop.shortestPath.target',__.has('name','peter'))"])

        tlr = Translator().of('g')

        for t in range(len(tests)):
            a = tlr.translate(tests[t][0].bytecode)
            assert a == tests[t][1]

    def test_target_language(self):
        tlr = Translator().of('g')
        assert tlr.get_target_language() == 'gremlin-groovy'

    def test_constructor(self):
        tlr = Translator().of('g')
        g = traversal().withGraph(Graph())
        assert tlr.translate(g.V().bytecode) == "g.V()"

    def test_source_name(self):
        tlr = Translator().of('g')
        assert tlr.get_traversal_source() == 'g'
