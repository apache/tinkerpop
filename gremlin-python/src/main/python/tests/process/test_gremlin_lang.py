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
Unit tests for the GremlinLang Class. Modified from deprecated Groovy Translator Tests.
"""
from gremlin_python.process.strategies import ReadOnlyStrategy, SubgraphStrategy, OptionsStrategy, PartitionStrategy
from gremlin_python.process.traversal import within, eq, T, Order, Scope, Column, Operator, P, Pop, Cardinality, \
    between, inside, WithOptions, ShortestPath, starting_with, ending_with, containing, gt, lte, GValue
from gremlin_python.statics import SingleByte, short, long, bigint, BigDecimal
from gremlin_python.structure.graph import Graph, Vertex, Edge, VertexProperty
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from datetime import datetime


class TestGremlinLang(object):

    def test_gremlin_lang(self):
        g = traversal().with_(None)

        tests = list()
        # 0
        tests.append([g.V(),
                      "g.V()"])
        # 1
        tests.append([g.V('1', '2', '3', '4'),
                      "g.V('1','2','3','4')"])
        # 2
        tests.append([g.V('3').value_map(True),
                      "g.V('3').valueMap(true)"])
        # 3
        tests.append([g.V().constant(5),
                      "g.V().constant(5)"])
        # 4
        tests.append([g.V().constant(1.5),
                      "g.V().constant(1.5D)"])
        # 5
        tests.append([g.V().constant('Hello'),
                      "g.V().constant('Hello')"])
        # 6
        tests.append([g.V().has_label('airport').limit(5),
                      "g.V().hasLabel('airport').limit(5)"])
        # 7
        tests.append([g.V().has_label(within('a', 'b', 'c')),
                      "g.V().hasLabel(within(['a','b','c']))"])
        # 8
        tests.append([g.V().has_label('airport', 'continent').out().limit(5),
                      "g.V().hasLabel('airport','continent').out().limit(5)"])
        # 9
        tests.append([g.V().has_label('airport').out().values('code').limit(5),
                      "g.V().hasLabel('airport').out().values('code').limit(5)"])
        # 10
        tests.append([g.V('3').as_('a').out('route').limit(10).where(eq('a')).by('region'),
                      "g.V('3').as('a').out('route').limit(10).where(eq('a')).by('region')"])
        # 11
        tests.append([g.V('3').repeat(__.out('route').simple_path()).times(2).path().by('code'),
                      "g.V('3').repeat(__.out('route').simplePath()).times(2).path().by('code')"])
        # 12
        tests.append([g.V().has_label('airport').out().has('region', 'US-TX').values('code').limit(5),
                      "g.V().hasLabel('airport').out().has('region','US-TX').values('code').limit(5)"])
        # 13
        tests.append([g.V().has_label('airport').union(__.values('city'), __.values('region')).limit(5),
                      "g.V().hasLabel('airport').union(__.values('city'),__.values('region')).limit(5)"])
        # 14
        tests.append([g.V('3').as_('a').out('route', 'routes'),
                      "g.V('3').as('a').out('route','routes')"])
        # 15
        tests.append([g.V().where(__.values('runways').is_(5)),
                      "g.V().where(__.values('runways').is(5))"])
        # 16
        tests.append([g.V('3').repeat(__.out().simple_path()).until(__.has('code', 'AGR')).path().by('code').limit(5),
                      "g.V('3').repeat(__.out().simplePath()).until(__.has('code','AGR')).path().by('code').limit(5)"])
        # 17
        tests.append([g.V().has_label('airport').order().by(__.id_()),
                      "g.V().hasLabel('airport').order().by(__.id())"])
        # 18
        tests.append([g.V().has_label('airport').order().by(T.id),
                      "g.V().hasLabel('airport').order().by(T.id)"])
        # 19
        tests.append([g.V().has_label('airport').order().by(__.id_(), Order.desc),
                      "g.V().hasLabel('airport').order().by(__.id(),Order.desc)"])
        # 20
        tests.append([g.V().has_label('airport').order().by('code', Order.desc),
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
        tests.append([g.V('5').out_e('route').in_v().path().limit(10),
                      "g.V('5').outE('route').inV().path().limit(10)"])
        # 25
        tests.append([g.V('5').property_map().select(Column.keys),
                      "g.V('5').propertyMap().select(Column.keys)"])
        # 26
        tests.append([g.V('5').property_map().select(Column.values),
                      "g.V('5').propertyMap().select(Column.values)"])
        # 27
        tests.append([g.V('3').values('runways').math('_ + 1'),
                      "g.V('3').values('runways').math('_ + 1')"])
        # 28
        tests.append([g.V('3').emit().repeat(__.out().simple_path()).times(3).limit(5).path(),
                      "g.V('3').emit().repeat(__.out().simplePath()).times(3).limit(5).path()"])
        # 29
        tests.append([g.V().match(__.as_('a').has('code', 'LHR').as_('b')).select('b').by('code'),
                      "g.V().match(__.as('a').has('code','LHR').as('b')).select('b').by('code')"])
        # 30
        tests.append([g.V().has('test-using-keyword-as-property', 'repeat'),
                      "g.V().has('test-using-keyword-as-property','repeat')"])
        # 31
        tests.append([g.V('1').add_e('test').to(__.V('4')),
                      "g.V('1').addE('test').to(__.V('4'))"])
        # 32
        tests.append([g.V().values('runways').max_(),
                      "g.V().values('runways').max()"])
        # 33
        tests.append([g.V().values('runways').min_(),
                      "g.V().values('runways').min()"])
        # 34
        tests.append([g.V().values('runways').sum_(),
                      "g.V().values('runways').sum()"])
        # 35
        tests.append([g.V().values('runways').mean(),
                      "g.V().values('runways').mean()"])
        # 36
        tests.append([g.with_sack(0).V('3', '5').sack(Operator.sum_).by('runways').sack(),
                      "g.withSack(0).V('3','5').sack(Operator.sum).by('runways').sack()"])
        # 37
        tests.append([g.V('3').values('runways').store('x').V('4').values('runways').store('x').by(__.constant(1)).V(
            '6').store('x').by(__.constant(1)).select('x').unfold().sum_(),
                      "g.V('3').values('runways').store('x').V('4').values('runways').store('x').by(__.constant(1)).V('6').store('x').by(__.constant(1)).select('x').unfold().sum()"])
        # 38
        tests.append([g.inject(3, 4, 5),
                      "g.inject(3,4,5)"])
        # 39
        tests.append([g.inject([3, 4, 5]),
                      "g.inject([3,4,5])"])
        # 40
        tests.append([g.inject(3, 4, 5).count(),
                      "g.inject(3,4,5).count()"])
        # 41
        tests.append([g.V().has('runways', gt(5)).count(),
                      "g.V().has('runways',gt(5)).count()"])
        # 42
        tests.append([g.V().has('runways', lte(5.3)).count(),
                      "g.V().has('runways',lte(5.3D)).count()"])
        # 43
        tests.append([g.V().has('code', within(123, 124)),
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
        tests.append([g.V('1', '2').has('region', P.within('US-TX', 'US-GA')),
                      "g.V('1','2').has('region',within(['US-TX','US-GA']))"])
        # 48
        tests.append([g.V().and_(__.has('runways', P.gt(5)), __.has('region', 'US-TX')),
                      "g.V().and(__.has('runways',gt(5)),__.has('region','US-TX'))"])
        # 49
        tests.append([g.V().union(__.has('runways', gt(5)), __.has('region', 'US-TX')),
                      "g.V().union(__.has('runways',gt(5)),__.has('region','US-TX'))"])
        # 50
        tests.append([g.V('3').choose(__.values('runways').is_(3), __.constant('three'), __.constant('not three')),
                      "g.V('3').choose(__.values('runways').is(3),__.constant('three'),__.constant('not three'))"])
        # 51
        tests.append(
            [g.V('3').choose(__.values('runways')).option(1, __.constant('three')).option(2, __.constant('not three')),
             "g.V('3').choose(__.values('runways')).option(1,__.constant('three')).option(2,__.constant('not three'))"])
        # 52
        tests.append([g.V('3').choose(__.values('runways')).option(1.5, __.constant('one and a half')).option(2, __.constant('not three')),
                      "g.V('3').choose(__.values('runways')).option(1.5D,__.constant('one and a half')).option(2,__.constant('not three'))"])
        # 53
        tests.append([g.V('3').repeat(__.out().simple_path()).until(__.loops().is_(1)).count(),
                      "g.V('3').repeat(__.out().simplePath()).until(__.loops().is(1)).count()"])
        # 54
        tests.append(
            [g.V().has_label('airport').limit(20).group().by('region').by('code').order(Scope.local).by(Column.keys),
             "g.V().hasLabel('airport').limit(20).group().by('region').by('code').order(Scope.local).by(Column.keys)"])
        # 55
        tests.append([g.V('1').as_('a').V('2').as_('a').select(Pop.all_, 'a'),
                      "g.V('1').as('a').V('2').as('a').select(Pop.all,'a')"])
        # 56
        tests.append([g.add_v('test').property(Cardinality.set_, 'p1', 10),
                      "g.addV('test').property(Cardinality.set,'p1',10)"])
        # 57
        tests.append([g.add_v('test').property(Cardinality.list_, 'p1', 10),
                      "g.addV('test').property(Cardinality.list,'p1',10)"])

        # 58
        tests.append([g.add_v('test').property(Cardinality.single, 'p1', 10),
                      "g.addV('test').property(Cardinality.single,'p1',10)"])
        # 59
        tests.append([g.V().limit(5).order().by(T.label),
                      "g.V().limit(5).order().by(T.label)"])

        # 60
        tests.append([g.V().range_(1, 5),
                      "g.V().range(1,5)"])

        # 61
        tests.append([g.add_v('test').property('p1', 123),
                      "g.addV('test').property('p1',123)"])

        # 62
        tests.append([g.add_v('test').property('date', datetime(2021, 2, 1, 9, 30)),
                      "g.addV('test').property('date',datetime(\"2021-02-01T09:30:00\"))"])
        # 63
        tests.append([g.add_v('test').property('date', datetime(2021, 2, 1)),
                      "g.addV('test').property('date',datetime(\"2021-02-01T00:00:00\"))"])
        # 64
        tests.append([g.add_e('route').from_(__.V('1')).to(__.V('2')),
                      "g.addE('route').from(__.V('1')).to(__.V('2'))"])
        # 65
        tests.append([g.with_side_effect('a', [1, 2]).V('3').select('a'),
                      "g.withSideEffect('a',[1,2]).V('3').select('a')"])
        # 66
        tests.append([g.with_side_effect('a', 1).V('3').select('a'),
                      "g.withSideEffect('a',1).V('3').select('a')"])
        # 67
        tests.append([g.with_side_effect('a', 'abc').V('3').select('a'),
                      "g.withSideEffect('a','abc').V('3').select('a')"])
        # 68
        tests.append([g.V().has('airport', 'region', 'US-NM').limit(3).values('elev').fold().index(),
                      "g.V().has('airport','region','US-NM').limit(3).values('elev').fold().index()"])
        # 69
        tests.append([g.V('3').repeat(__.time_limit(1000).out().simple_path()).until(__.has('code', 'AGR')).path(),
                      "g.V('3').repeat(__.timeLimit(1000).out().simplePath()).until(__.has('code','AGR')).path()"])

        # 70
        tests.append([g.V().has_label('airport').where(__.values('elev').is_(gt(14000))),
                      "g.V().hasLabel('airport').where(__.values('elev').is(gt(14000)))"])

        # 71
        tests.append([g.V().has_label('airport').where(__.out().count().is_(gt(250))).values('code'),
                      "g.V().hasLabel('airport').where(__.out().count().is(gt(250))).values('code')"])

        # 72
        tests.append([g.V().has_label('airport').filter_(__.out().count().is_(gt(250))).values('code'),
                      "g.V().hasLabel('airport').filter(__.out().count().is(gt(250))).values('code')"])
        # 73
        tests.append([g.with_sack(0).
                     V('3').
                     repeat(__.out_e('route').sack(Operator.sum_).by('dist').in_v()).
                     until(__.has('code', 'AGR').or_().loops().is_(4)).
                     has('code', 'AGR').
                     local(__.union(__.path().by('code').by('dist'), __.sack()).fold()).
                     limit(10),
                      "g.withSack(0).V('3').repeat(__.outE('route').sack(Operator.sum).by('dist').inV()).until(__.has('code','AGR').or().loops().is(4)).has('code','AGR').local(__.union(__.path().by('code').by('dist'),__.sack()).fold()).limit(10)"])

        # 74
        tests.append([g.add_v().as_('a').add_v().as_('b').add_e('knows').from_('a').to('b'),
                      "g.addV().as('a').addV().as('b').addE('knows').from('a').to('b')"])

        # 75
        tests.append([g.add_v('Person').as_('a').add_v('Person').as_('b').add_e('knows').from_('a').to('b'),
                      "g.addV('Person').as('a').addV('Person').as('b').addE('knows').from('a').to('b')"])

        # 76
        tests.append([g.V('3').project('Out', 'In').by(__.out().count()).by(__.in_().count()),
                      "g.V('3').project('Out','In').by(__.out().count()).by(__.in().count())"])

        # 77
        tests.append([g.V('44').out().aggregate('a').out().where(within('a')).path(),
                      "g.V('44').out().aggregate('a').out().where(within(['a'])).path()"])

        # 78
        tests.append([g.V().has('date', datetime(2021, 2, 22)),
                      "g.V().has('date',datetime(\"2021-02-22T00:00:00\"))"])

        # 79
        tests.append([g.V().has('date', within(datetime(2021, 2, 22), datetime(2021, 1, 1))),
                      "g.V().has('date',within([datetime(\"2021-02-22T00:00:00\"),datetime(\"2021-01-01T00:00:00\")]))"])

        # 80
        tests.append([g.V().has('date', between(datetime(2021, 1, 1), datetime(2021, 2, 22))),
                      "g.V().has('date',between(datetime(\"2021-01-01T00:00:00\"),datetime(\"2021-02-22T00:00:00\")))"])

        # 81
        tests.append([g.V().has('date', inside(datetime(2021, 1, 1), datetime(2021, 2, 22))),
                      "g.V().has('date',inside(datetime(\"2021-01-01T00:00:00\"),datetime(\"2021-02-22T00:00:00\")))"])

        # 82
        tests.append([g.V().has('date', P.gt(datetime(2021, 1, 1, 9, 30))),
                      "g.V().has('date',gt(datetime(\"2021-01-01T09:30:00\")))"])

        # 83
        tests.append([g.V().has('runways', between(3, 5)),
                      "g.V().has('runways',between(3,5))"])

        # 84
        tests.append([g.V().has('runways', inside(3, 5)),
                      "g.V().has('runways',inside(3,5))"])

        # 85
        tests.append([g.V('44').out_e().element_map(),
                      "g.V('44').outE().elementMap()"])

        # 86
        tests.append([g.V('44').value_map().by(__.unfold()),
                      "g.V('44').valueMap().by(__.unfold())"])

        # 87
        tests.append([g.V('44').value_map().with_(WithOptions.tokens, WithOptions.labels),
                      "g.V('44').valueMap().with('~tinkerpop.valueMap.tokens',2)"])

        # 88
        tests.append([g.V('44').value_map().with_(WithOptions.tokens),
                      "g.V('44').valueMap().with('~tinkerpop.valueMap.tokens')"])

        # 89
        tests.append([g.with_strategies(ReadOnlyStrategy()).add_v('test'),
                      "g.withStrategies(ReadOnlyStrategy).addV('test')"])
        # 90
        strategy = SubgraphStrategy(vertices=__.has('region', 'US-TX'), edges=__.has_label('route'))
        tests.append([g.with_strategies(strategy).V().count(),
                      "g.withStrategies(new SubgraphStrategy(vertices:__.has('region','US-TX'),edges:__.hasLabel('route'))).V().count()"])

        # 91
        strategy = SubgraphStrategy(vertex_properties=__.has_not('runways'))
        tests.append([g.with_strategies(strategy).V().count(),
                      "g.withStrategies(new SubgraphStrategy(vertexProperties:__.hasNot('runways'))).V().count()"])

        # 92
        strategy = SubgraphStrategy(vertices=__.has('region', 'US-TX'), vertex_properties=__.has_not('runways'))
        tests.append([g.with_strategies(strategy).V().count(),
                      "g.withStrategies(new SubgraphStrategy(vertices:__.has('region','US-TX'),vertexProperties:__.hasNot('runways'))).V().count()"])

        # 93
        strategy = SubgraphStrategy(vertices=__.has('region', 'US-TX'), edges=__.has_label('route'))
        tests.append([g.with_strategies(ReadOnlyStrategy(), strategy).V().count(),
                      "g.withStrategies(ReadOnlyStrategy,new SubgraphStrategy(vertices:__.has('region','US-TX'),edges:__.hasLabel('route'))).V().count()"])

        # 94
        strategy = SubgraphStrategy(vertices=__.has('region', 'US-TX'))
        tests.append([g.with_strategies(ReadOnlyStrategy(), strategy).V().count(),
                      "g.withStrategies(ReadOnlyStrategy,new SubgraphStrategy(vertices:__.has('region','US-TX'))).V().count()"])

        # 95 Note with_() options are now extracted into request message and is no longer sent with the script
        tests.append([g.with_('evaluationTimeout', 500).V().count(),
                      "g.V().count()"])

        # 96 Note OptionsStrategy are now extracted into request message and is no longer sent with the script
        tests.append([g.with_strategies(OptionsStrategy(evaluationTimeout=500)).V().count(),
                      "g.V().count()"])

        # 97
        tests.append([g.with_strategies(
            PartitionStrategy(partition_key="partition", write_partition="a", read_partitions=["a"])).add_v('test'),
                      "g.withStrategies(new PartitionStrategy(partitionKey:'partition',writePartition:'a',readPartitions:['a'])).addV('test')"])

        # 98
        tests.append([g.with_computer().V().shortest_path().with_(ShortestPath.target, __.has('name', 'peter')),
                      "g.withStrategies(VertexProgramStrategy).V().shortestPath().with('~tinkerpop.shortestPath.target',__.has('name','peter'))"])

        # 99
        tests.append([g.V().coalesce(__.E(), __.add_e('person')),
                      "g.V().coalesce(__.E(),__.addE('person'))"])

        # 100
        tests.append([g.inject(1).E(),
                      "g.inject(1).E()"])

        # 101
        tests.append([g.V().has("p1", starting_with("foo")),
                      "g.V().has('p1',startingWith('foo'))"])

        # 102
        tests.append([g.V().has("p1", ending_with("foo")),
                      "g.V().has('p1',endingWith('foo'))"])

        # 103
        class SuperStr(str):
            pass

        tests.append([g.V(SuperStr("foo_id")),
                      "g.V('foo_id')"])

        # 104
        tests.append([g.V().has("p1", containing(SuperStr("foo"))),
                      "g.V().has('p1',containing('foo'))"])

        # 105
        tests.append([g.V().has("p1", None),
                      "g.V().has('p1',null)"])

        # 106
        vertex = Vertex(0, "person")
        tests.append([g.V(vertex),
                      "g.V(new ReferenceVertex(0,'person'))"])

        # 107
        outVertex = Vertex(0, "person")
        inVertex = Vertex(1, "person")
        edge = Edge(2, outVertex, "knows", inVertex)
        tests.append([g.E(edge),
                      "g.E(_0)"])

        # 108
        vp = VertexProperty(3, "time", "18:00", None)
        tests.append([g.inject(vp),
                      "g.inject(_1)"])

        # 109
        tests.append([g.inject({'name': 'java'}, {T.id: 0}, {},
                               {'age': float(10), 'pos_inf': float("inf"), 'neg_inf': float("-inf"),
                                'nan': float("nan")}),
                      "g.inject(['name':'java'],[(T.id):0],[:],['age':10.0D,'pos_inf':+Infinity,'neg_inf':-Infinity,'nan':NaN])"])

        # 110
        tests.append([g.inject(float(1)).is_(P.eq(1).or_(P.gt(2)).or_(P.lte(3)).or_(P.gte(4))),
                      "g.inject(1.0D).is(eq(1).or(gt(2)).or(lte(3)).or(gte(4)))"])

        # 111
        tests.append([g.V().has_label('person').has('age', P.gt(10).or_(P.gte(11).and_(P.lt(20))).and_(
            P.lt(29).or_(P.eq(35)))).name,
                      "g.V().hasLabel('person').has('age',gt(10).or(gte(11).and(lt(20))).and(lt(29).or(eq(35)))).values('name')"])

        # 112
        tests.append([g.inject(set('a')),
                      "g.inject({'a'})"])

        # 113
        tests.append([g.merge_v(None),
                      "g.mergeV(null)"])

        # 114
        tests.append([g.merge_e(None),
                      "g.mergeE(null)"])

        # 115
        tests.append([g.add_v('\"test\"'),
                      "g.addV('\"test\"')"])

        # 116
        tests.append([g.add_v('t\'"est'),
                      "g.addV('t\\'\"est')"])

        # 117
        tests.append([g.inject(True, SingleByte(1), short(2), 3, long(4), float(5),
                               float(6), bigint(7), BigDecimal(0, 8)),
                      "g.inject(true,1B,2S,3,4L,5.0D,6.0D,7N,8M)"])

        #118

        for t in range(len(tests)):
            gremlin_lang = tests[t][0].gremlin_lang.get_gremlin()
            assert gremlin_lang == tests[t][1]

    def test_gvalue_name_cannot_be_null(self):
        g = traversal().with_(None)
        try:
            g.V(GValue(None, [1, 2, 3]))
        except Exception as ex:
            assert str(ex) == 'The parameter name cannot be None.'

    def test_gvalue_name_dont_need_escaping(self):
        g = traversal().with_(None)
        try:
            g.V(GValue('\"', [1, 2, 3]))
        except Exception as ex:
            assert str(ex) == 'invalid parameter name ".'

    def test_gvalue_is_not_number(self):
        g = traversal().with_(None)
        try:
            g.V(GValue('1', [1, 2, 3]))
        except Exception as ex:
            assert str(ex) == 'invalid parameter name 1.'

    def test_gvalue_is_valid_identifier(self):
        g = traversal().with_(None)
        try:
            g.V(GValue('1a', [1, 2, 3]))
        except Exception as ex:
            assert str(ex) == 'invalid parameter name 1a.'

    def test_gvalue_is_not_reserved(self):
        g = traversal().with_(None)
        try:
            g.V(GValue('_1', [1, 2, 3]))
        except Exception as ex:
            assert str(ex) == 'invalid GValue name _1. Should not start with _.'

    def test_gvalue_is_not_duplicate(self):
        g = traversal().with_(None)
        try:
            g.inject(GValue('ids', [1, 2])).V(GValue('ids', [2, 3]))
        except Exception as ex:
            assert str(ex) == 'parameter with name ids already exists.'

    def test_gvalue_allow_parameter_reuse(self):
        g = traversal().with_(None)
        val = [1, 2, 3]
        p = GValue('ids', val)
        gremlin = g.inject(p).V(p).gremlin_lang
        assert 'g.inject(ids).V(ids)' == gremlin.get_gremlin()
        assert val == gremlin.get_parameters().get('ids')
