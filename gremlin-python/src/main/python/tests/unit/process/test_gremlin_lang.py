# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
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
import uuid

from gremlin_python.process.strategies import ReadOnlyStrategy, SubgraphStrategy, OptionsStrategy, PartitionStrategy
from gremlin_python.process.traversal import within, eq, T, Order, Scope, Column, Operator, P, Pop, Cardinality, \
    between, inside, WithOptions, ShortestPath, starting_with, ending_with, containing, gt, lte, GValue
from gremlin_python.statics import SingleByte, short, long, bigint, BigDecimal, SingleChar
from gremlin_python.structure.graph import Graph, Vertex
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from datetime import datetime, timezone, timedelta


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
        # 37 - removed as store() was deprecated and removed in 3.8.0
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
                      "g.V(0)"])

        # 107
        tests.append([g.inject({'name': 'java'}, {T.id: 0}, {},
                               {'age': float(10), 'pos_inf': float("inf"), 'neg_inf': float("-inf"),
                                'nan': float("nan")}),
                      "g.inject(['name':'java'],[(T.id):0],[:],['age':10.0D,'pos_inf':+Infinity,'neg_inf':-Infinity,'nan':NaN])"])

        # 108
        tests.append([g.inject(float(1)).is_(P.eq(1).or_(P.gt(2)).or_(P.lte(3)).or_(P.gte(4))),
                      "g.inject(1.0D).is(eq(1).or(gt(2)).or(lte(3)).or(gte(4)))"])

        # 109
        tests.append([g.V().has_label('person').has('age', P.gt(10).or_(P.gte(11).and_(P.lt(20))).and_(
            P.lt(29).or_(P.eq(35)))).name,
                      "g.V().hasLabel('person').has('age',gt(10).or(gte(11).and(lt(20))).and(lt(29).or(eq(35)))).values('name')"])

        # 110
        tests.append([g.inject(set('a')),
                      "g.inject({'a'})"])

        # 111
        tests.append([g.merge_v(None),
                      "g.mergeV(null)"])

        # 112
        tests.append([g.merge_e(None),
                      "g.mergeE(null)"])

        # 113
        tests.append([g.add_v('\"test\"'),
                      "g.addV('\"test\"')"])

        # 114
        tests.append([g.add_v('t\'"est'),
                      "g.addV('t\\'\"est')"])

        # 115
        tests.append([g.inject(True, SingleByte(1), short(2), 3, long(4), float(5),
                               float(6), bigint(7), BigDecimal(0, 8)),
                      "g.inject(true,1B,2S,3,4L,5.0D,6.0D,7N,8M)"])

        # 116
        tests.append([g.inject(uuid.UUID('9b8d8a9c-61c2-43e5-9cc8-c27b9261290e')),
                      'g.inject(UUID("9b8d8a9c-61c2-43e5-9cc8-c27b9261290e"))'])

        # 117
        tests.append([g.add_v('test').property('date', datetime(2021, 2, 1, 9, 30, tzinfo=timezone.utc)),
                      "g.addV('test').property('date',datetime(\"2021-02-01T09:30:00+00:00\"))"])

        # 118
        tests.append([g.add_v('test').property('date', datetime(2021, 2, 1, 9, 30, tzinfo=timezone(timedelta(hours=7)))),
                      "g.addV('test').property('date',datetime(\"2021-02-01T09:30:00+07:00\"))"])

        # 119
        tests.append([g.add_v('test').property('date', datetime(2021, 2, 1, 9, 30, tzinfo=timezone(timedelta(hours=-5)))),
                      "g.addV('test').property('date',datetime(\"2021-02-01T09:30:00-05:00\"))"])

        # 120 - Character backslash (not in feature file due to Gherkin escaping issues)
        tests.append([g.inject(SingleChar('\\')),
                      "g.inject('\\\\'c)"])

        # 121 - Character single quote (no feature equivalent)
        tests.append([g.inject(SingleChar("'")),
                      "g.inject(\"'\"c)"])

        for t in range(len(tests)):
            gremlin_lang = tests[t][0].gremlin_lang.get_gremlin()
            assert gremlin_lang == tests[t][1]

    def test_gvalue_name_cannot_be_null(self):
        try:
            GValue(None, [1, 2, 3])
            assert False, 'expected exception for null name'
        except Exception as ex:
            assert str(ex) == 'GValue name cannot be null.'

    def test_gvalue_name_mid_string_dollar_accepted(self):
        assert GValue('a$b', [1, 2, 3]).get_name() == 'a$b'

    def test_gvalue_name_unicode_letter_accepted(self):
        g = traversal().with_(None)
        p = GValue('café', 42)
        gremlin = g.V(p).gremlin_lang
        assert 'g.V(café)' == gremlin.get_gremlin()
        assert 42 == gremlin.get_parameters().get('café')

    def test_gvalue_underscore_name_accepted(self):
        assert GValue('_1', [1, 2, 3]).get_name() == '_1'

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

    def test_gvalue_nested_in_child_traversal(self):
        g = traversal().with_(None)
        gremlin = g.V().where(__.is_(GValue('xx1', 1))).gremlin_lang
        assert 'g.V().where(__.is(xx1))' == gremlin.get_gremlin()
        assert 1 == gremlin.get_parameters().get('xx1')

    def test_gvalue_nested_across_multiple_child_traversals(self):
        g = traversal().with_(None)
        gremlin = g.V().union(__.V(GValue('vid1', 1)), __.V(GValue('vid4', 4))).gremlin_lang
        assert 'g.V().union(__.V(vid1),__.V(vid4))' == gremlin.get_gremlin()
        assert 1 == gremlin.get_parameters().get('vid1')
        assert 4 == gremlin.get_parameters().get('vid4')

    def test_gvalue_mid_string_underscore_accepted(self):
        g = traversal().with_(None)
        p = GValue('a_b', 42)
        gremlin = g.V(p).gremlin_lang
        assert 'g.V(a_b)' == gremlin.get_gremlin()
        assert 42 == gremlin.get_parameters().get('a_b')

    def test_gvalue_underscore_start_name_in_traversal(self):
        g = traversal().with_(None)
        gremlin = g.V(GValue('_1', [1, 2, 3])).gremlin_lang
        assert 'g.V(_1)' == gremlin.get_gremlin()
        assert [1, 2, 3] == gremlin.get_parameters().get('_1')

    def test_gvalue_dollar_name_in_traversal(self):
        g = traversal().with_(None)
        gremlin = g.V(GValue('a$b', 1)).gremlin_lang
        assert 'g.V(a$b)' == gremlin.get_gremlin()
        assert 1 == gremlin.get_parameters().get('a$b')

    def test_gvalue_invalid_name_raises_in_traversal(self):
        import pytest
        g = traversal().with_(None)
        with pytest.raises(Exception, match='Invalid parameter name'):
            g.V(GValue('1a', 1)).gremlin_lang.get_gremlin()

    def test_gvalue_construction_and_accessors(self):
        p = GValue('x', 1)
        assert 'x' == p.get_name()
        assert 1 == p.get()
        assert p.is_null() is False

    def test_gvalue_is_null(self):
        p = GValue('n', None)
        assert p.is_null() is True
        q = GValue('m', 0)
        assert q.is_null() is False

    def test_gvalue_string_representation(self):
        p = GValue('x', 1)
        assert repr(p) == 'x=1'
        assert str(p) == 'x=1'

    def test_gvalue_cannot_be_nested(self):
        try:
            GValue('x', GValue('y', 1))
        except Exception as ex:
            assert str(ex) == 'GValues cannot be nested'

    def test_unsupported_type_throws(self):
        g = traversal().with_(None)
        import pytest
        with pytest.raises(TypeError, match='cannot be represented as text'):
            g.inject(object()).gremlin_lang.get_gremlin()

    def test_unsupported_type_in_list_throws(self):
        g = traversal().with_(None)
        import pytest
        with pytest.raises(TypeError, match='cannot be represented as text'):
            g.inject([1, object()]).gremlin_lang.get_gremlin()

    def test_unsupported_type_in_dict_throws(self):
        g = traversal().with_(None)
        import pytest
        with pytest.raises(TypeError, match='cannot be represented as text'):
            g.inject({'key': object()}).gremlin_lang.get_gremlin()

    def test_unsupported_type_in_convert_parameters_to_string_throws(self):
        from gremlin_python.process.traversal import GremlinLang
        import pytest
        with pytest.raises(TypeError, match='cannot be represented as text'):
            GremlinLang.convert_parameters_to_string({'x': object()})

    def test_convert_parameters_to_string_empty(self):
        from gremlin_python.process.traversal import GremlinLang
        assert '[:]' == GremlinLang.convert_parameters_to_string({})
        assert '[:]' == GremlinLang.convert_parameters_to_string(None)

    def test_convert_parameters_to_string_basic(self):
        from gremlin_python.process.traversal import GremlinLang
        result = GremlinLang.convert_parameters_to_string({'x': 1, 'name': 'marko'})
        assert result.startswith('[')
        assert result.endswith(']')
        assert "'x':1" in result
        assert "'name':'marko'" in result

    def test_convert_parameters_to_string_nested_list(self):
        from gremlin_python.process.traversal import GremlinLang
        result = GremlinLang.convert_parameters_to_string({'ids': [1, 2, 3]})
        assert "'ids':[1,2,3]" in result

    def test_convert_parameters_to_string_escaped_string(self):
        from gremlin_python.process.traversal import GremlinLang
        result = GremlinLang.convert_parameters_to_string({'name': "it's a test"})
        assert "'name'" in result
        assert "it" in result

    def test_provider_defined_auto_dehydration(self):
        from gremlin_python.structure.graph import ProviderDefinedType, provider_defined
        g = traversal().with_(None)

        @provider_defined(name="com.example.Point")
        class Point:
            def __init__(self, x, y):
                self.x = x
                self.y = y

        p = Point(1, 2)
        gremlin = g.inject(p).gremlin_lang.get_gremlin()
        assert "PDT('com.example.Point',['x':1,'y':2])" in gremlin

    def test_pdt_adapter_takes_precedence_over_decorator(self):
        from gremlin_python.structure.graph import ProviderDefinedTypeRegistry, provider_defined
        g = traversal().with_(None)

        @provider_defined(name="com.example.Point")
        class Point:
            def __init__(self, x, y):
                self.x = x
                self.y = y

        registry = ProviderDefinedTypeRegistry()
        registry.register("com.adapter.Point",
                          deserialize_fn=lambda fields: Point(fields["a"], fields["b"]),
                          serialize_fn=lambda p: {"a": p.x, "b": p.y},
                          target_class=Point)
        g.gremlin_lang.pdt_registry = registry

        p = Point(3, 4)
        gremlin = g.inject(p).gremlin_lang.get_gremlin()
        # The adapter's type_name and fields must win over the decorator's
        assert "PDT('com.adapter.Point',['a':3,'b':4])" in gremlin

    def test_pdt_special_characters_in_name(self):
        from gremlin_python.structure.graph import ProviderDefinedType
        g = traversal().with_(None)

        pdt = ProviderDefinedType('say"hello"', {'v': 1})
        gremlin = g.inject(pdt).gremlin_lang.get_gremlin()
        assert "PDT('say\"hello\"',['v':1])" in gremlin

        pdt2 = ProviderDefinedType('back\\slash', {'v': 1})
        gremlin2 = g.inject(pdt2).gremlin_lang.get_gremlin()
        assert "PDT('back\\\\slash',['v':1])" in gremlin2

    def test_pdt_nested(self):
        from gremlin_python.structure.graph import ProviderDefinedType
        g = traversal().with_(None)

        inner = ProviderDefinedType('Inner', {'v': 1})
        outer = ProviderDefinedType('Outer', {'inner': inner})
        gremlin = g.inject(outer).gremlin_lang.get_gremlin()
        assert "PDT('Outer',['inner':PDT('Inner',['v':1])])" in gremlin

    def test_dehydrate_inner_decorated_in_unregistered_outer(self):
        """A @provider_defined inner object must dehydrate to PDT form even when nested
        as a field value inside a raw (unregistered/undecorated) ProviderDefinedType."""
        from gremlin_python.structure.graph import ProviderDefinedType, provider_defined
        g = traversal().with_(None)

        @provider_defined(name="com.example.Inner")
        class Inner:
            def __init__(self, val):
                self.val = val

        inner_obj = Inner(99)
        # Outer is a raw ProviderDefinedType — no adapter, no decorator
        outer_pdt = ProviderDefinedType("com.example.Outer", {"child": inner_obj, "count": 7})

        gremlin = g.inject(outer_pdt).gremlin_lang.get_gremlin()
        # The inner decorated object MUST be dehydrated to its PDT representation
        assert "PDT('com.example.Outer',['child':PDT('com.example.Inner',['val':99]),'count':7])" in gremlin

    def test_match_gql_query_bytecode(self):
        g = traversal().with_(None)
        query = 'MATCH (p:person)-[e:knows]->(friend:person)'
        assert ("g.match('" + query + "')") == g.match(query).gremlin_lang.get_gremlin()
        params = {'name': 'marko'}
        assert ("g.match('" + query + "',['name':'marko'])") == g.match(query, params).gremlin_lang.get_gremlin()
