#
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
import os

import pytest

from gremlin_python import statics
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.statics import long
from gremlin_python.process.traversal import TraversalStrategy, P, Order, T, DT, GValue, Cardinality
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.structure.graph import Vertex
from gremlin_python.process.strategies import SubgraphStrategy, SeedStrategy
from gremlin_python.structure.io.util import HashableDict
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.driver import serializer

gremlin_server_url = os.environ.get('GREMLIN_SERVER_URL', 'http://localhost:{}/')
test_no_auth_url = gremlin_server_url.format(45940)


class TestDriverRemoteConnection(object):

    # this is a temporary test for basic graphSONV4 connectivity, once all types are implemented, enable graphSON testing
    # in conftest.py and remove this
    def test_graphSONV4_temp(self):
        remote_conn = DriverRemoteConnection(test_no_auth_url, 'gmodern',
                                             request_serializer=serializer.GraphSONSerializersV4(),
                                             response_serializer=serializer.GraphSONSerializersV4())
        g = traversal().with_(remote_conn)
        assert long(6) == g.V().count().to_list()[0]
        # #
        assert Vertex(1) == g.V(1).next()
        assert Vertex(1) == g.V(Vertex(1)).next()
        assert 1 == g.V(1).id_().next()
        # assert Traverser(Vertex(1)) == g.V(1).nextTraverser()  # TODO check back after bulking added back
        assert 1 == len(g.V(1).to_list())
        assert isinstance(g.V(1).to_list(), list)
        results = g.V().repeat(__.out()).times(2).name
        results = results.to_list()
        assert 2 == len(results)
        assert "lop" in results
        assert "ripple" in results
        # #
        assert 10 == g.V().repeat(__.both()).times(5)[0:10].count().next()
        assert 1 == g.V().repeat(__.both()).times(5)[0:1].count().next()
        assert 0 == g.V().repeat(__.both()).times(5)[0:0].count().next()
        assert 4 == g.V()[2:].count().next()
        assert 2 == g.V()[:2].count().next()
        # #
        results = g.with_side_effect('a', ['josh', 'peter']).V(1).out('created').in_('created').values('name').where(
            P.within('a')).to_list()
        assert 2 == len(results)
        assert 'josh' in results
        assert 'peter' in results
        # #
        results = g.V().has('name', 'peter').as_('a').out('created').as_('b').select('a', 'b').by(
            __.value_map()).to_list()
        assert 1 == len(results)
        assert 'peter' == results[0]['a']['name'][0]
        assert 35 == results[0]['a']['age'][0]
        assert 'lop' == results[0]['b']['name'][0]
        assert 'java' == results[0]['b']['lang'][0]
        assert 2 == len(results[0]['a'])
        assert 2 == len(results[0]['b'])
        # #
        results = g.V(1).inject(g.V(2).next()).values('name').to_list()
        assert 2 == len(results)
        assert 'marko' in results
        assert 'vadas' in results
        # #
        # this test just validates that the underscored versions of steps conflicting with Gremlin work
        # properly and can be removed when the old steps are removed - TINKERPOP-2272
        results = g.V().filter_(__.values('age').sum_().and_(
            __.max_().is_(P.gt(0)), __.min_().is_(P.gt(0)))).range_(0, 1).id_().next()
        assert 1 == results
        # #
        # test dict keys
        # types for dict
        results = g.V().has('person', 'name', 'marko').element_map("name").group_count().next()
        assert {HashableDict.of({T.id: 1, T.label: 'person', 'name': 'marko'}): 1} == results
        results = g.V().has('person', 'name', 'marko').both('knows').group_count().by(__.values('name').fold()).next()
        assert {tuple(['vadas']): 1, tuple(['josh']): 1} == results

    def test_bulk_request_option(self, remote_connection):
        g = traversal().with_(remote_connection)
        result = g.inject(1,2,3,2,1).to_list()
        assert 5 == len(result)
        bulked_results = g.with_("language", "gremlin-lang").with_("bulkResults", True).inject(1,2,3,2,1).to_list()
        assert 5 == len(bulked_results)

    def test_extract_request_options(self, remote_connection):
        g = traversal().with_(remote_connection)
        t = g.with_("evaluationTimeout", 1000).with_("batchSize", 100).V().count()
        assert remote_connection.extract_request_options(t.gremlin_lang) == {'batchSize': 100,
                                                                             'evaluationTimeout': 1000,
                                                                             'bulkResults': True}
        assert 6 == t.to_list()[0]

    @pytest.mark.skip(reason="investigate why 'ids' parameter name fails to parse in gremlin-lang")
    def test_extract_request_options_with_params(self, remote_connection):
        g = traversal().with_(remote_connection)
        t = g.with_("evaluationTimeout",
                    1000).with_("batchSize", 100).with_("userAgent",
                                                        "test").V(GValue('ids', [1, 2, 3])).count()
        assert remote_connection.extract_request_options(t.gremlin_lang) == {'batchSize': 100,
                                                                             'evaluationTimeout': 1000,
                                                                             'userAgent': 'test',
                                                                             'bulkResults': True,
                                                                             'params': {'ids': [1, 2, 3]}}
        assert 3 == t.to_list()[0]

    def test_traversals(self, remote_connection):
        statics.load_statics(globals())
        g = traversal().with_(remote_connection)

        assert long(6) == g.V().count().to_list()[0]
        # #
        assert Vertex(1) == g.V(1).next()
        assert Vertex(1) == g.V(Vertex(1)).next()
        assert 1 == g.V(1).id_().next()
        # assert Traverser(Vertex(1)) == g.V(1).nextTraverser()  # TODO check back after bulking added back
        assert 1 == len(g.V(1).to_list())
        assert isinstance(g.V(1).to_list(), list)
        results = g.V().repeat(__.out()).times(2).name
        results = results.to_list()
        assert 2 == len(results)
        assert "lop" in results
        assert "ripple" in results
        # #
        assert 10 == g.V().repeat(__.both()).times(5)[0:10].count().next()
        assert 1 == g.V().repeat(__.both()).times(5)[0:1].count().next()
        assert 0 == g.V().repeat(__.both()).times(5)[0:0].count().next()
        assert 4 == g.V()[2:].count().next()
        assert 2 == g.V()[:2].count().next()
        # #
        results = g.with_side_effect('a', ['josh', 'peter']).V(1).out('created').in_('created').values('name').where(
            P.within('a')).to_list()
        assert 2 == len(results)
        assert 'josh' in results
        assert 'peter' in results
        # #
        # # todo: enable when profile/explain serialization is implemented
        # results = g.V().out().profile().to_list()
        # assert 1 == len(results)
        # assert 'metrics' in results[0]
        # assert 'dur' in results[0]
        # #
        results = g.V().has('name', 'peter').as_('a').out('created').as_('b').select('a', 'b').by(
            __.value_map()).to_list()
        assert 1 == len(results)
        assert 'peter' == results[0]['a']['name'][0]
        assert 35 == results[0]['a']['age'][0]
        assert 'lop' == results[0]['b']['name'][0]
        assert 'java' == results[0]['b']['lang'][0]
        assert 2 == len(results[0]['a'])
        assert 2 == len(results[0]['b'])
        # #
        results = g.V(1).inject(g.V(2).next()).values('name').to_list()
        assert 2 == len(results)
        assert 'marko' in results
        assert 'vadas' in results
        # #
        # this test just validates that the underscored versions of steps conflicting with Gremlin work
        # properly and can be removed when the old steps are removed - TINKERPOP-2272
        results = g.V().filter_(__.values('age').sum_().and_(
            __.max_().is_(P.gt(0)), __.min_().is_(P.gt(0)))).range_(0, 1).id_().next()
        assert 1 == results
        # #
        # test dict keys
        # types for dict
        results = g.V().has('person', 'name', 'marko').element_map("name").group_count().next()
        assert {HashableDict.of({T.id: 1, T.label: 'person', 'name': 'marko'}): 1} == results
        results = g.V().has('person', 'name', 'marko').both('knows').group_count().by(__.values('name').fold()).next()
        assert {tuple(['vadas']): 1, tuple(['josh']): 1} == results
        # #
        # test materializeProperties in V - GraphSON will deserialize into None and GraphBinary to []
        results = g.with_("materializeProperties", "tokens").V().to_list()
        for v in results:
            assert v.properties is None or len(v.properties) == 0
        # #
        # test materializeProperties in E - GraphSON will deserialize into None and GraphBinary to []
        results = g.with_("materializeProperties", "tokens").E().to_list()
        for e in results:
            assert e.properties is None or len(e.properties) == 0
        # #
        # test materializeProperties in VP - GraphSON will deserialize into None and GraphBinary to []
        results = g.with_("materializeProperties", "tokens").V().properties().to_list()
        for vp in results:
            assert vp.properties is None or len(vp.properties) == 0

    def test_iteration(self, remote_connection):
        statics.load_statics(globals())
        g = traversal().with_(remote_connection)

        t = g.V().count()
        assert t.has_next()
        assert t.has_next()
        assert t.has_next()
        assert t.has_next()
        assert t.has_next()
        assert 6 == t.next()
        assert not (t.has_next())
        assert not (t.has_next())
        assert not (t.has_next())
        assert not (t.has_next())
        assert not (t.has_next())

        t = g.V().has('name', P.within('marko', 'peter')).values('name').order()
        assert "marko" == t.next()
        assert t.has_next()
        assert t.has_next()
        assert t.has_next()
        assert t.has_next()
        assert t.has_next()
        assert "peter" == t.next()
        assert not (t.has_next())
        assert not (t.has_next())
        assert not (t.has_next())
        assert not (t.has_next())
        assert not (t.has_next())

        try:
            t.next()
            assert False
        except StopIteration:
            assert True

    def test_strategies(self, remote_connection):
        statics.load_statics(globals())
        g = traversal().with_(remote_connection). \
            with_strategies(TraversalStrategy("SubgraphStrategy",
                                             {"vertices": __.has_label("person"),
                                              "edges": __.has_label("created")},
                                             "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy"))
        assert 4 == g.V().count().next()
        assert 0 == g.E().count().next()
        assert 1 == g.V().label().dedup().count().next()
        assert "person" == g.V().label().dedup().next()
        #
        g = traversal().with_(remote_connection). \
            with_strategies(SubgraphStrategy(vertices=__.has_label("person"), edges=__.has_label("created")))
        assert 4 == g.V().count().next()
        assert 0 == g.E().count().next()
        assert 1 == g.V().label().dedup().count().next()
        assert "person" == g.V().label().dedup().next()
        #
        g = traversal().with_(remote_connection). \
            with_strategies(SubgraphStrategy(edges=__.has_label("created")))
        assert 6 == g.V().count().next()
        assert 4 == g.E().count().next()
        assert 1 == g.E().label().dedup().count().next()
        assert "created" == g.E().label().dedup().next()
        #
        g = g.without_strategies(SubgraphStrategy). \
            with_computer(vertices=__.has("name", "marko"), edges=__.limit(0))
        assert 1 == g.V().count().next()
        assert 0 == g.E().count().next()
        assert "person" == g.V().label().next()
        assert "marko" == g.V().name.next()
        #
        g = traversal().with_(remote_connection).with_computer()
        assert 6 == g.V().count().next()
        assert 6 == g.E().count().next()
        #
        g = traversal().with_(remote_connection).with_strategies(SeedStrategy(12345))
        shuffledResult = g.V().values("name").order().by(Order.shuffle).to_list()
        assert shuffledResult == g.V().values("name").order().by(Order.shuffle).to_list()
        assert shuffledResult == g.V().values("name").order().by(Order.shuffle).to_list()
        assert shuffledResult == g.V().values("name").order().by(Order.shuffle).to_list()

    def test_clone(self, remote_connection):
        g = traversal().with_(remote_connection)
        t = g.V().both()
        assert 12 == len(t.to_list())
        assert 5 == t.clone().limit(5).count().next()
        assert 10 == t.clone().limit(10).count().next()

    def test_receive_error(self, invalid_alias_remote_connection):
        g = traversal().with_(invalid_alias_remote_connection)

        try:
            g.V().next()
            assert False
        except GremlinServerError as err:
            assert err.status_code == 400
            assert ('Could not alias [g] to [does_not_exist] as [does_not_exist] not in the Graph or TraversalSource '
                    'global bindings') in err.status_message

    def test_authenticated(self, remote_connection_authenticated):
        statics.load_statics(globals())
        g = traversal().with_(remote_connection_authenticated)

        assert long(6) == g.V().count().to_list()[0]

    def test_forwards_interceptor_serializers(self, remote_connection_with_interceptor):
        g = traversal().with_(remote_connection_with_interceptor)
        result = g.inject(1).next()
        assert 2 == result # interceptor changes request to g.inject(2)
