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
import pytest

from tornado import ioloop, gen

from gremlin_python import statics
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.statics import long
from gremlin_python.driver.driver_remote_connection import (
    DriverRemoteConnection)
from gremlin_python.process.traversal import Traverser
from gremlin_python.process.traversal import TraversalStrategy
from gremlin_python.process.traversal import Bindings
from gremlin_python.process.traversal import P
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.structure.graph import Vertex
from gremlin_python.process.strategies import SubgraphStrategy, ReservedKeysVerificationStrategy

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


class TestDriverRemoteConnection(object):
    def test_traversals(self, remote_connection):
        statics.load_statics(globals())
        g = traversal().withRemote(remote_connection)

        assert long(6) == g.V().count().toList()[0]
        # #
        assert Vertex(1) == g.V(1).next()
        assert 1 == g.V(1).id().next()
        assert Traverser(Vertex(1)) == g.V(1).nextTraverser()
        assert 1 == len(g.V(1).toList())
        assert isinstance(g.V(1).toList(), list)
        results = g.V().repeat(out()).times(2).name
        results = results.toList()
        assert 2 == len(results)
        assert "lop" in results
        assert "ripple" in results
        # #
        assert 10 == g.V().repeat(both()).times(5)[0:10].count().next()
        assert 1 == g.V().repeat(both()).times(5)[0:1].count().next()
        assert 0 == g.V().repeat(both()).times(5)[0:0].count().next()
        assert 4 == g.V()[2:].count().next()
        assert 2 == g.V()[:2].count().next()
        # #
        results = g.withSideEffect('a', ['josh', 'peter']).V(1).out('created').in_('created').values('name').where(P.within('a')).toList()
        assert 2 == len(results)
        assert 'josh' in results
        assert 'peter' in results
        # #
        results = g.V().out().profile().toList()
        assert 1 == len(results)
        assert 'metrics' in results[0]
        assert 'dur' in results[0]
        # #
        results = g.V().has('name', 'peter').as_('a').out('created').as_('b').select('a', 'b').by(
            __.valueMap()).toList()
        assert 1 == len(results)
        assert 'peter' == results[0]['a']['name'][0]
        assert 35 == results[0]['a']['age'][0]
        assert 'lop' == results[0]['b']['name'][0]
        assert 'java' == results[0]['b']['lang'][0]
        assert 2 == len(results[0]['a'])
        assert 2 == len(results[0]['b'])
        # #
        results = g.V(1).inject(g.V(2).next()).values('name').toList()
        assert 2 == len(results)
        assert 'marko' in results
        assert 'vadas' in results
        # #
        results = g.V().has('person', 'name', 'marko').map(lambda: ("it.get().value('name')", "gremlin-groovy")).toList()
        assert 1 == len(results)
        assert 'marko' in results
        # #
        # this test just validates that the underscored versions of steps conflicting with Gremlin work
        # properly and can be removed when the old steps are removed - TINKERPOP-2272
        results = g.V().filter_(__.values('age').sum_().and_(
            __.max_().is_(gt(0)), __.min_().is_(gt(0)))).range_(0, 1).id_().next()
        assert 1 == results
        # #
        # test binding in P
        results = g.V().has('person', 'age', Bindings.of('x', lt(30))).count().next()
        assert 2 == results

    def test_lambda_traversals(self, remote_connection):
        statics.load_statics(globals())
        assert "remoteconnection[ws://localhost:45940/gremlin,gmodern]" == str(remote_connection)
        g = traversal().withRemote(remote_connection)

        assert 24.0 == g.withSack(1.0, lambda: ("x -> x + 1", "gremlin-groovy")).V().both().sack().sum().next()
        assert 24.0 == g.withSack(lambda: ("{1.0d}", "gremlin-groovy"), lambda: ("x -> x + 1", "gremlin-groovy")).V().both().sack().sum().next()

        assert 48.0 == g.withSack(1.0, lambda: ("x, y ->  x + y + 1", "gremlin-groovy")).V().both().sack().sum().next()
        assert 48.0 == g.withSack(lambda: ("{1.0d}", "gremlin-groovy"), lambda: ("x, y ->  x + y + 1", "gremlin-groovy")).V().both().sack().sum().next()

    def test_iteration(self, remote_connection):
        statics.load_statics(globals())
        g = traversal().withRemote(remote_connection)

        t = g.V().count()
        assert t.hasNext()
        assert t.hasNext()
        assert t.hasNext()
        assert t.hasNext()
        assert t.hasNext()
        assert 6 == t.next()
        assert not(t.hasNext())
        assert not(t.hasNext())
        assert not(t.hasNext())
        assert not(t.hasNext())
        assert not(t.hasNext())

        t = g.V().has('name', P.within('marko', 'peter')).values('name').order()
        assert "marko" == t.next()
        assert t.hasNext()
        assert t.hasNext()
        assert t.hasNext()
        assert t.hasNext()
        assert t.hasNext()
        assert "peter" == t.next()
        assert not(t.hasNext())
        assert not(t.hasNext())
        assert not(t.hasNext())
        assert not(t.hasNext())
        assert not(t.hasNext())

        try:
            t.next()
            assert False
        except StopIteration:
            assert True

    def test_strategies(self, remote_connection):
        statics.load_statics(globals())
        g = traversal().withRemote(remote_connection). \
            withStrategies(TraversalStrategy("SubgraphStrategy",
                                             {"vertices": __.hasLabel("person"),
                                              "edges": __.hasLabel("created")},
                                              "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy"))
        assert 4 == g.V().count().next()
        assert 0 == g.E().count().next()
        assert 1 == g.V().label().dedup().count().next()
        assert 4 == g.V().filter(lambda: ("lambda x: True", "gremlin-python")).count().next()
        assert "person" == g.V().label().dedup().next()
        #
        g = traversal().withRemote(remote_connection). \
            withStrategies(SubgraphStrategy(vertices=__.hasLabel("person"), edges=__.hasLabel("created")))
        assert 4 == g.V().count().next()
        assert 0 == g.E().count().next()
        assert 1 == g.V().label().dedup().count().next()
        assert "person" == g.V().label().dedup().next()
        #
        g = traversal().withRemote(remote_connection). \
            withStrategies(SubgraphStrategy(edges=__.hasLabel("created")))
        assert 6 == g.V().count().next()
        assert 4 == g.E().count().next()
        assert 1 == g.E().label().dedup().count().next()
        assert "created" == g.E().label().dedup().next()
        #
        g = g.withoutStrategies(SubgraphStrategy). \
            withComputer(vertices=__.has("name", "marko"), edges=__.limit(0))
        assert 1 == g.V().count().next()
        assert 0 == g.E().count().next()
        assert "person" == g.V().label().next()
        assert "marko" == g.V().name.next()
        #
        g = traversal().withRemote(remote_connection).withComputer()
        assert 6 == g.V().count().next()
        assert 6 == g.E().count().next()
        #
        g = traversal().withRemote(remote_connection). \
            withStrategies(ReservedKeysVerificationStrategy(throw_exception=True))
        try:
            g.addV("person").property("id", "please-don't-use-id").iterate()
            assert False
        except GremlinServerError as gse:
            assert gse.status_code == 500
        #
        g = traversal().withRemote(remote_connection).with_("x", True).with_('evaluationTimeout', 10)
        try:
            g.inject(1).sideEffect(lambda: ("Thread.sleep(5000)", "gremlin-groovy")).iterate()
            assert False
        except GremlinServerError as gse:
            assert gse.status_code == 598

    def test_side_effects(self, remote_connection):
        statics.load_statics(globals())
        #
        g = traversal().withRemote(remote_connection)
        ###
        t = g.V().hasLabel("project").name.iterate()
        assert 0 == len(t.side_effects.keys())
        with pytest.raises(Exception):
            m = t.side_effects["m"]
        ###
        t = g.V().out("created").groupCount("m").by("name")
        results = t.toSet()
        assert 2 == len(results)
        assert Vertex(3) in results
        assert Vertex(5) in results
        assert 1 == len(t.side_effects.keys())
        assert "m" in t.side_effects.keys()
        m = t.side_effects["m"]
        assert isinstance(m, dict)
        assert 2 == len(m)
        assert 3 == m["lop"]
        assert 1 == m["ripple"]

        # check status attributes
        assert "host" in t.side_effects.status_attributes

        ##
        t = g.V().out("created").groupCount("m").by("name").name.aggregate("n")
        results = t.toSet()
        assert 2 == len(results)
        assert "lop" in results
        assert "ripple" in results
        assert 2 == len(t.side_effects.keys())
        assert "m" in t.side_effects.keys()
        assert "n" in t.side_effects.keys()
        n = t.side_effects.get("n")
        assert isinstance(n, dict)
        assert 2 == len(n)
        assert "lop" in n.keys()
        assert "ripple" in n.keys()
        assert 3 == n["lop"]
        assert 1 == n["ripple"]

        t = g.withSideEffect('m', 32).V().map(lambda: "x: x.sideEffects('m')")
        results = t.toSet()
        assert 1 == len(results)
        assert 32 == list(results)[0]
        assert 32 == t.side_effects['m']
        assert 1 == len(t.side_effects.keys())
        with pytest.raises(Exception):
            x = t.side_effects["x"]

        a = g.V().has("name", "marko").next()
        b = g.V().has("name", "peter").next()
        edge = g.withSideEffect("b", b).V(a).addE("knows").to("b").next()
        assert "knows" == edge.label
        assert a == edge.outV
        assert b == edge.inV
        g.V().has("name", "marko").outE("knows").where(__.inV().has("name", "peter")).drop().iterate()
        ##
        edge = g.withSideEffect("a", a).withSideEffect("b", b).V().limit(1).addE("knows").from_("a").to("b").next()
        assert "knows" == edge.label
        assert a == edge.outV
        assert b == edge.inV
        g.V().has("name", "marko").outE("knows").where(__.inV().has("name", "peter")).drop().iterate()

    def test_side_effect_close(self, remote_connection):
        g = traversal().withRemote(remote_connection)
        t = g.V().aggregate('a').aggregate('b')
        t.toList()

        # The 'a' key should return some side effects
        results = t.side_effects.get('a')
        assert results

        # Close result is None
        results = t.side_effects.close()
        assert not results

        # Shouldn't get any new info from server
        # 'b' isn't in local cache
        results = t.side_effects.get('b')
        assert not results

        # But 'a' should still be cached locally
        results = t.side_effects.get('a')
        assert results

        # 'a' should have been added to local keys cache, but not 'b'
        results = t.side_effects.keys()
        assert len(results) == 1
        a, = results
        assert a == 'a'

        # Try to get 'b' directly from server, should throw error
        with pytest.raises(Exception):
            t.side_effects.value_lambda('b')

    def test_promise(self, remote_connection):
        g = traversal().withRemote(remote_connection)
        future = g.V().aggregate('a').promise()
        t = future.result()
        assert len(t.toList()) == 6
        a, = t.side_effects.keys()
        assert a == 'a'
        results = t.side_effects.get('a')
        assert results
        results = t.side_effects.close()
        assert not results

    def test_clone(self, remote_connection):
        g = traversal().withRemote(remote_connection)
        t = g.V().count()
        assert 6 == t.next()
        assert 6 == t.clone().next()
        assert 6 == t.clone().next()

