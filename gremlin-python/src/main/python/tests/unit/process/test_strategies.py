#
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

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.strategies import *
from gremlin_python.process.graph_traversal import __


class TestTraversalStrategies(object):
    def test_singletons(self):
        g = traversal().with_(None)
        gremlin_script = g.with_strategies(ReadOnlyStrategy()).gremlin_lang
        gremlin_instr = gremlin_script.gremlin
        assert "withStrategies" in str(gremlin_script)
        assert "ReadOnlyStrategy" in str(gremlin_script)
        assert "withStrategies(ReadOnlyStrategy)" == gremlin_instr[1]
        assert 0 == len(g.traversal_strategies.traversal_strategies)  # these strategies are proxies
        ##
        g = g.with_strategies(ReadOnlyStrategy(), IncidentToAdjacentStrategy())
        gremlin_script = g.gremlin_lang
        gremlin_instr = gremlin_script.gremlin
        assert "withStrategies" in str(gremlin_script)
        assert "ReadOnlyStrategy" in str(gremlin_script)
        assert "withStrategies(ReadOnlyStrategy,IncidentToAdjacentStrategy)" == gremlin_instr[1]
        ##
        gremlin_script = g.V().gremlin_lang
        gremlin_instr = gremlin_script.gremlin
        assert "withStrategies" in str(gremlin_script)
        assert "ReadOnlyStrategy" in str(gremlin_script)
        assert "IncidentToAdjacentStrategy" in str(gremlin_script)
        assert "V()" in str(gremlin_script)
        assert "withStrategies(ReadOnlyStrategy,IncidentToAdjacentStrategy)" == gremlin_instr[1]
        assert 'V' == gremlin_instr[3]
        ##
        gremlin_script = g.without_strategies(ReadOnlyStrategy()).V().gremlin_lang
        gremlin_instr = gremlin_script.gremlin
        assert "withStrategies" in str(gremlin_script)
        assert "ReadOnlyStrategy" in str(gremlin_script)
        assert "IncidentToAdjacentStrategy" in str(gremlin_script)
        assert "withoutStrategies" in str(gremlin_script)
        assert "V()" in str(gremlin_script)
        assert "withStrategies(ReadOnlyStrategy,IncidentToAdjacentStrategy)" == gremlin_instr[1]
        assert "withoutStrategies(ReadOnlyStrategy)" in str(gremlin_script)
        ##
        gremlin_script = g.without_strategies(ReadOnlyStrategy(), LazyBarrierStrategy()).V().gremlin_lang
        assert "withStrategies" in str(gremlin_script)
        assert "ReadOnlyStrategy" in str(gremlin_script)
        assert "IncidentToAdjacentStrategy" in str(gremlin_script)
        assert "withoutStrategies" in str(gremlin_script)
        assert "V()" in str(gremlin_script)
        assert "withoutStrategies(ReadOnlyStrategy,LazyBarrierStrategy)" in str(gremlin_script)
        ###
        g = traversal().with_(None)
        gremlin_script = g.with_("x", "test").with_("y").gremlin_lang
        options = gremlin_script.options_strategies
        assert "test" == options[0].configuration["x"]
        assert options[1].configuration["y"]

    def test_configurable(self):
        g = traversal().with_(None)
        gremlin_script = g.with_strategies(MatchAlgorithmStrategy("greedy")).gremlin_lang
        assert "withStrategies" in str(gremlin_script)
        assert "MatchAlgorithmStrategy" in str(gremlin_script)
        assert "greedy" in str(gremlin_script)
        ###
        gremlin_script = g.with_strategies(SubgraphStrategy(vertices=__.has("name", "marko"))).gremlin_lang
        assert "withStrategies" in str(gremlin_script)
        assert "SubgraphStrategy" in str(gremlin_script)
        assert "__.has('name','marko')" in str(gremlin_script)
        ###
        gremlin_script = g.with_strategies(OptionsStrategy(x="test", y=True)).gremlin_lang
        options = gremlin_script.options_strategies
        assert "test" == options[0].configuration["x"]
        assert options[0].configuration["y"]

    def test_custom_strategies(self):
        g = traversal().with_(None)

        gremlin_script = g.with_strategies(TraversalStrategy(strategy_name='CustomSingletonStrategy')).gremlin_lang
        assert "withStrategies(CustomSingletonStrategy)" in str(gremlin_script)

        gremlin_script = g.with_strategies(TraversalStrategy(
            strategy_name='CustomConfigurableStrategy',
            stringKey='string value',
            intKey=5,
            booleanKey=True
        )).gremlin_lang
        assert "withStrategies(new CustomConfigurableStrategy(stringKey:'string value',intKey:5,booleanKey:true))" in str(gremlin_script)

    def test_configured_strategy_full_strings(self):
        # Assert the exact 'g.withStrategies(new <Name>(key:value,...)).V()' string for
        # configured strategies (option order = configuration-dict insertion order).
        g = traversal().with_(None)

        tests = list()
        # EdgeLabelVerificationStrategy: always emits logWarning + throwException (both stored
        # unconditionally in __init__), so even a configured instance lists both keys.
        tests.append([g.with_strategies(
            EdgeLabelVerificationStrategy(log_warning=True, throw_exception=True)).V(),
            "g.withStrategies(new EdgeLabelVerificationStrategy(logWarning:true,throwException:true)).V()"])
        # Default EdgeLabelVerificationStrategy() still renders as configured (both flags False).
        tests.append([g.with_strategies(EdgeLabelVerificationStrategy()).V(),
            "g.withStrategies(new EdgeLabelVerificationStrategy(logWarning:false,throwException:false)).V()"])
        # ReservedKeysVerificationStrategy defaults keys=['id','label'] -> rendered as a list literal.
        tests.append([g.with_strategies(ReservedKeysVerificationStrategy()).V(),
            "g.withStrategies(new ReservedKeysVerificationStrategy(logWarning:false,throwException:false,keys:['id','label'])).V()"])
        # SeedStrategy: single int option, emitted bare (no type suffix).
        tests.append([g.with_strategies(SeedStrategy(seed=999999)).V(),
            "g.withStrategies(new SeedStrategy(seed:999999)).V()"])
        # HaltedTraverserStrategy: string option -> repr()'d with single quotes.
        tests.append([g.with_strategies(HaltedTraverserStrategy(halted_traverser_factory='detached')).V(),
            "g.withStrategies(new HaltedTraverserStrategy(haltedTraverserFactory:'detached')).V()"])
        # SubgraphStrategy: only the provided (non-None) checkAdjacentVertices option is emitted.
        tests.append([g.with_strategies(SubgraphStrategy(check_adjacent_vertices=True)).V(),
            "g.withStrategies(new SubgraphStrategy(checkAdjacentVertices:true)).V()"])
        # PartitionStrategy: options emitted in insertion order (partitionKey, writePartition,
        # readPartitions, includeMetaProperties); readPartitions is a list literal.
        tests.append([g.with_strategies(PartitionStrategy(
            partition_key='p', write_partition='a', read_partitions=['a', 'b'], include_meta_properties=True)).V(),
            "g.withStrategies(new PartitionStrategy(partitionKey:'p',writePartition:'a',readPartitions:['a','b'],includeMetaProperties:true)).V()"])
        # VertexProgramStrategy: exercise its config path with a single non-None option (workers).
        tests.append([g.with_strategies(VertexProgramStrategy(workers=4)).V(),
            "g.withStrategies(new VertexProgramStrategy(workers:4)).V()"])

        for t in range(len(tests)):
            assert tests[t][0].gremlin_lang.get_gremlin() == tests[t][1]
