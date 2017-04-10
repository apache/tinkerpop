'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
'''

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

from gremlin_python.structure.graph import Graph
from gremlin_python.process.strategies import *
from gremlin_python.process.graph_traversal import __


class TestTraversalStrategies(object):
    def test_singletons(self):
        g = Graph().traversal()
        bytecode = g.withStrategies(ReadOnlyStrategy()).bytecode
        assert 1 == len(bytecode.source_instructions)
        assert 2 == len(bytecode.source_instructions[0])
        assert "withStrategies" == bytecode.source_instructions[0][0]
        assert ReadOnlyStrategy() == bytecode.source_instructions[0][1]
        assert "ReadOnlyStrategy" == str(bytecode.source_instructions[0][1])
        assert hash(ReadOnlyStrategy()) == hash(bytecode.source_instructions[0][1])
        assert 0 == len(g.traversal_strategies.traversal_strategies)  # these strategies are proxies
        ##
        g = g.withStrategies(ReadOnlyStrategy(), IncidentToAdjacentStrategy())
        bytecode = g.bytecode
        assert 1 == len(bytecode.source_instructions)
        assert 3 == len(bytecode.source_instructions[0])
        assert "withStrategies" == bytecode.source_instructions[0][0]
        assert ReadOnlyStrategy() == bytecode.source_instructions[0][1]
        assert IncidentToAdjacentStrategy() == bytecode.source_instructions[0][2]
        ##
        bytecode = g.V().bytecode
        assert 1 == len(bytecode.source_instructions)
        assert 3 == len(bytecode.source_instructions[0])
        assert "withStrategies" == bytecode.source_instructions[0][0]
        assert ReadOnlyStrategy() == bytecode.source_instructions[0][1]
        assert IncidentToAdjacentStrategy() == bytecode.source_instructions[0][2]
        assert 1 == len(bytecode.step_instructions)
        assert "V" == bytecode.step_instructions[0][0]
        ##
        bytecode = g.withoutStrategies(ReadOnlyStrategy()).V().bytecode
        assert 2 == len(bytecode.source_instructions)
        assert 3 == len(bytecode.source_instructions[0])
        assert 2 == len(bytecode.source_instructions[1])
        assert "withStrategies" == bytecode.source_instructions[0][0]
        assert ReadOnlyStrategy() == bytecode.source_instructions[0][1]
        assert IncidentToAdjacentStrategy() == bytecode.source_instructions[0][2]
        assert "withoutStrategies" == bytecode.source_instructions[1][0]
        assert ReadOnlyStrategy() == bytecode.source_instructions[1][1]
        assert 1 == len(bytecode.step_instructions)
        assert "V" == bytecode.step_instructions[0][0]
        ##
        bytecode = g.withoutStrategies(ReadOnlyStrategy(), LazyBarrierStrategy()).V().bytecode
        assert 2 == len(bytecode.source_instructions)
        assert 3 == len(bytecode.source_instructions[0])
        assert 3 == len(bytecode.source_instructions[1])
        assert "withStrategies" == bytecode.source_instructions[0][0]
        assert ReadOnlyStrategy() == bytecode.source_instructions[0][1]
        assert IncidentToAdjacentStrategy() == bytecode.source_instructions[0][2]
        assert "withoutStrategies" == bytecode.source_instructions[1][0]
        assert ReadOnlyStrategy() == bytecode.source_instructions[1][1]
        assert LazyBarrierStrategy() == bytecode.source_instructions[1][2]
        assert 1 == len(bytecode.step_instructions)
        assert "V" == bytecode.step_instructions[0][0]

    def test_configurable(self):
        g = Graph().traversal()
        bytecode = g.withStrategies(MatchAlgorithmStrategy("greedy")).bytecode
        assert 1 == len(bytecode.source_instructions)
        assert 2 == len(bytecode.source_instructions[0])
        assert "withStrategies" == bytecode.source_instructions[0][0]
        assert MatchAlgorithmStrategy() == bytecode.source_instructions[0][1]
        assert "MatchAlgorithmStrategy" == str(bytecode.source_instructions[0][1])
        assert hash(MatchAlgorithmStrategy()) == hash(
            bytecode.source_instructions[0][1])  # even though different confs, same strategy
        assert 0 == len(g.traversal_strategies.traversal_strategies)  # these strategies are proxies
        ###
        bytecode = g.withStrategies(SubgraphStrategy(vertices=__.has("name","marko"))).bytecode
        assert 1 == len(bytecode.source_instructions)
        assert 2 == len(bytecode.source_instructions[0])
        assert "withStrategies" == bytecode.source_instructions[0][0]
        assert SubgraphStrategy() == bytecode.source_instructions[0][1]
        strategy = bytecode.source_instructions[0][1]
        assert 1 == len(strategy.configuration)
        assert __.has("name","marko") == strategy.configuration["vertices"]
