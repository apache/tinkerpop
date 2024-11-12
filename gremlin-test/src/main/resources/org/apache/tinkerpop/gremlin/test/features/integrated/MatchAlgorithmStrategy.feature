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

@StepClassIntegrated
Feature: Step - MatchAlgorithmStrategy

  @WithMatchAlgorithmStrategy
  Scenario: g_withStrategiesXMatchAlgorithmStrategyXmatchAlgorithm_CountMatchAlgorithmXX_V_matchXa_knows_b__a_created_cX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(MatchAlgorithmStrategy(matchAlgorithm: "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep$CountMatchAlgorithm"))
                  .V().match(__.as("a").out("knows").as("b"),
                  __.as("a").out("created").as("c"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[vadas]","c":"v[lop]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[lop]"}] |

  @WithMatchAlgorithmStrategy
  Scenario: g_withStrategiesXMatchAlgorithmStrategyXmatchAlgorithm_GreedyMatchAlgorithmXX_V_matchXa_knows_b__a_created_cX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(MatchAlgorithmStrategy(matchAlgorithm: "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep$GreedyMatchAlgorithm"))
                  .V().match(__.as("a").out("knows").as("b"),
                  __.as("a").out("created").as("c"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[vadas]","c":"v[lop]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[lop]"}] |

  Scenario: g_withoutStrategiesXMatchAlgorithmStrategyX_V_matchXa_knows_b__a_created_cX
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(MatchAlgorithmStrategy)
                .V().match(__.as("a").out("knows").as("b"),
                  __.as("a").out("created").as("c"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[vadas]","c":"v[lop]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[lop]"}] |
