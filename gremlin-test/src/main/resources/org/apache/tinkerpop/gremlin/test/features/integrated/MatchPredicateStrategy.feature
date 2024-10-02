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
Feature: Step - MatchPredicateStrategy

  @WithMatchPredicateStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXMatchPredicateStrategyX_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(MatchPredicateStrategy).V().match(__.as("a").out("created").has("name", "lop").as("b"),
                  __.as("b").in("created").has("age", 29).as("c")).where(__.as("c").repeat(__.out()).times(2)).select("a", "b", "c")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[lop]","c":"v[marko]"}] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withoutStrategiesXMatchPredicateStrategyX_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(MatchPredicateStrategy).V().match(__.as("a").out("created").has("name", "lop").as("b"),
                  __.as("b").in("created").has("age", 29).as("c")).where(__.as("c").repeat(__.out()).times(2)).select("a", "b", "c")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[lop]","c":"v[marko]"}] |
