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

@StepClassFilter @StepCoin
Feature: Step - coin()

  Scenario: g_V_coinX1X
    Given the modern graph
    And the traversal of
      """
      g.V().coin(1.0)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter]  |

  Scenario: g_V_coinX0X
    Given the modern graph
    And the traversal of
      """
      g.V().coin(0.0)
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationStrategyNotSupported @WithSeedStrategy
  Scenario: g_withStrategiesXSeedStrategyX_V_order_byXnameX_coinX50X
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SeedStrategy(seed: 999999)).V().order().by("name").coin(0.5)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |