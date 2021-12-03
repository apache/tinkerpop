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

@StepClassFilter @StepSample
Feature: Step - sample()

  Scenario: g_E_sampleX1X
    Given the modern graph
    And the traversal of
      """
      g.E().sample(1)
      """
    When iterated to list
    Then the result should have a count of 1

  Scenario: g_E_sampleX2X_byXweightX
    Given the modern graph
    And the traversal of
      """
      g.E().sample(2).by("weight")
      """
    When iterated to list
    Then the result should have a count of 2

  Scenario: g_V_localXoutE_sampleX1X_byXweightXX
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.outE().sample(1).by("weight"))
      """
    When iterated to list
    Then the result should have a count of 3

  Scenario: g_withStrategiesXSeedStrategyX_V_group_byXlabelX_byXbothE_weight_order_sampleX2X_foldXunfold
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SeedStrategy(seed: 999999)).V().group().by(T.label).by(__.bothE().values("weight").order().sample(2).fold()).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"software":"l[d[1.0].d,d[0.4].d]"}] |
      | m[{"person":"l[d[0.5].d,d[1.0].d]"}] |

  Scenario: g_withStrategiesXSeedStrategyX_V_group_byXlabelX_byXbothE_weight_order_fold_sampleXlocal_5XXunfold
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SeedStrategy(seed: 999999)).V().group().by(T.label).by(__.bothE().values("weight").order().fold().sample(Scope.local, 5)).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"software":"l[d[0.2].d,d[0.4].d,d[0.4].d,d[1.0].d]"}] |
      | m[{"person":"l[d[0.5].d,d[1.0].d,d[0.4].d,d[0.2].d,d[1.0].d]"}] |

  Scenario: g_withStrategiesXSeedStrategyX_V_order_byXlabel_descX_sampleX1X_byXageX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SeedStrategy(seed: 999999)).V().order().by(T.label, Order.desc).sample(1).by("age")
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[peter] |
      | v[marko] |
      | v[josh] |
      | v[vadas] |
    Then the result should have a count of 1