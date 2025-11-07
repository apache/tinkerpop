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

  Scenario: g_V_sampleX1X_byXageX_byXT_idX
    Given the modern graph
    And the traversal of
      """
      g.V().sample(1).by("age").by(T.id)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Sample step can only have one by modulator"

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

  @WithSeedStrategy
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

  @WithSeedStrategy
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

  @WithSeedStrategy
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

  Scenario: g_VX1X_valuesXageX_sampleXlocal_5X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).values("age").sample(Scope.local, 5)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  Scenario: g_V_repeatXsampleX2XX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(sample(2)).times(2)
      """
    When iterated to list
    Then the result should have a count of 2

  Scenario: g_V_sampleX2X_sampleX2X
    Given the modern graph
    And the traversal of
      """
      g.V().sample(2).sample(2)
      """
    When iterated to list
    Then the result should have a count of 2

  Scenario: g_V3_repeatXout_order_byXperformancesX_sampleX2X_aggregateXxXX_untilXloops_isX2XX_capXxX_unfold
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.out().order().by("performances").sample(2).aggregate('x')).until(__.loops().is(2)).cap('x').unfold()
      """
    When iterated to list
    Then the result should have a count of 4

  Scenario: g_V3_out_order_byXperformancesX_sampleX2X_aggregateXxX_out_order_byXperformancesX_sampleX2X_aggregateXxX_capXxX_unfold
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).out().order().by("performances").sample(2).aggregate('x').out().order().by("performances").sample(2).aggregate('x').cap('x').unfold()
      """
    When iterated to list
    Then the result should have a count of 4