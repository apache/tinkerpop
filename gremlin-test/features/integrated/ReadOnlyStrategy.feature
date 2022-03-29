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
Feature: Step - ReadOnlyStrategy

  @WithReadOnlyStrategy
  Scenario: g_withStrategiesXReadOnlyStrategyX_V
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ReadOnlyStrategy).V()
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

  @WithReadOnlyStrategy
  Scenario: g_withStrategiesXReadOnlyStrategyX_V_outXknowsX_name
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ReadOnlyStrategy).V().out("knows").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |

  @WithReadOnlyStrategy
  Scenario: g_withStrategiesXReadOnlyStrategyX_addVXpersonX
    Given the empty graph
    And the traversal of
      """
      g.withStrategies(ReadOnlyStrategy).addV("person")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The provided traversal has a mutating step and thus is not read only"

  @WithReadOnlyStrategy
  Scenario: g_withStrategiesXReadOnlyStrategyX_addVXpersonX_fromXVX1XX_toXVX2XX
    Given the empty graph
    And the traversal of
      """
      g.withStrategies(ReadOnlyStrategy).addE("link").from(__.V(1)).to(__.V(2))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The provided traversal has a mutating step and thus is not read only"

  @WithReadOnlyStrategy
  Scenario: g_withStrategiesXReadOnlyStrategyX_V_addVXpersonX_fromXVX1XX
    Given the empty graph
    And the traversal of
      """
      g.withStrategies(ReadOnlyStrategy).V().addE("link").from(__.V(1))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The provided traversal has a mutating step and thus is not read only"

  @WithReadOnlyStrategy
  Scenario: g_withStrategiesXReadOnlyStrategyX_V_propertyXname_joshX
    Given the empty graph
    And the traversal of
      """
      g.withStrategies(ReadOnlyStrategy).V().property("name","josh")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The provided traversal has a mutating step and thus is not read only"

  @WithReadOnlyStrategy
  Scenario: g_withStrategiesXReadOnlyStrategyX_E_propertyXweight_0X
    Given the empty graph
    And the traversal of
      """
      g.withStrategies(ReadOnlyStrategy).E().property("weight", 0)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The provided traversal has a mutating step and thus is not read only"

