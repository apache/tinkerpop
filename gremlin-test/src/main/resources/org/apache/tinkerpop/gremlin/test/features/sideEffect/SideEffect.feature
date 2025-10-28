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

@StepClassSideEffect @StepSideEffect
Feature: Step - sideEffect()

  Scenario: g_V_sideEffectXidentityX
    Given the modern graph
    And the traversal of
      """
      g.V().sideEffect(__.identity())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_sideEffectXidentity_valuesXnameXX
    Given the modern graph
    And the traversal of
      """
      g.V().sideEffect(__.identity().values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_sideEffectXpropertyXage_22X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("age",21)
      """
    And the traversal of
      """
      g.V().sideEffect(__.property("age",22))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().has(\"age\",21)"
    And the graph should return 1 for count of "g.V().has(\"age\",22)"

  Scenario: g_V_group_byXvaluesXnameX_sideEffectXconstantXzyxXX_substringX1XX_byXconstantX1X_sideEffectXconstantXxyzXXX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(__.values("name").sideEffect(__.constant("zyx")).substring(0,1)).by(__.constant(1).sideEffect(__.constant("xyz")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p":"d[1].i", "r":"d[1].i", "v":"d[1].i", "j":"d[1].i", "l":"d[1].i", "m":"d[1].i"}] |

  Scenario: g_withSideEffectXx_setX_V_both_both_sideEffectXlocalXaggregateXxX_byXnameXX_capXxX_unfold
    Given the modern graph
    And the traversal of
      """
      g.withSideEffect("x",{}).V().both().both().sideEffect(__.local(aggregate("x").by("name"))).cap("x").unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |