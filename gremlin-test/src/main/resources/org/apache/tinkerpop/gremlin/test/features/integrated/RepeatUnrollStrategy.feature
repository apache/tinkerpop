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
Feature: Step - RepeatUnrollStrategy

  @WithRepeatUnrollStrategy
  Scenario: g_withStrategiesXRepeatUnrollStrategyX_V_repeatXoutX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(RepeatUnrollStrategy).V().repeat(out()).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  Scenario: g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXoutX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V().repeat(out()).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  @WithRepeatUnrollStrategy
  Scenario: g_withStrategiesXRepeatUnrollStrategyX_V_repeatXinX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(RepeatUnrollStrategy).V().repeat(in()).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |

  Scenario: g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXinX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V().repeat(in()).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |

  @WithRepeatUnrollStrategy
  Scenario: g_withStrategiesXRepeatUnrollStrategyX_V_repeatXout_hasXnameXX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(RepeatUnrollStrategy).V().repeat(out().has("name", notStartingWith("z"))).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  Scenario: g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXout_hasXnameXX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V().repeat(out().has("name", notStartingWith("z"))).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  @WithRepeatUnrollStrategy
  Scenario: g_withStrategiesXRepeatUnrollStrategyX_V_repeatXin_hasXnameXX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(RepeatUnrollStrategy).V().repeat(in().has("age", gt(20))).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |

  Scenario: g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXin_hasXnameXX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V().repeat(in().has("age", gt(20))).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |

  @WithRepeatUnrollStrategy
  Scenario: g_withStrategiesXRepeatUnrollStrategyX_V_repeatXboth_hasXage_ltX30XXX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(RepeatUnrollStrategy).V().repeat(both().has("age", lt(30))).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[vadas] |
      | v[vadas] |

  Scenario: g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXboth_hasXage_ltX30XXX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V().repeat(both().has("age", lt(30))).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[vadas] |
      | v[vadas] |

  @WithRepeatUnrollStrategy
  Scenario: g_withStrategiesXRepeatUnrollStrategyX_V_repeatXbothE_otherV_hasXage_gtX30XXX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(RepeatUnrollStrategy).V().repeat(bothE().otherV().has("age", lt(30))).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[peter] |

  Scenario: g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXbothE_otherV_hasXage_gtX30XXX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V().repeat(bothE().otherV().has("age", lt(30))).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[peter] |
