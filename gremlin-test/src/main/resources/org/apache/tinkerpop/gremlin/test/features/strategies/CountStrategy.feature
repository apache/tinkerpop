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

@StepClassIntegrated
Feature: Step - CountStrategy

  @WithCountStrategy
  Scenario: g_withStrategiesXCountStrategyX_V_whereXoutE_count_isX0XX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(CountStrategy).V().where(outE().count().is(0))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |
      | v[ripple]  |

  Scenario: g_withoutStrategiesXCountStrategyX_V_whereXoutE_count_isX0XX
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(CountStrategy).V().where(outE().count().is(0))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |
      | v[ripple]  |

  # a following is() must see the real count, not the value capped by the limit() CountStrategy injects
  Scenario: g_withStrategiesXCountStrategyX_V_whereXoutE_count_isXgtX1XX_isXltX3XXX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(CountStrategy).V().where(outE().count().is(gt(1)).is(lt(3)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  # the first is() only passes counts below the limit, so the limit is safe and the count seen downstream is exact
  Scenario: g_withStrategiesXCountStrategyX_V_whereXoutE_count_isXltX3XX_isXgtX1XXX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(CountStrategy).V().where(outE().count().is(lt(3)).is(gt(1)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  Scenario: g_withStrategiesXCountStrategyX_V_outE_count_isXgtX2XX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(CountStrategy).V().outE().count().is(gt(2))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_withStrategiesXCountStrategyX_V_mapXoutE_count_isXgtX1XXX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(CountStrategy).V().map(outE().count().is(gt(1)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l |
      | d[2].l |

  # the lt() bound is a traversal, so the strategy can't know it and must not cap the count
  Scenario: g_withStrategiesXCountStrategyX_V_filterXoutE_count_isXgtX1X_andXltXconstantX3XXXXX_name
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(CountStrategy).V().filter(outE().count().is(P.gt(1).and(P.lt(__.constant(3l))))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |

  # the predicate traversal reads the label on count(), so it must see the real count
  Scenario: g_withStrategiesXCountStrategyX_V_outE_count_asXaX_isXgtX1X_andXlteXselectXaXXXX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(CountStrategy).V().outE().count().as("a").is(P.gt(1).and(P.lte(__.select("a"))))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  # without() accepts every count above its largest value, so a count of 7 must not be capped at 6
  Scenario: g_withStrategiesXCountStrategyX_V_unionXidentity_hasXname_markoXX_count_isXwithoutX2_6_4XX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(CountStrategy).V().union(identity(), has("name", "marko")).count().is(P.without(2, 6, 4))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[7].l |

  # not(lte(2)) is gt(2), so the count must not be capped at 2
  Scenario: g_withStrategiesXCountStrategyX_V_count_isXnotXlteX2XXX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(CountStrategy).V().count().is(P.not(P.lte(2)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |
