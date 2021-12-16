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

@StepClassMap @StepMax
Feature: Step - max()

  Scenario: g_V_age_max
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[35].i |

  Scenario: g_V_foo_max
    Given the modern graph
    And the traversal of
      """
      g.V().values("foo").max()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_name_max
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |

  Scenario: g_V_age_fold_maxXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().max(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[35].i |

  Scenario: g_V_aggregateXaX_byXageX_capXaX_maxXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("age").cap("a").max(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[35].i |

  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_capXaX_maxXlocalX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().aggregate("a").by("age").cap("a").max(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[35].i |

  Scenario: g_V_aggregateXaX_byXageX_capXaX_unfold_max
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("age").cap("a").unfold().max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[35].i |

  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_capXaX_unfold_max
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().aggregate("a").by("age").cap("a").unfold().max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[35].i |

  Scenario: g_V_aggregateXaX_byXfooX_capXaX_maxXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("foo").cap("a").max(Scope.local)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_capXaX_maxXlocalX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().aggregate("a").by("foo").cap("a").max(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  Scenario: g_V_aggregateXaX_byXfooX_capXaX_unfold_max
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("foo").cap("a").unfold().max()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_capXaX_unfold_max
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().aggregate("a").by("foo").cap("a").unfold().max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  Scenario: g_V_foo_fold_maxXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("foo").fold().max(Scope.local)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_name_fold_maxXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().max(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |

  Scenario: g_V_repeatXbothX_timesX5X_age_max
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.both()).times(5).values("age").max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[35].i |

  Scenario: g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_maxX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").group().by("name").by(__.bothE().values("weight").max())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[1.0].d","lop":"d[0.4].d"}] |
