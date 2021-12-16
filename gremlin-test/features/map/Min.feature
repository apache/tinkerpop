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

@StepClassMap @StepMin
Feature: Step - min()

  Scenario: g_V_age_min
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_V_foo_min
    Given the modern graph
    And the traversal of
      """
      g.V().values("foo").min()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_name_min
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |

  Scenario: g_V_age_fold_minXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().min(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_V_aggregateXaX_byXageX_capXaX_minXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("age").cap("a").min(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_capXaX_minXlocalX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().aggregate("a").by("age").cap("a").min(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_V_aggregateXaX_byXageX_capXaX_unfold_min
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("age").cap("a").unfold().min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXageX_capXaX_unfold_min
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().aggregate("a").by("age").cap("a").unfold().min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_V_aggregateXaX_byXfooX_capXaX_minXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("foo").cap("a").min(Scope.local)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_capXaX_minXlocalX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().aggregate("a").by("foo").cap("a").min(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  Scenario: g_V_aggregateXaX_byXfooX_capXaX_unfold_min
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("foo").cap("a").unfold().min()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXaX_byXfooX_capXaX_unfold_min
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().aggregate("a").by("foo").cap("a").unfold().min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  Scenario: g_V_foo_fold_minXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("foo").fold().min(Scope.local)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_name_fold_minXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().min(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |

  Scenario: g_V_repeatXbothX_timesX5X_age_min
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.both()).times(5).values("age").min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_minX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").group().by("name").by(__.bothE().values("weight").min())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[1.0].d","lop":"d[0.2].d"}] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_V_foo_injectX9999999999X_min
    Given the modern graph
    And using the parameter xx1 defined as "d[9999999999].l"
    And the traversal of
      """
      g.V().values("foo").inject(xx1).min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[9999999999].l |
