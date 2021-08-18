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

@StepClassMap @StepSum
Feature: Step - sum()

  Scenario: g_V_age_sum
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[123].l |

  Scenario: g_V_foo_sum
    Given the modern graph
    And the traversal of
      """
      g.V().values("foo").sum()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_age_fold_sumXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().sum(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[123].i |

  Scenario: g_V_foo_fold_sumXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("foo").fold().sum(Scope.local)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_sumX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").group().by("name").by(__.bothE().values("weight").sum())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[1.0].d","lop":"d[1.0].d"}] |

  # null values are ignored in sum() which is similar to sum aggregation works in SQL
  Scenario: g_V_aggregateXaX_byXageX_sumXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("age").cap("a").sum(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[123].i |

  # null values are ignored in sum() which is similar to sum aggregation works in SQL
  Scenario: g_V_aggregateXaX_byXageX_capXaX_unfold_sum
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("age").cap("a").unfold().sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[123].l |

  # if all values are null then the result is null
  Scenario: g_V_aggregateXaX_byXfooX_sumXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("foo").cap("a").sum(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  # if all values are null then the result is null
  Scenario: g_V_aggregateXaX_byXfooX_capXaX_unfold_sum
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("a").by("foo").cap("a").unfold().sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  Scenario: g_injectXnull_10_5_nullX_sum
    Given the modern graph
    And using the parameter xx1 defined as "d[10].i"
    And using the parameter xx2 defined as "d[5].i"
    And the traversal of
      """
      g.inject(null, xx1, xx2, null).sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[15].l |

  Scenario: g_injectXlistXnull_10_5_nullXX_sumXlocalX
    Given the modern graph
    And using the parameter xx1 defined as "l[null,d[10].i,d[5].i,null]"
    And the traversal of
      """
      g.inject(xx1).sum(local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[15].i |