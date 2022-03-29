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

@StepClassMap @StepProject
Feature: Step - project()

  Scenario: g_V_hasLabelXpersonX_projectXa_bX_byXoutE_countX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        project("a", "b").
          by(__.outE().count()).
          by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"d[3].l", "b":"d[29].i"}] |
      | m[{"a":"d[0].l", "b":"d[27].i"}] |
      | m[{"a":"d[2].l", "b":"d[32].i"}] |
      | m[{"a":"d[1].l", "b":"d[35].i"}] |

  Scenario: g_V_outXcreatedX_projectXa_bX_byXnameX_byXinXcreatedX_countX_order_byXselectXbX__descX_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").
        project("a", "b").
          by("name").
          by(__.in("created").count()).
        order().
          by(__.select("b"), Order.desc).
        select("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | lop |
      | lop |
      | ripple |

  Scenario: g_V_valueMap_projectXxX_byXselectXnameXX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().project("x").by(__.select("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"x":["marko"]}] |
      | m[{"x":["josh"]}] |
      | m[{"x":["vadas"]}] |
      | m[{"x":["peter"]}] |
      | m[{"x":["lop"]}] |
      | m[{"x":["ripple"]}] |

  Scenario: g_V_projectXa_bX_byXinE_countX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().project("a", "b").
          by(__.inE().count()).
          by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"d[1].l"}] |
      | m[{"a":"d[3].l"}] |
      | m[{"a":"d[0].l", "b":"d[29].i"}] |
      | m[{"a":"d[1].l", "b":"d[27].i"}] |
      | m[{"a":"d[1].l", "b":"d[32].i"}] |
      | m[{"a":"d[0].l", "b":"d[35].i"}] |

  @WithProductiveByStrategy
  Scenario: g_withStrategiesXProductiveByStrategyX_V_projectXa_bX_byXinE_countX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().project("a", "b").
          by(__.inE().count()).
          by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"d[1].l", "b":null}] |
      | m[{"a":"d[3].l", "b":null}] |
      | m[{"a":"d[0].l", "b":"d[29].i"}] |
      | m[{"a":"d[1].l", "b":"d[27].i"}] |
      | m[{"a":"d[1].l", "b":"d[32].i"}] |
      | m[{"a":"d[0].l", "b":"d[35].i"}] |