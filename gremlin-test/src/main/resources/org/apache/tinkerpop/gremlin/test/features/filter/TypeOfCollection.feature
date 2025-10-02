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

@StepClassFilter @StepIs
Feature: Predicate - typeOf() Collection

  Scenario: g_V_hasLabelXcollectionX_valuesXlistX_isXtypeOfXGType_LISTXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("list").is(P.typeOf(GType.LIST))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[list] |

  Scenario: g_V_hasLabelXcollectionX_valuesXsetX_isXtypeOfXGType_SETXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("set").is(P.typeOf(GType.SET))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[set] |

  Scenario: g_V_hasLabelXcollectionX_valuesXmapX_isXtypeOfXGType_MAPXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("map").is(P.typeOf(GType.MAP))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"key":"value"}] |

  Scenario: g_V_valueXnameX_aggregateXxX_capXxX_isXtypeOfXGType_SETX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").aggregate("x").cap("x").is(P.typeOf(GType.SET))
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |
      | lop |
      | vadas |
      | ripple |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_LISTXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.LIST))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_MAPXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.MAP))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_SETXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.SET))
      """
    When iterated to list
    Then the result should be empty
