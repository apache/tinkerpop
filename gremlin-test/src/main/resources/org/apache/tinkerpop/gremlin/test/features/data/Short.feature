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

@StepClassData @DataShort
Feature: Data - SHORT

  Scenario: g_V_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 100)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.SHORT).is(P.typeOf(GType.SHORT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[100].s |

  Scenario: g_V_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX_mathXmulX10XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 50)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.SHORT).is(P.typeOf(GType.SHORT)).math("_ * 10")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[500].d |

  Scenario: g_V_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX_isXbetweenX20_30XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 25)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.SHORT).is(P.typeOf(GType.SHORT)).is(P.between(20, 30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[25].s |

  Scenario: g_V_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX_minX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 10).
        addV("data").property("int", 20).
        addV("data").property("int", 30)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.SHORT).is(P.typeOf(GType.SHORT)).min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[10].s |

  Scenario: g_V_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX_maxX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 15).
        addV("data").property("int", 25).
        addV("data").property("int", 35)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.SHORT).is(P.typeOf(GType.SHORT)).max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[35].s |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX42X_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX_storeXaX_capXaX
    Given the empty graph
    And the traversal of
      """
      g.inject(42).asNumber(GType.SHORT).is(P.typeOf(GType.SHORT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[42].s |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_SHORTXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.SHORT))
      """
    When iterated to list
    Then the result should be empty