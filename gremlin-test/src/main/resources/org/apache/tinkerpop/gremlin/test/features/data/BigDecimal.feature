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

@StepClassData @DataBigDecimal
Feature: Data - BIGDECIMAL

  Scenario: g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 123)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGDECIMAL).is(P.typeOf(GType.BIGDECIMAL))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[123].m |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_mathXaddX0_5XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 10)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGDECIMAL).is(P.typeOf(GType.BIGDECIMAL)).math("_ + 0.5")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[10.5].d |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_isXgtX0XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 5)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGDECIMAL).is(P.typeOf(GType.BIGDECIMAL)).is(P.gt(0))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].m |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_sumX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 2).
        addV("data").property("int", 3).
        addV("data").property("int", 4)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGDECIMAL).is(P.typeOf(GType.BIGDECIMAL)).sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[9].m |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_minX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 1).
        addV("data").property("int", 5).
        addV("data").property("int", 10)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGDECIMAL).is(P.typeOf(GType.BIGDECIMAL)).min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].m |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_maxX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 7).
        addV("data").property("int", 14).
        addV("data").property("int", 21)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGDECIMAL).is(P.typeOf(GType.BIGDECIMAL)).max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[21].m |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_project_byXidentityX_byXmathXmulX10XXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 6)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGDECIMAL).is(P.typeOf(GType.BIGDECIMAL)).project("original", "multiplied").by(identity()).by(math("_ * 10"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":"d[6].m","multiplied":"d[60].d"}] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX99X_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX_groupCount
    Given the empty graph
    And the traversal of
      """
      g.inject(99).asNumber(GType.BIGDECIMAL).is(P.typeOf(GType.BIGDECIMAL)).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[99].m":"d[1].l"}] |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_BIGDECIMALXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.BIGDECIMAL))
      """
    When iterated to list
    Then the result should be empty
