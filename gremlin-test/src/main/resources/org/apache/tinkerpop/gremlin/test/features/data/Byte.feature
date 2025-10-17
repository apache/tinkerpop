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

@StepClassData @DataByte
Feature: Data - BYTE

  Scenario: g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 5)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BYTE).is(P.typeOf(GType.BYTE))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].b |

  Scenario: g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_mathXaddX20XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 10)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BYTE).is(P.typeOf(GType.BYTE)).math("_ + 20")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[30].d |

  Scenario: g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_isXltX10XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 7)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BYTE).is(P.typeOf(GType.BYTE)).is(P.lt(10))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[7].b |

  Scenario: g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_sumX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 1).
        addV("data").property("int", 2).
        addV("data").property("int", 3)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BYTE).is(P.typeOf(GType.BYTE)).sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].b |

  Scenario: g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_project_byXidentityX_byXmathXmulX2XXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 8)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BYTE).is(P.typeOf(GType.BYTE)).project("original", "doubled").by(identity()).by(math("_ * 2"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":"d[8].b","doubled":"d[16].d"}] |

  Scenario: g_V_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_chooseXisXeqX12XX_constantXtwelveX_constantXotherXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 12)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BYTE).is(P.typeOf(GType.BYTE)).choose(__.is(P.eq(12)), constant("twelve"), constant("other"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | twelve |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX15X_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX_groupCount
    Given the empty graph
    And the traversal of
      """
      g.inject(15).asNumber(GType.BYTE).is(P.typeOf(GType.BYTE)).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[15].b":"d[1].l"}] |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_BYTEXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.BYTE))
      """
    When iterated to list
    Then the result should be empty
