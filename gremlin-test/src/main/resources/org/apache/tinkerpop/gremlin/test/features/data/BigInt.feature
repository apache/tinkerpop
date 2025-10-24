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

@StepClassData @DataBigInt
Feature: Data - BIGINT

  Scenario: g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 456)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGINT).is(P.typeOf(GType.BIGINT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[456].n |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_mathXmulX1000XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 100)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGINT).is(P.typeOf(GType.BIGINT)).math("_ * 1000")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[100000].d |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_isXeqX42XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 42)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGINT).is(P.typeOf(GType.BIGINT)).is(P.eq(42))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[42].n |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_sumX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 10).
        addV("data").property("int", 20).
        addV("data").property("int", 30)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGINT).is(P.typeOf(GType.BIGINT)).sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[60].n |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_minX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 5).
        addV("data").property("int", 15).
        addV("data").property("int", 25)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGINT).is(P.typeOf(GType.BIGINT)).min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].n |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_maxX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 100).
        addV("data").property("int", 200).
        addV("data").property("int", 300)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGINT).is(P.typeOf(GType.BIGINT)).max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[300].n |

  Scenario: g_V_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_project_byXidentityX_byXmathXaddX999XXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("int", 50)
      """
    And the traversal of
      """
      g.V().values("int").asNumber(GType.BIGINT).is(P.typeOf(GType.BIGINT)).project("original", "added").by(identity()).by(math("_ + 999"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":"d[50].n","added":"d[1049].d"}] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX777X_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX_groupCount
    Given the empty graph
    And the traversal of
      """
      g.inject(777).asNumber(GType.BIGINT).is(P.typeOf(GType.BIGINT)).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[777].n":"d[1].l"}] |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_BIGINTXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.BIGINT))
      """
    When iterated to list
    Then the result should be empty
