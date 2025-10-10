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

@StepClassData @DataFloat
Feature: Data - FLOAT

  Scenario: g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("float", 2.5)
      """
    And the traversal of
      """
      g.V().values("float").asNumber(GType.FLOAT).is(P.typeOf(GType.FLOAT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[2.5].f |

  Scenario: g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_mathXmulX2XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("float", 3.0)
      """
    And the traversal of
      """
      g.V().values("float").asNumber(GType.FLOAT).is(P.typeOf(GType.FLOAT)).math("_ * 2")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6.0].d |

  Scenario: g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_isXeqX1_5XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("float", 1.5)
      """
    And the traversal of
      """
      g.V().values("float").asNumber(GType.FLOAT).is(P.typeOf(GType.FLOAT)).is(P.eq(1.5))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1.5].f |

  Scenario: g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_sumX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("float", 1.5).
        addV("data").property("float", 2.5).
        addV("data").property("float", 3.0)
      """
    And the traversal of
      """
      g.V().values("float").asNumber(GType.FLOAT).is(P.typeOf(GType.FLOAT)).sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[7.0].f |

  Scenario: g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_project_byXidentityX_byXmathXmulX10XXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("float", 4.5)
      """
    And the traversal of
      """
      g.V().values("float").asNumber(GType.FLOAT).is(P.typeOf(GType.FLOAT)).project("original", "multiplied").by(identity()).by(math("_ * 10"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":"d[4.5].f","multiplied":"d[45.0].d"}] |

  Scenario: g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_whereXisXgtX1_0XXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("float", 0.5).
        addV("data").property("float", 1.5)
      """
    And the traversal of
      """
      g.V().values("float").asNumber(GType.FLOAT).is(P.typeOf(GType.FLOAT)).where(__.is(P.gt(1.0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1.5].f |

  Scenario: g_V_valuesXfloatX_isXtypeOfXGType_FLOATXX_chooseXisXeqX3_0XX_constantXthreeX_constantXotherXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("float", 3.0)
      """
    And the traversal of
      """
      g.V().values("float").asNumber(GType.FLOAT).is(P.typeOf(GType.FLOAT)).choose(__.is(P.eq(3.0)), constant("three"), constant("other"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | three |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX2_0fX_isXtypeOfXGType_FLOATXX_groupCount
    Given the empty graph
    And the traversal of
      """
      g.inject(2.0).asNumber(GType.FLOAT).is(P.typeOf(GType.FLOAT)).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[2.0].f":"d[1].l"}] |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_FLOATXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.FLOAT))
      """
    When iterated to list
    Then the result should be empty