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

@StepClassData @DataInt
Feature: Data - INT

  Scenario: g_V_valuesXageX_isXtypeOfXGType_INTXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.INT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_hasXage_typeOfXGType_INTXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.typeOf(GType.INT)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | peter |

  Scenario: g_V_whereXvaluesXageX_isXtypeOfXGType_INTXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.values("age").is(P.typeOf(GType.INT))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | peter |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_INTXX_mathXincX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name", "age").is(P.typeOf(GType.INT)).math("_ + 1")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[30].d |
      | d[28].d |
      | d[33].d |
      | d[36].d |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_INTXX_sumX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.INT)).sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[123].i |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_INTXX_minX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.INT)).min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_INTXX_maxX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.INT)).max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[35].i |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_INTXX_meanX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.INT)).mean()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[30.75].d |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_INTXX_order_byXdescX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.INT)).order().by(desc)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[35].i |
      | d[32].i |
      | d[29].i |
      | d[27].i |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_INTXX_groupCount
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.INT)).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[29].i":"d[1].l","d[27].i":"d[1].l","d[32].i":"d[1].l","d[35].i":"d[1].l"}] |
