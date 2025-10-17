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

@StepClassData @DataLong
Feature: Data - LONG

  Scenario: g_V_valuesXlongX_isXtypeOfXGType_LONGXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("long", 1L)
      """
    And the traversal of
      """
      g.V().values("long").is(P.typeOf(GType.LONG))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |

  Scenario: g_V_hasXlong_typeOfXGType_LONGXX_valuesXnameX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("name", "test").property("long", 1L)
      """
    And the traversal of
      """
      g.V().has("long", P.typeOf(GType.LONG)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | test |

  Scenario: g_V_valuesXlongX_isXtypeOfXGType_LONGXX_mathXmulX2XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("long", 5L)
      """
    And the traversal of
      """
      g.V().values("long").is(P.typeOf(GType.LONG)).math("_ * 2")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[10].d |

  Scenario: g_V_valuesXlongX_isXtypeOfXGType_LONGXX_isXgtX5XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("long", 10L)
      """
    And the traversal of
      """
      g.V().values("long").is(P.typeOf(GType.LONG)).is(P.gt(5L))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[10].l |

  Scenario: g_V_valuesXlongX_isXtypeOfXGType_LONGXX_sumX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("long", 1L).
        addV("data").property("long", 2L).
        addV("data").property("long", 3L)
      """
    And the traversal of
      """
      g.V().values("long").is(P.typeOf(GType.LONG)).sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_V_valuesXlongX_isXtypeOfXGType_LONGXX_localXaggregateXaXX_capXaX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("long", 100L)
      """
    And the traversal of
      """
      g.V().values("long").is(P.typeOf(GType.LONG)).local(aggregate("a")).cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | d[100].l |
