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

@StepClassData @DataSet
Feature: Data - SET

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

  Scenario: g_V_valuesXageX_isXtypeOfXGType_SETXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.SET))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valueMap_selectXkeysX_dedup_isXtypeOfXGType_SETXX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().select(keys).dedup().is(P.typeOf(GType.SET))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[name,age] |
      | s[name,lang] |

  Scenario: g_V_valuesXsetX_isXtypeOfXGType_SETXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("set", {"a", "b", "c"})
      """
    And the traversal of
      """
      g.V().values("set").is(P.typeOf(GType.SET))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[a,b,c] |

  Scenario: g_V_hasXset_typeOfXGType_SETXX_valuesXnameX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("name", "test").property("set", {1, 2, 3})
      """
    And the traversal of
      """
      g.V().has("set", P.typeOf(GType.SET)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | test |

  Scenario: g_V_valuesXsetX_isXtypeOfXGType_SETXX_unfold
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("set", {"x", "y", "z"})
      """
    And the traversal of
      """
      g.V().values("set").is(P.typeOf(GType.SET)).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | x |
      | y |
      | z |

  Scenario: g_V_valuesXsetX_isXtypeOfXGType_SETXX_countXlocalX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("set", {1, 2, 3, 4, 5})
      """
    And the traversal of
      """
      g.V().values("set").is(P.typeOf(GType.SET)).count(local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].l |

  Scenario: g_V_valuesXsetX_isXtypeOfXGType_SETXX_whereXcountXlocalX_isXeqX3XXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("set", {1, 2}).
        addV("data").property("set", {1, 2, 3})
      """
    And the traversal of
      """
      g.V().values("set").is(P.typeOf(GType.SET)).where(count(local).is(P.eq(3)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[1].i,d[2].i,d[3].i] |

  Scenario: g_V_valuesXsetX_isXtypeOfXGType_SETXX_unfold_limitX2X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("set", {"first", "second", "third", "fourth"})
      """
    And the traversal of
      """
      g.V().values("set").is(P.typeOf(GType.SET)).unfold().limit(2)
      """
    When iterated to list
    Then the result should have a count of 2

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXsetX_isXtypeOfXGType_SETXX_groupCount
    Given the empty graph
    And the traversal of
      """
      g.inject({"test"}).is(P.typeOf(GType.SET)).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"s[test]":"d[1].l"}] |
