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

@StepClassData @DataList
Feature: Data - LIST

  Scenario: g_V_valuesXnameX_fold_isXtypeOfXGType_LISTXX_count
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().is(P.typeOf(GType.LIST)).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_LISTXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.LIST))
      """
    When iterated to list
    Then the result should be empty

  @AllowListPropertyValues
  Scenario: g_V_valuesXlistX_isXtypeOfXGType_LISTXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("list", ["a", "b", "c"])
      """
    And the traversal of
      """
      g.V().values("list").is(P.typeOf(GType.LIST))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[a,b,c] |


  @AllowListPropertyValues
  Scenario: g_V_hasXlist_typeOfXGType_LISTXX_valuesXnameX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("name", "test").property("list", [1, 2, 3])
      """
    And the traversal of
      """
      g.V().has("list", P.typeOf(GType.LIST)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | test |


  @AllowListPropertyValues
  Scenario: g_V_valuesXlistX_isXtypeOfXGType_LISTXX_unfold
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("list", ["x", "y", "z"])
      """
    And the traversal of
      """
      g.V().values("list").is(P.typeOf(GType.LIST)).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | x |
      | y |
      | z |


  @AllowListPropertyValues
  Scenario: g_V_valuesXlistX_isXtypeOfXGType_LISTXX_countXlocalX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("list", [1, 2, 3, 4, 5])
      """
    And the traversal of
      """
      g.V().values("list").is(P.typeOf(GType.LIST)).count(local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].l |


  @AllowListPropertyValues
  Scenario: g_V_valuesXlistX_isXtypeOfXGType_LISTXX_unfold_rangeX1_3X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("list", ["first", "second", "third", "fourth"])
      """
    And the traversal of
      """
      g.V().values("list").is(P.typeOf(GType.LIST)).unfold().range(1, 3)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | second |
      | third |


  @AllowListPropertyValues
  Scenario: g_V_valuesXlistX_isXtypeOfXGType_LISTXX_project_byXidentityX_byXcountXlocalX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("list", ["apple", "banana"])
      """
    And the traversal of
      """
      g.V().values("list").is(P.typeOf(GType.LIST)).project("original", "size").by(identity()).by(count(local))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":"l[apple,banana]","size":"d[2].l"}] |


  @AllowListPropertyValues
  Scenario: g_V_valuesXlistX_isXtypeOfXGType_LISTXX_whereXcountXlocalX_isXgtX2XXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("list", [1]).
        addV("data").property("list", [1, 2, 3])
      """
    And the traversal of
      """
      g.V().values("list").is(P.typeOf(GType.LIST)).where(count(local).is(P.gt(2)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[1].i,d[2].i,d[3].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXlistX_isXtypeOfXGType_LISTXX_groupCount
    Given the empty graph
    And the traversal of
      """
      g.inject(["test"]).is(P.typeOf(GType.LIST)).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"l[test]":"d[1].l"}] |
