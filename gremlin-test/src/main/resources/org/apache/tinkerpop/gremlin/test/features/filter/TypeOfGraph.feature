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
Feature: Predicate - typeOf() Graph

  Scenario: g_V_path_isXtypeOfXGType_PATHXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").path().is(P.typeOf(GType.PATH))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],marko] |
      | p[v[vadas],vadas] |
      | p[v[josh],josh] |
      | p[v[peter],peter] |

  Scenario: g_V_out_path_isXtypeOfXGType_PATHXX_count
    Given the modern graph
    And the traversal of
      """
      g.V().out().path().is(P.typeOf(GType.PATH)).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_V_hasXname_markoX_out_out_path_isXtypeOfXGType_PATHXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", "marko").out().out().path().is(P.typeOf(GType.PATH))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[josh],v[lop]] |

  Scenario: g_V_out_tree_isXtypeOfXGType_TREEXX_count
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", "marko").out().tree().is(P.typeOf(GType.TREE)).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |

  Scenario: g_V_whereXtree_isXtypeOfXGType_TREEXXX_values_name
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.tree().is(P.typeOf(GType.TREE))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_PATHXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.PATH))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_TREEXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.TREE))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_GRAPHXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.GRAPH))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_VPROPERTYXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.VPROPERTY))
      """
    When iterated to list
    Then the result should be empty
