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

@StepClassData @DataMap
Feature: Data - MAP

  Scenario: g_V_hasLabelXpersonX_valueMap_isXtypeOfXGType_MAPXX_count
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").valueMap().is(P.typeOf(GType.MAP)).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[4].l |

  Scenario: g_V_groupCount_byXlabelX_isXtypeOfXGType_MAPX
    Given the modern graph
    And the traversal of
      """
      g.V().groupCount().by(label).is(P.typeOf(GType.MAP))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"software":"d[2].l","person":"d[4].l"}] |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_MAPXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.MAP))
      """
    When iterated to list
    Then the result should be empty

  @AllowMapPropertyValues
  Scenario: g_V_valuesXmapX_isXtypeOfXGType_MAPXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("map", ["key1": "1", "key2": "2"])
      """
    And the traversal of
      """
      g.V().values("map").is(P.typeOf(GType.MAP))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"key1":"1","key2":"2"}] |

  @AllowMapPropertyValues
  Scenario: g_V_hasXmap_typeOfXGType_MAPXX_valuesXnameX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("name", "test").property("map", ["a": 1, "b": 2])
      """
    And the traversal of
      """
      g.V().has("map", P.typeOf(GType.MAP)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | test |

  @AllowMapPropertyValues
  Scenario: g_V_valuesXmapX_isXtypeOfXGType_MAPXX_countXlocalX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("map", ["a": 1, "b": 2, "c": 3])
      """
    And the traversal of
      """
      g.V().values("map").is(P.typeOf(GType.MAP)).count(local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l |

  @AllowMapPropertyValues
  Scenario: g_V_valuesXmapX_isXtypeOfXGType_MAPXX_selectXvaluesX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("map", ["city": "NYC", "country": "USA"])
      """
    And the traversal of
      """
      g.V().values("map").is(P.typeOf(GType.MAP)).select(values)
      """
    When iterated next
    Then the result should be unordered
      | result |
      | NYC |
      | USA |

  @AllowMapPropertyValues
  Scenario: g_V_valuesXmapX_isXtypeOfXGType_MAPXX_whereX_countXlocalX_isXgtX1XXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("map", ["single": "value"]).
        addV("data").property("map", ["key1": "1", "key2": "2"])
      """
    And the traversal of
      """
      g.V().values("map").is(P.typeOf(GType.MAP)).where(count(local).is(P.gt(1)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"key1":"1","key2":"2"}] |

  @AllowMapPropertyValues
  Scenario: g_V_valuesXmapX_isXtypeOfXGType_MAPXX_foldX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("map", ["a": 1]).
        addV("data").property("map", ["b": 2, "c": 3])
      """
    And the traversal of
      """
      g.V().values("map").is(P.typeOf(GType.MAP)).fold()
      """
    When iterated next
    Then the result should be unordered
      | result |
      | m[{"a":"d[1].i"}] |
      | m[{"b":"d[2].i","c":"d[3].i"}] |
