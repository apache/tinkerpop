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

Feature: Step - and()

  Scenario: g_V_andXhasXage_gt_27X__outE_count_gte_2X_name
    Given the modern graph
    And the traversal of
      """
      g.V().and(__.has("age", P.gt(27)), __.outE().count().is(P.gte(2))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | josh |

  Scenario: g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name
    Given the modern graph
    And the traversal of
      """
      g.V().and(__.outE(), __.has(T.label, "person").and().has("age", P.gte(32))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | peter  |

  Scenario: g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("knows").and().out("created").in("created").as("a").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_V_asXaX_andXselectXaX_selectXaXX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").and(__.select("a"), __.select("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter]  |