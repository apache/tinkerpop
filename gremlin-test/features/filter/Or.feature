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

@StepClassFilter @StepOr
Feature: Step - or()

  Scenario: g_V_orXhasXage_gt_27X__outE_count_gte_2X_name
    Given the modern graph
    And the traversal of
      """
      g.V().or(__.has("age", P.gt(27)), __.outE().count().is(P.gte(2))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | josh  |
      | peter |

  Scenario: g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name
    Given the modern graph
    And the traversal of
      """
      g.V().or(__.outE("knows"), __.has(T.label, "software").or().has("age", P.gte(35))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | ripple |
      | lop  |
      | peter |

  Scenario: g_V_asXaX_orXselectXaX_selectXaXX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").or(__.select("a"), __.select("a"))
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

