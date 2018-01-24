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

Feature: Step - range()

  Scenario: g_VX1X_out_limitX2X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out().limit(2)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[josh] |
      | v[vadas] |
      | v[lop] |
    And the result should have a count of 2

  Scenario: g_V_localXoutE_limitX1X_inVX_limitX3X
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.outE().limit(1)).inV().limit(3)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[josh] |
      | v[vadas] |
      | v[lop] |
      | v[ripple] |
    And the result should have a count of 3

  Scenario: g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out("knows").outE("created").range(0, 1).inV()
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[lop] |
      | v[ripple] |
    And the result should have a count of 1

  Scenario: g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out("knows").out("created").range(0, 1)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[lop] |
      | v[ripple] |
    And the result should have a count of 1

  Scenario: g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out("created").in("created").range(1, 3)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[marko] |
      | v[josh] |
      | v[peter] |
    And the result should have a count of 2

  Scenario: g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out("created").inE("created").range(1, 3).outV()
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[marko] |
      | v[josh] |
      | v[peter] |
    And the result should have a count of 2

  Scenario: g_V_repeatXbothX_timesX3X_rangeX5_11X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.both()).times(3).range(5, 11)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[marko] |
      | v[josh] |
      | v[peter] |
      | v[lop] |
      | v[vadas] |
      | v[ripple] |
    And the result should have a count of 6

  Scenario: g_V_asXaX_in_asXaX_in_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_limitXlocal_2X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").in().as("a").in().as("a").select("a").by(__.unfold().values("name").fold()).limit(Scope.local, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[lop,josh] |
      | l[ripple,josh] |

  Scenario: g_V_asXaX_in_asXaX_in_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_limitXlocal_1X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").in().as("a").in().as("a").select("a").by(__.unfold().values("name").fold()).limit(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | ripple |

  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_3X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select("a").by(__.unfold().values("name").fold()).range(Scope.local, 1, 3)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[josh,ripple] |
      | l[josh,lop] |

  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select("a").by(__.unfold().values("name").fold()).range(Scope.local, 1, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | josh |

  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_4_5X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select("a").by(__.unfold().values("name").fold()).range(Scope.local, 4, 5)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").in().as("b").in().as("c").select("a","b","c").by("name").limit(Scope.local, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"lop","b":"josh"}] |
      | m[{"a":"ripple","b":"josh"}] |

  Scenario: g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_1X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").in().as("b").in().as("c").select("a","b","c").by("name").limit(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"lop"}] |
      | m[{"a":"ripple"}] |

  Scenario: g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_3X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").out().as("c").select("a","b","c").by("name").range(Scope.local, 1, 3)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"b":"josh","c":"lop"}] |
      | m[{"b":"josh","c":"ripple"}] |

  Scenario: g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_2X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").out().as("c").select("a","b","c").by("name").range(Scope.local, 1, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"b":"josh"}] |
      | m[{"b":"josh"}] |

  Scenario: g_V_repeatXbothX_timesX3X_rangeX5_11X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(both()).times(3).range(5, 11)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[6].l |

  Scenario: g_V_repeatXbothX_timesX3X_rangeX5_11X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out("created").inE("created").range(1, 3).outV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[ripple] |