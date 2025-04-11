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

@StepClassMap @StepMap
Feature: Step - map()

  Scenario: g_VX1X_mapXvaluesXnameXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).map(__.values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: g_VX1X_outE_label_mapXlengthX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE().label().map(__.length())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[7].i |
      | d[5].i |
      | d[5].i |

  Scenario: g_VX1X_out_mapXvaluesXnameXX_mapXlengthX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().map(__.values("name")).map(__.length())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].i |
      | d[5].i |
      | d[4].i |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withPath_V_asXaX_out_mapXselectXaX_valuesXnameXX
    Given the modern graph
    And the traversal of
      """
      g.withPath().V().as("a").out().map(__.select("a").values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | marko |
      | marko |
      | josh  |
      | josh  |
      | peter |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withPath_V_asXaX_out_out_asXbX_mapXselectXaX_valuesXnameX_concatXselectXbX_valuesXnameXXX
    Given the modern graph
    And the traversal of
      """
      g.withPath().V().as("a").out().out().as("b").map(__.select("a").values("name").concat(__.select("b").values("name")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | markoripple |
      | markolop |

  Scenario: g_V_mapXselectXaXX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").map(__.select("a"))
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

  Scenario: g_V_mapXconstantXnullXX
    Given the modern graph
    And the traversal of
      """
      g.V().map(__.constant(null))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |
      | null |
      | null |
      | null |
      | null |
      | null |