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

Feature: Step - map()

  Scenario: g_VX1X_mapXnameX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter l1 defined as "c[it.get().value('name')]"
    And the traversal of
      """
      g.V(vid1).map(l1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: g_VX1X_outE_label_mapXlengthX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter l1 defined as "c[it.get().length()]"
    And the traversal of
      """
      g.V(vid1).outE().label().map(l1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[7].i |
      | d[5].i |
      | d[5].i |

  Scenario: g_VX1X_out_mapXnameX_mapXlengthX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter l1 defined as "c[it.get().value('name')]"
    And using the parameter l2 defined as "c[it.get().toString().length()]"
    And the traversal of
      """
      g.V(vid1).out().map(l1).map(l2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].i |
      | d[5].i |
      | d[4].i |

  @RemoteOnly
  Scenario: g_VX1X_out_mapXlambdaXnameXX_mapXlambdaXlengthXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().map(Lambda.function("it.get().value('name')")).map(Lambda.function("it.get().toString().length()"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].i |
      | d[5].i |
      | d[4].i |

  Scenario: g_withPath_V_asXaX_out_mapXa_nameX
    Given the modern graph
    And using the parameter l1 defined as "c[it.path('a').value('name')]"
    And the traversal of
      """
      g.withPath().V().as("a").out().map(l1)
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

  Scenario: g_withPath_V_asXaX_out_out_mapXa_name_it_nameX
    Given the modern graph
    And using the parameter l1 defined as "c[it.path('a').value('name')  + it.get().value('name')]"
    And the traversal of
      """
      g.withPath().V().as("a").out().out().map(l1)
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