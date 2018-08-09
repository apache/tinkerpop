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

Feature: Step - peerPressure()
                
  Scenario: g_V_peerPressure_hasXclusterX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().peerPressure().has("gremlin.peerPressureVertexProgram.cluster")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |
    And the graph should return 6 for count of "g.withComputer().V().peerPressure().has(\"gremlin.peerPressureVertexProgram.cluster\")"

  Scenario: g_V_peerPressure_byXclusterX_byXoutEXknowsXX_pageRankX1X_byXrankX_byXoutEXknowsXX_timesX2X_group_byXclusterX_byXrank_sumX_limitX100X
    Given an unsupported test
    Then nothing should happen because
      """
      The result returned is not supported under GraphSON 2.x and therefore cannot be properly asserted. More
      specifically it has long keys which basically get toString()'d under GraphSON 2.x. This test can be supported
      with GraphSON 3.x.
      """

  Scenario: g_V_hasXname_rippleX_inXcreatedX_peerPressure_byXoutEX_byXclusterX_repeatXunionXidentity__bothX_timesX2X_dedup_valueMapXname_clusterX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().has("name", "ripple").in("created").peerPressure().by(__.outE()).by("cluster").repeat(__.union(__.identity(), __.both())).times(2).dedup().valueMap("name", "cluster")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["marko"], "cluster": [1]}] |
      | m[{"name": ["vadas"], "cluster": [2]}] |
      | m[{"name": ["lop"], "cluster": [4]}] |
      | m[{"name": ["josh"], "cluster": [4]}] |
      | m[{"name": ["ripple"], "cluster": [4]}] |
      | m[{"name": ["peter"], "cluster": [6]}] |

  Scenario: g_V_hasXname_rippleX_inXcreatedX_peerPressure_withXEDGES_outEX_withXPROPERTY_NAME_clusterX_repeatXunionXidentity__bothX_timesX2X_dedup_valueMapXname_clusterX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().has("name", "ripple").in("created").peerPressure().with("~tinkerpop.peerPressure.edges",__.outE()).with("~tinkerpop.peerPressure.propertyName", "cluster").repeat(__.union(__.identity(), __.both())).times(2).dedup().valueMap("name", "cluster")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["marko"], "cluster": [1]}] |
      | m[{"name": ["vadas"], "cluster": [2]}] |
      | m[{"name": ["lop"], "cluster": [4]}] |
      | m[{"name": ["josh"], "cluster": [4]}] |
      | m[{"name": ["ripple"], "cluster": [4]}] |
      | m[{"name": ["peter"], "cluster": [6]}] |

