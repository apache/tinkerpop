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

@StepClassSideEffect @StepSubgraph
Feature: Step - subgraph()

  Scenario: g_VX1X_outEXknowsX_subgraphXsgX_name_capXsgX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows").subgraph("sg").values("name").cap("sg")
      """
    When iterated next
    Then the result should be a subgraph with the following
      | edges |
      | e[marko-knows->josh] |
      | e[marko-knows->vadas] |
    And the result should be a subgraph with the following
      | vertices |
      | v[marko] |
      | v[vadas] |
      | v[josh] |

  Scenario: g_V_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup_capXsgX
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.bothE("created").subgraph("sg").outV()).times(5).values("name").dedup().cap("sg")
      """
    When iterated next
    Then the result should be a subgraph with the following
      | edges |
      | e[marko-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |
      | e[peter-created->lop] |
    And the result should be a subgraph with the following
      | vertices |
      | v[marko] |
      | v[lop] |
      | v[josh] |
      | v[peter] |
      | v[ripple] |

  Scenario: g_V_outEXnoexistX_subgraphXsgXcapXsgX
    Given the modern graph
    And the traversal of
      """
      g.V().outE("noexist").subgraph("sg").cap("sg")
      """
    When iterated next
    Then the result should be a subgraph with the following
      | edges |
    And the result should be a subgraph with the following
      | vertices |

  Scenario: g_E_hasXweight_0_5X_subgraphXaX_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.E().has("weight", 0.4).subgraph("a").select("a")
      """
    When iterated next
    Then the result should be a subgraph with the following
      | edges |
      | e[marko-created->lop] |
      | e[josh-created->lop] |
    And the result should be a subgraph with the following
      | vertices |
      | v[marko] |
      | v[lop] |
      | v[josh] |
